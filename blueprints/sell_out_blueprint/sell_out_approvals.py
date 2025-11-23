from flask import abort, request, jsonify, flash, render_template, session
from flask import Blueprint
from blueprints.auth import require_role
from decimal import Decimal
from sqlalchemy.exc import IntegrityError
from contextlib import contextmanager
from datetime import datetime, date
import os
from zoneinfo import ZoneInfo

from models import (
    model, distinct, or_, func,
    SP_SellOutUploads, SP_SellOutApproval, SP_MCSI_SellOut,
    SP_InventoryLedger, SP_Customer, SP_SellOutUploadFile, Brands,
    SP_SOH_Uploads, SP_SOH_Detail, SP_SellOutNegPreview, joinedload
)


from collections import defaultdict
from config import STATIC_DIR, TZ_RIYADH

bp = Blueprint("sell_out_approvals", __name__, static_folder=STATIC_DIR, url_prefix="/sell-out-approvals")

class NegativeSOHError(Exception):
    pass


@contextmanager
def _tx():
    try:
        yield
        model.commit()
    except Exception:
        model.rollback()
        raise



@bp.route("/", methods=["GET"])
@require_role("brand_manager","finance_manager","admin","developer")
def approvals_ui():
    return render_template("./sell_out/approvals.html")





def _post_sellout_running(upload: SP_SellOutUploads, actor: str, comment: str | None):
    """
    Approve & post a sell-out upload:
      - Validate per (SKU, date) that cumulative sell-out from this upload
        does not push balance below zero, where balance is derived from:
           active SOH snapshot <= date  +  ledger movements (since snapshot .. date)
      - If valid, write SELLOUT ledger rows (negative qty) per line and mark header Posted.
    """
    if upload.Status == "Posted":
        raise ValueError(f"Upload {upload.UploadID} already posted")

    # 1) pull active detail rows for this upload
    details = (model.query(SP_MCSI_SellOut)
               .filter(SP_MCSI_SellOut.UploadID == upload.UploadID,
                       SP_MCSI_SellOut.IsActive == True)
               .order_by(SP_MCSI_SellOut.SKU_ID.asc(),
                         SP_MCSI_SellOut.DocumentDate.asc(),
                         SP_MCSI_SellOut.RowNumber.asc())
               .all())
    if not details:
        raise ValueError("No active detail rows to post")

    # 3) write immutable SELLOUT ledger rows per detail line
    for d in details:
        model.add(SP_InventoryLedger(
            CustomerID   = upload.CustomerID,
            SKU_ID       = d.SKU_ID,
            DocDate      = d.DocumentDate,
            MovementType = "SELLOUT",
            MovementSubType = None,
            Qty          = -float(d.SellOutQty or 0.0),
            UploadID     = upload.UploadID,
            RefTable     = "SP_SellOutUploads",
            RefID        = str(upload.UploadID),
            IdempotencyKey = f"SO_APPROVE:{upload.UploadID}:{d.RowNumber}",
            CreatedAt    = datetime.utcnow()
        ))

    # 4) finalize header + audit trail
    upload.Status     = "Posted"
    upload.ApprovedBy = actor or "approver"
    upload.ApprovedAt = datetime.utcnow()

    model.add(SP_SellOutApproval(
        UploadID=upload.UploadID,
        Action="APPROVE",
        Actor=actor or "approver",
        Comment=comment
    ))

def _latest_active_snapshot(customer_id: int, brand: str | None, sku_id: int, as_of: date):
    """
    Return (snap_date, snap_qty) from the latest ACTIVE snapshot for (customer, brand?, sku)
    with SOHDate <= as_of. None if no snapshot.
    """
    q = (model.query(SP_SOH_Detail.SOHDate, SP_SOH_Detail.SOHQty)
         .join(SP_SOH_Uploads, SP_SOH_Uploads.SOHUploadID == SP_SOH_Detail.SOHUploadID)
         .filter(SP_SOH_Uploads.CustomerID == customer_id,
                 SP_SOH_Detail.SKU_ID == sku_id,
                 SP_SOH_Detail.IsActive == True,
                 SP_SOH_Detail.SOHDate <= as_of))
    if brand:
        q = q.filter(SP_SOH_Uploads.Brand == brand)
    q = q.order_by(SP_SOH_Detail.SOHDate.desc(), SP_SOH_Uploads.SOHUploadID.desc())
    row = q.first()
    return (row[0], float(row[1])) if row else (None, 0.0)

def _sum_ledger_between(customer_id: int, sku_id: int, since_excl: date | None, until_incl: date):
    """
    Sum signed ledger Qty for (customer, sku) in (since_excl, until_incl].
    If since_excl is None, sum from 'beginning' up to until_incl.
    """
    q = (model.query(func.coalesce(func.sum(SP_InventoryLedger.Qty), 0.0))
         .filter(SP_InventoryLedger.CustomerID == customer_id,
                 SP_InventoryLedger.SKU_ID == sku_id,
                 SP_InventoryLedger.DocDate <= until_incl))
    if since_excl:
        q = q.filter(SP_InventoryLedger.DocDate > since_excl)
    return float(q.scalar() or 0.0)

def _balance_as_of(customer_id: int, brand: str | None, sku_id: int, as_of: date) -> float:
    """
    Compute stock as of 'as_of' date:
      latest_active_snapshot_qty + sum(ledger movements after snapshot_date .. as_of)
    """
    snap_date, snap_qty = _latest_active_snapshot(customer_id, brand, sku_id, as_of)
    delta = _sum_ledger_between(customer_id, sku_id, snap_date, as_of)
    return float(snap_qty) + float(delta)

def _get_upload_or_404(upload_id:int):
    obj = model.query(SP_SellOutUploads).get(upload_id)
    if not obj:
        abort(404, description="Upload not found")
    return obj

def _preview_negatives(upload: SP_SellOutUploads):
    """
    Returns (has_negatives, per_line_info)
    per_line_info is a list of dicts keyed by RowNumber with:
      SKU_ID, DocumentDate, SellOutQty, AvailableBefore, CumulativeFromUpload, ResultingBalance, IsNegative
    Per-line cumulative: if the same SKU appears multiple times on the same day,
    we compute using a single 'available before' for that day and add each line’s quantity sequentially.
    """
    brand = upload.Brand
    details = (model.query(SP_MCSI_SellOut)
               .filter(SP_MCSI_SellOut.UploadID == upload.UploadID,
                       SP_MCSI_SellOut.IsActive == True)
               .order_by(SP_MCSI_SellOut.SKU_ID.asc(),
                         SP_MCSI_SellOut.DocumentDate.asc(),
                         SP_MCSI_SellOut.RowNumber.asc())
               .all())
    if not details:
        return False, []

    has_neg = False
    per_line = []

    # group rows by SKU
    by_sku = defaultdict(list)
    for d in details:
        by_sku[d.SKU_ID].append(d)

    for sku_id, rows in by_sku.items():
        rows.sort(key=lambda r: (r.DocumentDate, r.RowNumber))

        cum_total = 0.0              # cumulative from this upload across dates
        last_date = None
        available_for_date = 0.0     # available BEFORE today's upload for that date

        for d in rows:
            if d.DocumentDate != last_date:
                # Recompute availability once at the switch to a new date
                available_for_date = _balance_as_of(upload.CustomerID, brand, sku_id, d.DocumentDate)
                last_date = d.DocumentDate

            qty = float(d.SellOutQty or 0.0)
            cum_total += qty
            resulting = available_for_date - cum_total
            is_neg = (resulting < -1e-9)
            has_neg = has_neg or is_neg

            per_line.append({
                "RowNumber": d.RowNumber,
                "SKU_ID": d.SKU_ID,
                "DocumentDate": str(d.DocumentDate),
                "SellOutQty": qty,
                "AvailableBefore": float(available_for_date),
                "CumulativeFromUpload": float(cum_total),
                "ResultingBalance": float(resulting),
                "IsNegative": bool(is_neg),
            })

    return has_neg, per_line

def _load_persisted_preview(upload_id: int):
    rows = (model.query(SP_SellOutNegPreview)
            .filter(SP_SellOutNegPreview.UploadID == upload_id)
            .order_by(SP_SellOutNegPreview.RowNumber.asc())
            .all())
    if not rows:
        return None
    per_line = [{
        "RowNumber": r.RowNumber,
        "SKU_ID": r.SKU_ID,
        "DocumentDate": r.DocumentDate.isoformat() if r.DocumentDate else None,
        "SellOutQty": float(r.SellOutQty) if r.SellOutQty is not None else 0.0,
        "AvailableBefore": float(r.AvailableBefore) if r.AvailableBefore is not None else None,
        "CumulativeFromUpload": float(r.CumulativeFromUpload) if r.CumulativeFromUpload is not None else None,
        "ResultingBalance": float(r.ResultingBalance) if r.ResultingBalance is not None else None,
        "IsNegative": bool(r.IsNegative),
    } for r in rows]
    has_neg = any(x["IsNegative"] for x in per_line)
    return has_neg, per_line

def _compute_and_persist_preview(upload: SP_SellOutUploads):
    has_neg, per_line = _preview_negatives(upload)

    # 1) Clear old rows for this upload (ORM delete)
    model.query(SP_SellOutNegPreview)\
         .filter(SP_SellOutNegPreview.UploadID == upload.UploadID)\
         .delete(synchronize_session=False)

    # 2) Insert new rows (ORM bulk)
    objs = []
    for r in per_line:
        # Normalize types
        doc_date = r["DocumentDate"]
        if isinstance(doc_date, str):
            # 'YYYY-MM-DD' from your JSON; convert to date
            doc_date = date.fromisoformat(doc_date)

        def dec_or_none(x):
            if x is None:
                return None
            # Accept float/str -> Decimal
            return Decimal(str(x))

        objs.append(SP_SellOutNegPreview(
            UploadID             = upload.UploadID,
            RowNumber            = int(r["RowNumber"]),
            SKU_ID               = int(r["SKU_ID"]),
            DocumentDate         = doc_date,
            SellOutQty           = dec_or_none(r.get("SellOutQty", 0)),
            AvailableBefore      = dec_or_none(r.get("AvailableBefore")),
            CumulativeFromUpload = dec_or_none(r.get("CumulativeFromUpload")),
            ResultingBalance     = dec_or_none(r.get("ResultingBalance")),
            IsNegative           = bool(r.get("IsNegative", False)),
        ))

    if objs:
        # Use bulk_save_objects for speed; add_all is fine too.
        model.bulk_save_objects(objs)

    # 3) Update header flags
    upload.HasPotentialNegatives = bool(has_neg)
    upload.NegPreviewComputedAt  = datetime.utcnow()

    model.flush()

def _write_approval(upload_id:int, action:str, actor:str, comment:str|None):
    model.add(SP_SellOutApproval(
        UploadID=upload_id, Action=action, Actor=actor or "system", Comment=comment
    ))

def _fmt_dt_local(dt):
    if not dt: return None
    try: return dt.replace(tzinfo=ZoneInfo("UTC")).astimezone(TZ_RIYADH).strftime("%Y-%m-%d %H:%M")
    except Exception: return dt.isoformat()


# ---- list pending ----
@bp.route("/approvals/pending", methods=["GET"])
@require_role("brand_manager","finance_manager","admin","developer")
def list_pending():
    q = model.query(SP_SellOutUploads)\
             .filter(SP_SellOutUploads.Status=="Draft")\
             .order_by(SP_SellOutUploads.CreatedAt.desc())
    # optional filters
    brand = (request.args.get("brand") or "").strip() or None
    cust  = request.args.get("customer_id", type=int)
    if brand: q = q.filter(SP_SellOutUploads.Brand==brand)
    if cust:  q = q.filter(SP_SellOutUploads.CustomerID==cust)

    rows = [{
        "UploadID": u.UploadID,
        "CustomerID": u.CustomerID,
        "Brand": u.Brand,
        "Period": [str(u.PeriodStart), str(u.PeriodEnd)],
        "CreatedBy": u.CreatedBy,
        "ApprovedBy": u.ApprovedBy,
        "ApprovedAt": u.ApprovedAt,
        "CreatedAt": u.CreatedAt.isoformat()
    } for u in q.all()]
    return jsonify(ok=True, items=rows)


# ---- details for a single upload (for review UI) -------------
@bp.route("/approvals/<int:upload_id>", methods=["GET"])
def approval_details(upload_id:int):
    u = _get_upload_or_404(upload_id)

    det = (model.query(SP_MCSI_SellOut)
           .options(joinedload(SP_MCSI_SellOut.SKU))  # eager-load SP_SKU
           .filter(SP_MCSI_SellOut.UploadID == upload_id)
           .order_by(SP_MCSI_SellOut.SKU_ID.asc(),
                     SP_MCSI_SellOut.DocumentDate.asc(),
                     SP_MCSI_SellOut.RowNumber.asc())
           .all())
    
    details = [{
        "RowNumber": d.RowNumber,
        "SKU_ID": d.SKU_ID,
        "ArticleCode": d.SKU.ArticleCode if d.SKU else None, 
        "CustSKUCode": d.CustSKUCode,
        "DocumentDate": str(d.DocumentDate),
        "SellOutQty": float(d.SellOutQty or 0.0),
        "ReportedSOH": float(d.ReportedSOH) if d.ReportedSOH is not None else None,
        "IsActive": bool(d.IsActive),
    } for d in det]

    force = (request.args.get("recompute") or "").lower() in ("1","true","yes")

    cached = None if force else _load_persisted_preview(u.UploadID)
    if cached:
        has_neg, per_line_preview = cached
    else:
        has_neg, per_line_preview = _preview_negatives(u)
        # ⬇️ make the persisted preview durable
        try:
            with _tx():
                _compute_and_persist_preview(u)
        except Exception:
            pass

    return jsonify(ok=True, 
                   header={
                       "UploadID": u.UploadID, "Status": u.Status, "Brand": u.Brand,
                       "CustomerID": u.CustomerID,
                       "Period":[str(u.PeriodStart), str(u.PeriodEnd)],
                       "CreatedBy": u.CreatedBy, "CreatedAt": u.CreatedAt.isoformat(),
                       "HasPotentialNegatives": bool(has_neg if (force or not cached) 
                                                     else getattr(u, "HasPotentialNegatives", False))
                   },
                   details=details,
                   warnings=per_line_preview)

# ---- submit (move Draft/Rejected -> Draft) ----
@bp.route("/approvals/submit-bulk", methods=["POST"])
@require_role("brand_manager","finance_manager","admin","developer")
def submit_bulk():
    data = request.get_json(force=True) if request.is_json else request.form
    ids = data.get("ids")
    actor   = data.get("actor") or "uploader"
    comment = data.get("comment")

    if not ids:
        return jsonify(ok=False, error="ids required"), 400
    if isinstance(ids, str):
        ids = [int(x) for x in ids.split(",") if x.strip().isdigit()]
    else:
        ids = [int(x) for x in ids]

    submitted, skipped, failed = [], [], []
    for upload_id in ids:
        try:
            u = model.query(SP_SellOutUploads).get(upload_id)
            if not u:
                failed.append({"UploadID": upload_id, "error": "not found"}); continue
            if u.Status not in ("Draft", "Rejected"):
                skipped.append({"UploadID": upload_id, "Status": u.Status}); continue
            u.Status = "Draft"
            model.add(SP_SellOutApproval(
                UploadID=upload_id, Action="SUBMIT", Actor=actor, Comment=comment
            ))
            model.commit()
            submitted.append(upload_id)
        except Exception as e:
            model.rollback()
            failed.append({"UploadID": upload_id, "error": str(e)})

    return jsonify(ok=True, submitted=submitted, skipped=skipped, failed=failed)

# ---- approve (bulk supported) ----
@require_role("brand_manager","finance_manager","admin","developer")
@bp.route("/approvals/approve", methods=["POST"])
def approve_uploads():
    data = request.get_json(force=True) if request.is_json else request.form
    ids = data.get("ids")
    actor   = data.get("actor") or "approver"
    comment = data.get("comment")

    if not ids:
        return jsonify(ok=False, error="ids required"), 400
    if isinstance(ids, str):
        ids = [int(x) for x in ids.split(",") if x.strip().isdigit()]
    else:
        ids = [int(x) for x in ids]

    posted, skipped, failed = [], [], []
    for upload_id in ids:
        try:
            with _tx():
                u = _get_upload_or_404(upload_id)

                # Claim atomically: Draft -> Posting (NOT Posted yet)
                affected = (model.query(SP_SellOutUploads)
                            .filter(SP_SellOutUploads.UploadID == u.UploadID,
                                    SP_SellOutUploads.Status == "Draft")
                            .update({SP_SellOutUploads.Status: "Posting"}, synchronize_session=False))
                model.flush()
                if affected != 1:
                    skipped.append({"UploadID": upload_id, "Status": u.Status})
                    continue

                # Now do the heavy work; will set Status="Posted" on success
                _post_sellout_running(u, actor, comment)
                posted.append(upload_id)
        except NegativeSOHError as e:
            try: flash(str(e), "danger")
            except Exception: pass
            failed.append({"UploadID": upload_id, "error": str(e)})
        except Exception as e:
            failed.append({"UploadID": upload_id, "error": str(e)})

    return jsonify(ok=True, posted=posted, skipped=skipped, failed=failed)


# ---- reject ----
@bp.route("/approvals/reject", methods=["POST"])
@require_role("brand_manager","finance_manager","admin","developer")
def reject_uploads():
    data = request.get_json(force=True) if request.is_json else request.form
    ids = data.get("ids")
    comment = data.get("comment") or "Rejected"
    actor = data.get("actor") or "approver"
    if not ids:
        return jsonify(ok=False, error="ids required"), 400
    if isinstance(ids, str):
        ids = [int(x) for x in ids.split(",") if x.strip().isdigit()]
    ids = [int(x) for x in ids]

    with _tx():
        for upload_id in ids:
            u = _get_upload_or_404(upload_id)
            if u.Status != "Draft":
                continue
            u.Status = "Rejected"
            u.ApprovedBy = None
            u.ApprovedAt = None
            _write_approval(upload_id, "REJECT", actor, comment)

    return jsonify(ok=True, rejected=ids)

# ---- list uploads (for line-item UI) ----
@bp.route("/uploads", methods=["GET"])
def list_uploads():
    """
    Returns paginated sell-out uploads with summary + attachments for line-item UI.
    Query params (optional):
      - page, page_size
      - status: Draft|Rejected|Posted
      - brand
      - customer_id
      - date_from, date_to   (filter by CreatedAt)
      - q  (search in CreatedBy or SourceFileName)
    """
    page      = max(1, request.args.get("page", type=int) or 1)
    page_size = min(100, request.args.get("page_size", type=int) or 20)
    status    = (request.args.get("status") or "").strip() or None
    brand     = (request.args.get("brand") or "").strip() or None
    cust_id   = request.args.get("customer_id", type=int)
    date_from = request.args.get("date_from")  # ISO date
    date_to   = request.args.get("date_to")
    q         = (request.args.get("q") or "").strip() or None

    base = model.query(SP_SellOutUploads, SP_Customer.CustName, SP_Customer.CustCode)\
        .join(SP_Customer, SP_Customer.CustomerID == SP_SellOutUploads.CustomerID)

    if status:  base = base.filter(SP_SellOutUploads.Status == status)
    if brand:   base = base.filter(SP_SellOutUploads.Brand == brand)
    if cust_id: base = base.filter(SP_SellOutUploads.CustomerID == cust_id)
    if date_from: base = base.filter(SP_SellOutUploads.CreatedAt >= date_from)
    if date_to:   base = base.filter(SP_SellOutUploads.CreatedAt <= date_to)
    if q:
        like = f"%{q}%"
        base = base.filter(
            (SP_SellOutUploads.CreatedBy.ilike(like)) |
            (SP_SellOutUploads.SourceFileName.ilike(like))
            .options(joinedload(SP_SellOutUploads.Approver))
        )

    # aggregates over detail (active rows only)
    agg = model.query(
        SP_MCSI_SellOut.UploadID.label("UploadID"),
        func.count().label("RowCount"),
        func.count(distinct(SP_MCSI_SellOut.SKU_ID)).label("DistinctSKU"),
        func.coalesce(func.sum(SP_MCSI_SellOut.SellOutQty), 0.0).label("TotalSellOutQty"),
    )\
     .group_by(SP_MCSI_SellOut.UploadID)\
     .subquery()

    # attachment counts (optional list comes next)
    att_agg = model.query(
        SP_SellOutUploadFile.UploadID.label("UploadID"),
        func.count().label("AttachmentCount")
    ).group_by(SP_SellOutUploadFile.UploadID).subquery()

    # most recent approval action (for quick display)
    last_appr = model.query(
        SP_SellOutApproval.UploadID.label("UploadID"),
        func.max(SP_SellOutApproval.ActedAt).label("LastActedAt")
    ).group_by(SP_SellOutApproval.UploadID).subquery()

    # apply pagination
    total = base.count()
    # JOIN + SELECT the subquery columns explicitly
    rows = (
        base
        .outerjoin(agg,     agg.c.UploadID     == SP_SellOutUploads.UploadID)
        .outerjoin(att_agg, att_agg.c.UploadID == SP_SellOutUploads.UploadID)
        .outerjoin(last_appr, last_appr.c.UploadID == SP_SellOutUploads.UploadID)
        .add_columns(
            func.coalesce(agg.c.RowCount, 0).label("RowCount"),
            func.coalesce(agg.c.DistinctSKU, 0).label("DistinctSKU"),
            func.coalesce(agg.c.TotalSellOutQty, 0.0).label("TotalSellOutQty"),
            func.coalesce(att_agg.c.AttachmentCount, 0).label("AttachmentCount"),
            last_appr.c.LastActedAt.label("LastActedAt"),
        )
        .order_by(SP_SellOutUploads.CreatedAt.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    # collect upload ids to fetch attachments (for preview links)
    upload_ids = [u.UploadID for (u, *_rest) in rows]
    atts_by_upload = {}
    if upload_ids:
        atts = model.query(SP_SellOutUploadFile)\
                    .filter(SP_SellOutUploadFile.UploadID.in_(upload_ids))\
                    .order_by(SP_SellOutUploadFile.UploadedAt.desc())\
                    .all()
        for a in atts:
            atts_by_upload.setdefault(a.UploadID, []).append({
                "FileID": a.FileID,
                "OriginalName": a.OriginalName,
                "SizeBytes": a.SizeBytes,
                "UploadedAt": _fmt_dt_local(a.UploadedAt),
                "Url": f"/sell_out_uploads/upload-attachment/load/{a.ServerID}"  # FilePond load endpoint
            })

    items = []
    for (u, cust_name, cust_code, rowcount, distinct_sku, total_qty, att_count, last_acted) in rows:
        items.append({
            "UploadID": u.UploadID,
            "Status": u.Status,
            "Brand": u.Brand,
            "Customer": {"id": u.CustomerID, "name": cust_name, "code": cust_code},
            "LevelType": u.LevelType,
            "UploadType": u.UploadType,
            "Period": [
                str(u.PeriodStart) if u.PeriodStart else None,
                str(u.PeriodEnd)   if u.PeriodEnd   else None,
            ],
            "CreatedBy": u.CreatedBy,
            "CreatedAtUTC": u.CreatedAt.isoformat() if u.CreatedAt else None,
            "CreatedAtLocal": _fmt_dt_local(u.CreatedAt) if u.CreatedAt else None,
            "ApprovedBy": (
                (u.Approver.Fullname or u.Approver.Username) 
                if getattr(u, "Approver", None) 
                else None),
            "ApprovedAtLocal": _fmt_dt_local(u.ApprovedAt) if u.ApprovedAt else None,
            "SourceFileName": u.SourceFileName,
            "RowCount": int(rowcount or 0),
            "DistinctSKU": int(distinct_sku or 0),
            "TotalSellOutQty": float(total_qty or 0.0),
            "AttachmentCount": int(att_count or 0),
            "Attachments": atts_by_upload.get(u.UploadID, [])[:3],
            "LastApprovalActionAt": _fmt_dt_local(last_acted) if last_acted else None,
            "Notes": u.Notes,
            "HasPotentialNegatives": bool(getattr(u, "HasPotentialNegatives", False)),
            "SupersededByUploadID": u.SupersededByUploadID,
        })
    
    # has_neg = False
    # try:
    #     # Only compute for visible rows (cheap). Uses your existing preview logic.
    #     u_for_flag = model.query(SP_SellOutUploads).get(u.UploadID)
    #     has_neg, _ = _preview_negatives(u_for_flag)
    # except Exception:
    #     has_neg = False
    
    # items[-1]["HasPotentialNegatives"] = bool(has_neg)

    return jsonify(ok=True, page=page, page_size=page_size, total=total, items=items)

# ---- detail for a single upload (for line-item UI) ----
@bp.route("/uploads/<int:upload_id>", methods=["GET"])
def upload_detail(upload_id: int):
    u = model.query(SP_SellOutUploads).get(upload_id)
    if not u:
        return jsonify(ok=False, error="Not found"), 404

    cust = model.query(SP_Customer.CustName, SP_Customer.CustCode)\
                .filter(SP_Customer.CustomerID == u.CustomerID).first()

    # details summary
    agg = model.query(
        func.count().label("RowCount"),
        func.count(distinct(SP_MCSI_SellOut.SKU_ID)).label("DistinctSKU"),
        func.coalesce(func.sum(SP_MCSI_SellOut.SellOutQty), 0.0).label("TotalSellOutQty"),
    ).filter(SP_MCSI_SellOut.UploadID == upload_id,
             SP_MCSI_SellOut.IsActive == True).one()

    # attachments
    atts = model.query(SP_SellOutUploadFile)\
                .filter(SP_SellOutUploadFile.UploadID == upload_id)\
                .order_by(SP_SellOutUploadFile.UploadedAt.desc()).all()
    attachments = [{
        "FileID": a.FileID,
        "OriginalName": a.OriginalName,
        "SizeBytes": a.SizeBytes,
        "UploadedAt": _fmt_dt_local(a.UploadedAt),
        "Url": f"/sell_out_uploads/upload-attachment/load/{a.ServerID}"
    } for a in atts]

    # approvals trail (optional, nice for audit)
    trail = model.query(SP_SellOutApproval)\
                 .filter(SP_SellOutApproval.UploadID == upload_id)\
                 .order_by(SP_SellOutApproval.ActedAt.asc()).all()
    approvals = [{
        "Action": t.Action,
        "Actor": t.Actor,
        "Comment": t.Comment,
        "ActedAtLocal": _fmt_dt_local(t.ActedAt)
    } for t in trail]

    return jsonify(ok=True, item={
        "UploadID": u.UploadID,
        "Status": u.Status,
        "Brand": u.Brand,
        "Customer": {"id": u.CustomerID, "name": (cust[0] if cust else None), "code": (cust[1] if cust else None)},
        "LevelType": u.LevelType,
        "UploadType": u.UploadType,
        "Period": [str(u.PeriodStart) if u.PeriodStart else None,
                   str(u.PeriodEnd)   if u.PeriodEnd   else None],
        "CreatedBy": u.CreatedBy,
        "CreatedAtLocal": _fmt_dt_local(u.CreatedAt),
        "ApprovedBy": u.ApprovedBy,
        "ApprovedAtLocal": _fmt_dt_local(u.ApprovedAt) if u.ApprovedAt else None,
        "SourceFileName": u.SourceFileName,
        "Notes": u.Notes,
        "SupersededByUploadID": u.SupersededByUploadID,
        "RowCount": int(agg.RowCount or 0),
        "DistinctSKU": int(agg.DistinctSKU or 0),
        "TotalSellOutQty": float(agg.TotalSellOutQty or 0.0),
        "Attachments": attachments,
        "Approvals": approvals
    })

# ---- choices for filters (brands, customers, statuses) ----
@bp.route("/choices", methods=["GET"])
def choices():
    """
    GET /sell-out/choices
    Query params (optional):
      - brand_q: substring match on brand
      - customer_q: substring match on customer name/code
      - include_counts: '1'|'true' to include pending-approval counts
    Response:
      {
        "brands":     [{"name": "Pepsi", "pending": 3}, ...],
        "customers":  [{"id": 1, "code": "CUST001", "name": "Acme KSA", "level": "HO", "pending": 2}, ...],
        "statuses":   ["Draft","Rejected","Posted"],
        "level_types":["HO","Branch"],
        "upload_types":["Customer-Format","Company-Format"]
      }
    """
    brand_q    = (request.args.get("brand_q") or "").strip()
    customer_q = (request.args.get("customer_q") or "").strip()
    include_counts = (request.args.get("include_counts") or "").lower() in ("1","true","yes")

    # ---- brands ----
    bq = model.query(Brands.BrandName).filter(Brands.BrandName.isnot(None))
    if brand_q:
        bq = bq.filter(Brands.BrandName.ilike(f"%{brand_q}%"))
    brand_rows = bq.distinct().order_by(Brands.BrandName).all()
    brands = [{"name": b[0]} for b in brand_rows]

    # ---- customers ----
    cq = model.query(
        SP_Customer.CustomerID, SP_Customer.CustCode, SP_Customer.CustName, SP_Customer.LevelType
    )
    if customer_q:
        like = f"%{customer_q}%"
        cq = cq.filter(or_(SP_Customer.CustName.ilike(like), SP_Customer.CustCode.ilike(like)))
    cust_rows = cq.order_by(SP_Customer.CustName).all()
    customers = [{
        "id":   cid,
        "code": code,
        "name": name,
        "level": level
    } for (cid, code, name, level) in cust_rows]

    # ---- optional: pending counts (nice for badges) ----
    if include_counts:
        pend = model.query(
            SP_SellOutUploads.Brand,
            SP_SellOutUploads.CustomerID,
            func.count().label("cnt")
        ).filter(SP_SellOutUploads.Status == "Draft")\
         .group_by(SP_SellOutUploads.Brand, SP_SellOutUploads.CustomerID).all()

        brand_counts = {}
        cust_counts  = {}
        for brand, customer_id, cnt in pend:
            if brand:
                brand_counts[brand] = brand_counts.get(brand, 0) + int(cnt or 0)
            if customer_id:
                cust_counts[customer_id] = cust_counts.get(customer_id, 0) + int(cnt or 0)

        for b in brands:
            b["pending"] = brand_counts.get(b["name"], 0)
        for c in customers:
            c["pending"] = cust_counts.get(c["id"], 0)

    return jsonify({
        "brands": brands,
        "customers": customers,
        # extras that are useful for dropdowns (your UI can ignore if unused)
        "statuses": ["Draft","Rejected","Posted"],
        "level_types": ["HO","Branch"],
        "upload_types": ["Customer-Format","Company-Format"],
    })
