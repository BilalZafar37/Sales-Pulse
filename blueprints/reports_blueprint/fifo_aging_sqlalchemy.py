# blueprints/fifo_aging_sqlalchemy.py
from __future__ import annotations
from flask import Blueprint, request, jsonify, render_template, Response, session
from datetime import date, datetime
from sqlalchemy import (
    select, func, case, literal, and_, or_, text, Float
)
import io, csv
import os

# ---- models / session ----
from models import (
    model,
    SP_InventoryLedger, SP_Customer, SP_SKU, SP_MCSI_SellIn
)

from config import STATIC_DIR

fifo_aging_bp = Blueprint(
    "fifo_aging_sa",
    __name__,
    static_folder=STATIC_DIR,
    url_prefix="/fifo_aging"
)

MOVT_ADJUST = "ADJUST"

# ----------------------------
# Utilities
# ----------------------------

def _parse_date(s, default=None):
    if not s:
        return default
    if isinstance(s, datetime):
        return s.date()
    if isinstance(s, date):
        return s
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return default

def _parse_int(s):
    try:
        return int(s) if s not in (None, "", "null") else None
    except Exception:
        return None

def _listify(s):
    if s is None or s == "":
        return []
    if isinstance(s, (list, tuple)):
        return list(s)
    return [x.strip() for x in str(s).split(",") if x.strip()]

def _brand_visibility_clause(sku_tbl):
    """
    Brand access via session + ?brand filter.
    session["user_brand_access"] may be e.g. ["Sony","TCL"]
    """
    user_access = session.get("user_brand_access") or []
    brand_param = request.args.get("brand")

    predicate = literal(True)
    if brand_param:
        predicate = and_(predicate, sku_tbl.c.Brand == brand_param)
        if user_access:
            predicate = and_(predicate, sku_tbl.c.Brand.in_(user_access))
    elif user_access:
        predicate = and_(predicate, sku_tbl.c.Brand.in_(user_access))
    return predicate

def build_fifo_ctes(
    as_of_date: date,
    customer_id: int | None,
    sku_id: int | None,
    article_code: str | None,
    brand: str | None,
    sales_office: list[str],
    sales_group: list[str],
    min_age: int | None,
    max_age: int | None,
    only_positive_soh: bool,
):
    L = SP_InventoryLedger.__table__
    C = SP_Customer.__table__
    S = SP_SKU.__table__

    # -------- 1) Find customer-level earliest ADJUST date (inclusive) --------
    adj_pred = [L.c.MovementType == MOVT_ADJUST, L.c.DocDate <= as_of_date]
    if customer_id:
        adj_pred.append(L.c.CustomerID == customer_id)

    AdjustDates = (
        select(
            L.c.CustomerID,
            func.min(L.c.DocDate).label("adjust_date")
        )
        .where(and_(*adj_pred))
        .group_by(L.c.CustomerID)
        .cte("AdjustDates")
    )
    # NOTE: Customers with no ADJUST won't appear here → will be excluded downstream.

    # -------- 2) Movements window = [adjust_date .. as_of_date] (no brand scoping) --------
    base_pred = [
        L.c.DocDate <= as_of_date,
        L.c.DocDate >= AdjustDates.c.adjust_date,       # inclusive
        L.c.CustomerID == AdjustDates.c.CustomerID      # join constraint
    ]
    if sku_id:
        base_pred.append(L.c.SKU_ID == sku_id)
    if customer_id:
        base_pred.append(L.c.CustomerID == customer_id)

    Movements = (
        select(
            L.c.CustomerID,
            L.c.SKU_ID,
            L.c.DocDate,
            L.c.Qty.cast(Float).label("Qty"),
            L.c.MovementType,
            L.c.LedgerID,
        )
        .select_from(L.join(AdjustDates, L.c.CustomerID == AdjustDates.c.CustomerID))
        .where(and_(*base_pred))
        .cte("Movements")
    )

    # -------- 3) Per (Customer, SKU) totals from the window --------
    Totals = (
        select(
            Movements.c.CustomerID,
            Movements.c.SKU_ID,
            func.sum(case((Movements.c.Qty < 0, -Movements.c.Qty), else_=0.0)).label("total_issues"),
            func.sum(case((Movements.c.Qty > 0,  Movements.c.Qty),  else_=0.0)).label("total_receipts"),
            func.max(Movements.c.DocDate).label("last_movement_date"),
            func.max(case((Movements.c.Qty < 0, Movements.c.DocDate), else_=None)).label("last_sellout_date"),
        )
        .group_by(Movements.c.CustomerID, Movements.c.SKU_ID)
        .cte("Totals")
    )

    # -------- 4) Receipts view (positive Qty), cumulative sums for FIFO --------
    cum_in_including = func.sum(Movements.c.Qty).over(
        partition_by=[Movements.c.CustomerID, Movements.c.SKU_ID],
        order_by=[Movements.c.DocDate, Movements.c.LedgerID],
    )
    cum_in_prev = func.sum(Movements.c.Qty).over(
        partition_by=[Movements.c.CustomerID, Movements.c.SKU_ID],
        order_by=[Movements.c.DocDate, Movements.c.LedgerID],
        rows=(None, -1),
    )
    Receipts = (
        select(
            Movements.c.CustomerID,
            Movements.c.SKU_ID,
            Movements.c.DocDate.label("lot_date"),
            Movements.c.LedgerID,
            Movements.c.Qty.label("lot_qty"),
            cum_in_including.label("cum_in_including"),
            cum_in_prev.label("cum_in_prev"),
        )
        .where(Movements.c.Qty > 0)
        .cte("Receipts")
    )

    # -------- 5) FIFO allocation (how much of each lot is consumed) --------
    consumed_expr = case(
        (Totals.c.total_issues <= func.coalesce(Receipts.c.cum_in_prev, 0), 0.0),
        (
            Totals.c.total_issues >= (func.coalesce(Receipts.c.cum_in_prev, 0) + Receipts.c.lot_qty),
            Receipts.c.lot_qty,
        ),
        else_=(Totals.c.total_issues - func.coalesce(Receipts.c.cum_in_prev, 0)),
    ).label("consumed_qty")

    remaining_qty_expr = (Receipts.c.lot_qty - consumed_expr).label("remaining_qty")

    Allocated = (
        select(
            Receipts.c.CustomerID,
            Receipts.c.SKU_ID,
            Receipts.c.lot_date,
            Receipts.c.LedgerID,
            Receipts.c.lot_qty,
            func.coalesce(Receipts.c.cum_in_prev, 0).label("cum_in_prev"),
            func.coalesce(Totals.c.total_issues, 0).label("total_issues"),
            consumed_expr,
            remaining_qty_expr,
        )
        .select_from(
            Receipts.join(
                Totals,
                and_(
                    Totals.c.CustomerID == Receipts.c.CustomerID,
                    Totals.c.SKU_ID     == Receipts.c.SKU_ID,
                ),
                isouter=True,
            )
        )
        .cte("Allocated")
    )

    # -------- 6) Live layers (remaining > 0) + aging --------
    age_days = func.datediff(text("day"), Allocated.c.lot_date, literal(as_of_date)).label("age_days")
    LiveLayers = (
        select(
            Allocated.c.CustomerID,
            Allocated.c.SKU_ID,
            Allocated.c.lot_date,
            Allocated.c.remaining_qty,
            age_days,
        )
        .where(Allocated.c.remaining_qty > 0)
        .cte("LiveLayers")
    )

    # Optional age filters
    live_pred = [literal(True)]
    if min_age is not None:
        live_pred.append(LiveLayers.c.age_days >= min_age)
    if max_age is not None:
        live_pred.append(LiveLayers.c.age_days <= max_age)

    LiveLayersFiltered = (
        select(
            LiveLayers.c.CustomerID,
            LiveLayers.c.SKU_ID,
            LiveLayers.c.lot_date,
            LiveLayers.c.remaining_qty,
            LiveLayers.c.age_days,
        )
        .where(and_(*live_pred))
        .cte("LiveLayersFiltered")
    )

    # -------- 7) Buckets + SOH/aging stats --------
    b0_30  = func.sum(case((LiveLayersFiltered.c.age_days.between(0, 30),  LiveLayersFiltered.c.remaining_qty), else_=0.0)).label("b_0_30")
    b31_60 = func.sum(case((LiveLayersFiltered.c.age_days.between(31, 60), LiveLayersFiltered.c.remaining_qty), else_=0.0)).label("b_31_60")
    b61_90 = func.sum(case((LiveLayersFiltered.c.age_days.between(61, 90), LiveLayersFiltered.c.remaining_qty), else_=0.0)).label("b_61_90")
    b90p   = func.sum(case((LiveLayersFiltered.c.age_days > 90,               LiveLayersFiltered.c.remaining_qty), else_=0.0)).label("b_90p")
    soh_qty = func.sum(LiveLayersFiltered.c.remaining_qty).label("soh_qty")
    weighted_age_sum = func.sum(LiveLayersFiltered.c.remaining_qty * LiveLayersFiltered.c.age_days).label("weighted_age_sum")
    oldest_lot = func.min(LiveLayersFiltered.c.lot_date).label("oldest_lot_date")
    newest_lot = func.max(LiveLayersFiltered.c.lot_date).label("newest_lot_date")

    Buckets = (
        select(
            LiveLayersFiltered.c.CustomerID,
            LiveLayersFiltered.c.SKU_ID,
            b0_30, b31_60, b61_90, b90p,
            soh_qty,
            weighted_age_sum,
            oldest_lot,
            newest_lot,
        )
        .group_by(LiveLayersFiltered.c.CustomerID, LiveLayersFiltered.c.SKU_ID)
        .cte("Buckets")
    )

    # -------- 8) Enrich + visibility filters (brand only here; does not affect SOH math) --------
    base_join = (
        Buckets.join(C, C.c.CustomerID == Buckets.c.CustomerID, isouter=True)
               .join(S, S.c.SKU_ID == Buckets.c.SKU_ID, isouter=True)
               .join(Totals, and_(Totals.c.CustomerID == Buckets.c.CustomerID,
                                  Totals.c.SKU_ID == Buckets.c.SKU_ID), isouter=True)
    )

    brand_guard = _brand_visibility_clause(S)  # applies session/user access, optional brand query param
    more_pred = [brand_guard]
    if brand:
        more_pred.append(S.c.Brand == brand)
    if article_code:
        more_pred.append(S.c.ArticleCode == article_code)
    if customer_id:
        more_pred.append(Buckets.c.CustomerID == customer_id)
    if sku_id:
        more_pred.append(Buckets.c.SKU_ID == sku_id)

    # Optional Sell-In dimension filters (SalesOffice / SalesGroup) via EXISTS
    if sales_office or sales_group:
        MSI = SP_MCSI_SellIn.__table__
        exists_pred = and_(MSI.c.Article == S.c.ArticleCode, MSI.c.SoldToParty == C.c.CustName)
        if sales_office:
            exists_pred = and_(exists_pred, MSI.c.SalesOffice.in_(sales_office))
        if sales_group:
            exists_pred = and_(exists_pred, MSI.c.SalesGroup.in_(sales_group))
        more_pred.append(select(literal(1)).where(exists_pred).exists())

    if only_positive_soh:
        more_pred.append(Buckets.c.soh_qty > 0)

    avg_age = case(
        (Buckets.c.soh_qty > 0, Buckets.c.weighted_age_sum / Buckets.c.soh_qty),
        else_=None
    ).label("avg_age_days")

    # ---- Price metrics from Sell-In (unchanged, still optional if columns exist) ----
    price_cols = []
    try:
        SI = SP_MCSI_SellIn.__table__
        si_qty  = 'GrossSale'
        si_net  = 'Net'
        si_date = 'DocumentDate'
        si_art  = 'Article'
        si_cust = 'SoldToParty'

        if all([si_qty, si_net, si_date, si_art, si_cust]):
            unit_price = (SI.c[si_net].cast(Float) / func.nullif(SI.c[si_qty].cast(Float), 0.0))

            price_avg = select(
                (func.sum(SI.c[si_net].cast(Float)) / func.nullif(func.sum(SI.c[si_qty].cast(Float)), 0.0))
            ).select_from(
                SI.join(C, C.c.CustName == SI.c[si_cust]).join(S, S.c.ArticleCode == SI.c[si_art])
            ).where(
                and_(
                    C.c.CustomerID == Buckets.c.CustomerID,
                    S.c.SKU_ID     == Buckets.c.SKU_ID,
                    SI.c[si_date] <= as_of_date,
                    SI.c[si_qty] > 0.0
                )
            ).scalar_subquery().label("avg_price")

            price_high = select(func.max(unit_price)).select_from(
                SI.join(C, C.c.CustName == SI.c[si_cust]).join(S, S.c.ArticleCode == SI.c[si_art])
            ).where(
                and_(
                    C.c.CustomerID == Buckets.c.CustomerID,
                    S.c.SKU_ID     == Buckets.c.SKU_ID,
                    SI.c[si_date] <= as_of_date,
                    SI.c[si_qty] > 0.0
                )
            ).scalar_subquery().label("price_high")

            price_low = select(func.min(unit_price)).select_from(
                SI.join(C, C.c.CustName == SI.c[si_cust]).join(S, S.c.ArticleCode == SI.c[si_art])
            ).where(
                and_(
                    C.c.CustomerID == Buckets.c.CustomerID,
                    S.c.SKU_ID     == Buckets.c.SKU_ID,
                    SI.c[si_date] <= as_of_date,
                    SI.c[si_qty] > 0.0
                )
            ).scalar_subquery().label("price_low")

            last_price = select(unit_price).select_from(
                SI.join(C, C.c.CustName == SI.c[si_cust]).join(S, S.c.ArticleCode == SI.c[si_art])
            ).where(
                and_(
                    C.c.CustomerID == Buckets.c.CustomerID,
                    S.c.SKU_ID     == Buckets.c.SKU_ID,
                    SI.c[si_date] <= as_of_date,
                    SI.c[si_qty] > 0.0
                )
            ).order_by(SI.c[si_date].desc()).limit(1).scalar_subquery().label("last_price")

            price_note = literal("Prices are indicative averages from Sell-In and may vary by shipment.").label("price_note")
            price_cols = [price_avg, price_high, price_low, last_price, price_note]
    except Exception:
        price_cols = []

    # -------- 9) Final statements --------
    bucket_stmt = (
        select(
            Buckets.c.CustomerID,
            C.c.CustName,
            Buckets.c.SKU_ID,
            S.c.ArticleCode,
            S.c.Description,
            S.c.Brand,

            Buckets.c.b_0_30, Buckets.c.b_31_60, Buckets.c.b_61_90, Buckets.c.b_90p,
            Buckets.c.soh_qty,
            avg_age,

            Buckets.c.oldest_lot_date,
            Buckets.c.newest_lot_date,

            Totals.c.total_receipts,
            Totals.c.total_issues,
            Totals.c.last_sellout_date,
            Totals.c.last_movement_date,
            *price_cols,
        )
        .select_from(base_join)
        .where(and_(*more_pred))
        .order_by(C.c.CustName, S.c.Brand, S.c.ArticleCode)
    )

    layer_stmt = (
        select(
            LiveLayers.c.CustomerID,
            C.c.CustName,
            LiveLayers.c.SKU_ID,
            S.c.ArticleCode,
            S.c.Description,
            S.c.Brand,
            LiveLayers.c.lot_date,
            LiveLayers.c.remaining_qty,
            LiveLayers.c.age_days,
        )
        .select_from(
            LiveLayers.join(C, C.c.CustomerID == LiveLayers.c.CustomerID, isouter=True)
                      .join(S, S.c.SKU_ID == LiveLayers.c.SKU_ID, isouter=True)
        )
        .where(and_(
            _brand_visibility_clause(S),
            *( [LiveLayers.c.CustomerID == customer_id] if customer_id else [] ),
            *( [LiveLayers.c.SKU_ID == sku_id] if sku_id else [] ),
            *( [S.c.ArticleCode == article_code] if article_code else [] ),
        ))
        .order_by(LiveLayers.c.CustomerID, LiveLayers.c.SKU_ID, LiveLayers.c.lot_date, LiveLayers.c.remaining_qty.desc())
    )

    return bucket_stmt, layer_stmt

# ----------------------------
# Routes
# ----------------------------

@fifo_aging_bp.route("/", methods=["GET"])
def page():
    """Render the aging page with comprehensive filters."""
    as_of_date = _parse_date(request.args.get("as_of_date"), default=date.today())
    return render_template("./reports/fifo_aging_report.html", as_of_date=as_of_date)

@fifo_aging_bp.route("/data", methods=["GET"])
def data():
    as_of_date  = _parse_date(request.args.get("as_of_date"), default=date.today())
    customer_id = _parse_int(request.args.get("customer_id"))
    sku_id      = _parse_int(request.args.get("sku_id"))
    article     = request.args.get("article_code") or None
    brand       = request.args.get("brand") or None

    sales_office = _listify(request.args.get("sales_office"))
    sales_group  = _listify(request.args.get("sales_group"))

    min_age      = _parse_int(request.args.get("min_age_days"))
    max_age      = _parse_int(request.args.get("max_age_days"))
    only_pos     = (request.args.get("only_positive_soh") == "1")

    bucket_stmt, _ = build_fifo_ctes(
        as_of_date, customer_id, sku_id, article, brand,
        sales_office, sales_group, min_age, max_age, only_pos
    )

    rows = model.execute(bucket_stmt).mappings().all()
    return jsonify([dict(r) for r in rows])

@fifo_aging_bp.route("/detail", methods=["GET"])
def detail():
    as_of_date  = _parse_date(request.args.get("as_of_date"), default=date.today())
    customer_id = _parse_int(request.args.get("customer_id"))
    sku_id      = _parse_int(request.args.get("sku_id"))
    article     = request.args.get("article_code") or None
    brand       = request.args.get("brand") or None

    _, layer_stmt = build_fifo_ctes(
        as_of_date, customer_id, sku_id, article, brand,
        sales_office=[], sales_group=[],
        min_age=_parse_int(request.args.get("min_age_days")),
        max_age=_parse_int(request.args.get("max_age_days")),
        only_positive_soh=False,
    )

    rows = model.execute(layer_stmt).mappings().all()
    return jsonify([dict(r) for r in rows])

@fifo_aging_bp.route("/export.csv", methods=["GET"])
def export_csv():
    as_of_date  = _parse_date(request.args.get("as_of_date"), default=date.today())
    customer_id = _parse_int(request.args.get("customer_id"))
    sku_id      = _parse_int(request.args.get("sku_id"))
    article     = request.args.get("article_code") or None
    brand       = request.args.get("brand") or None

    sales_office = _listify(request.args.get("sales_office"))
    sales_group  = _listify(request.args.get("sales_group"))

    min_age      = _parse_int(request.args.get("min_age_days"))
    max_age      = _parse_int(request.args.get("max_age_days"))
    only_pos     = (request.args.get("only_positive_soh") == "1")

    bucket_stmt, _ = build_fifo_ctes(
        as_of_date, customer_id, sku_id, article, brand,
        sales_office, sales_group, min_age, max_age, only_pos
    )

    rows = model.execute(bucket_stmt).mappings().all()
    data = [dict(r) for r in rows]

    # CSV
    si = io.StringIO()
    writer = csv.writer(si)
    headers = [
        "CustomerID","CustName","SKU_ID","ArticleCode","Description","Brand",
        "b_0_30","b_31_60","b_61_90","b_90p","soh_qty","avg_age_days",
        "oldest_lot_date","newest_lot_date",
        "total_receipts","total_issues","last_sellout_date","last_movement_date",
        # price metrics (present when SI columns exist)
        "avg_price","price_high","price_low","last_price"
    ]
    writer.writerow(headers)
    for r in data:
        writer.writerow([r.get(h) for h in headers])

    output = si.getvalue()
    filename = f"fifo_aging_{as_of_date.isoformat()}.csv"
    return Response(
        output,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
