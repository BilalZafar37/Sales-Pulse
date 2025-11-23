from flask import Blueprint, request, jsonify, render_template, Response
from datetime import date, datetime
from sqlalchemy import func, or_, and_
from zoneinfo import ZoneInfo
from config import STATIC_DIR

# import your shared session and models
from models import (
    model,
    SP_InventoryLedger,
    SP_SOH_Uploads, SP_SOH_Detail,
    SP_Customer, SP_SKU, SP_Customer_SKU_Map,
    SP_CategoriesMappingMain
)

bp = Blueprint("overselling", __name__, static_folder=STATIC_DIR, url_prefix="/overselling")

# ---- constants you can adjust ----
TZ_RIYADH = ZoneInfo("Asia/Riyadh")
EPS = 1e-9
MOVT_SELLIN  = "SELLIN"
MOVT_SELLOUT = "SELLOUT"
MOVT_ADJUST  = "ADJUST"   # initial SOH is positive


# ============== helpers ==============

SAFE_CHUNK = 800  # stay far under 2100 param limit

def _load_skus_by_id(sku_ids):
    out = {}
    ids = list(sku_ids)
    for i in range(0, len(ids), SAFE_CHUNK):
        batch = ids[i:i+SAFE_CHUNK]
        for s in model.query(SP_SKU).filter(SP_SKU.SKU_ID.in_(batch)).all():
            out[s.SKU_ID] = s
    return out

def _load_customers_by_id(cust_ids):
    out = {}
    ids = list(cust_ids)
    for i in range(0, len(ids), SAFE_CHUNK):
        batch = ids[i:i+SAFE_CHUNK]
        for c in model.query(SP_Customer).filter(SP_Customer.CustomerID.in_(batch)).all():
            out[c.CustomerID] = c
    return out

def _load_categories_by_id(cat_ids):
    out = {}
    ids = list(cat_ids)
    for i in range(0, len(ids), SAFE_CHUNK):
        batch = ids[i:i+SAFE_CHUNK]
        for c in model.query(SP_CategoriesMappingMain).filter(SP_CategoriesMappingMain.ID.in_(batch)).all():
            out[c.ID] = c
    return out

def _load_custsku_map(cust_ids, sku_ids, c_chunk=300, s_chunk=300):
    """
    Chunk BOTH dimensions; returns dict[(CustomerID, SKU_ID)] -> CustSKUCode.
    """
    out = {}
    cust_ids = list(cust_ids)
    sku_ids  = list(sku_ids)
    for ci in range(0, len(cust_ids), c_chunk):
        c_batch = cust_ids[ci:ci+c_chunk]
        for si in range(0, len(sku_ids), s_chunk):
            s_batch = sku_ids[si:si+s_chunk]
            q = (model.query(SP_Customer_SKU_Map)
                    .filter(SP_Customer_SKU_Map.CustomerID.in_(c_batch),
                            SP_Customer_SKU_Map.SKU_ID.in_(s_batch)))
            for m in q.all():
                out[(m.CustomerID, m.SKU_ID)] = m.CustSKUCode
    return out


def _parse_date(s: str | None, default: date) -> date:
    try:
        return datetime.strptime((s or "").strip(), "%Y-%m-%d").date()
    except Exception:
        return default

def _latest_active_snapshot(customer_id: int, brand: str | None, sku_id: int, as_of: date):
    """
    (snap_date, snap_qty) from the latest ACTIVE snapshot for (customer, brand?, sku)
    with SOHDate <= as_of. Returns (None, 0.0) if none found.
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

def _sum_ledger_between(customer_id: int, sku_id: int, since_excl: date | None, until_incl: date, movement_type: str | None = None) -> float:
    """
    Sum signed ledger Qty for (customer, sku) in (since_excl, until_incl].
    If since_excl is None, sum from 'beginning' up to until_incl.
    Optionally filter by MovementType.
    """
    q = (model.query(func.coalesce(func.sum(SP_InventoryLedger.Qty), 0.0))
         .filter(SP_InventoryLedger.CustomerID == customer_id,
                 SP_InventoryLedger.SKU_ID == sku_id,
                 SP_InventoryLedger.DocDate <= until_incl))
    if since_excl:
        q = q.filter(SP_InventoryLedger.DocDate > since_excl)
    if movement_type:
        q = q.filter(SP_InventoryLedger.MovementType == movement_type)
    return float(q.scalar() or 0.0)

def _current_soh(customer_id: int, sku_id: int, as_of: date) -> tuple[float, float, float, date | None]:
    """
    Returns (soh, sellin_sum, sellout_abs, start_date) where:
    - start_date is the customer-level earliest ADJUST date (inclusive).
    - If no ADJUST exists for the customer, returns (0,0,0,None) and the caller should skip it.
    """
    start_date = _customer_adjust_date(customer_id, as_of)
    if not start_date:
        return 0.0, 0.0, 0.0, None

    # totals in [start_date .. as_of]
    sellin  = _sum_ledger_between_inclusive(customer_id, sku_id, start_date, as_of, MOVT_SELLIN)
    sellout = _sum_ledger_between_inclusive(customer_id, sku_id, start_date, as_of, MOVT_SELLOUT)
    adjust  = _sum_ledger_between_inclusive(customer_id, sku_id, start_date, as_of, MOVT_ADJUST)

    # Current SOH = ADJUST + SELLIN + SELLOUT (+ any other movements if you have them)
    soh = float(adjust + sellin + sellout)  # remember: sellout is negative in your ledger

    return float(soh), float(sellin), abs(float(sellout)), start_date


def _customer_adjust_date(customer_id: int, as_of: date) -> date | None:
    """
    Earliest ADJUST DocDate for this customer (<= as_of).
    If none exists, return None (we treat SOH as 0 and skip negatives).
    """
    q = (model.query(func.min(SP_InventoryLedger.DocDate))
            .filter(
                SP_InventoryLedger.CustomerID == customer_id,
                SP_InventoryLedger.MovementType == MOVT_ADJUST,
                SP_InventoryLedger.DocDate <= as_of
            ))
    return q.scalar()  # None if no ADJUST at all


def _sum_ledger_between_inclusive(customer_id: int, sku_id: int,
                                  since_incl: date, until_incl: date,
                                  movement_type: str | None = None) -> float:
    """
    Sum signed ledger Qty for (customer, sku) in [since_incl, until_incl].
    ADJUST and SELLIN are +ve, SELLOUT is -ve in your ledger.
    """
    q = (model.query(func.coalesce(func.sum(SP_InventoryLedger.Qty), 0.0))
         .filter(
            SP_InventoryLedger.CustomerID == customer_id,
            SP_InventoryLedger.SKU_ID == sku_id,
            SP_InventoryLedger.DocDate >= since_incl,
            SP_InventoryLedger.DocDate <= until_incl
         ))
    if movement_type:
        q = q.filter(SP_InventoryLedger.MovementType == movement_type)
    return float(q.scalar() or 0.0)

# --- add near imports ---
from sqlalchemy import desc

# --- add this helper below _current_soh() ---
def _sellin_price_stats(customer_id: int, sku_id: int, since_incl: date | None, until_incl: date):
    """
    Weighted average / hi / low / last for SELLIN in [since_incl .. until_incl].
    If since_incl is None => no ADJUST => we return (None, None, None, None).
    """
    if since_incl is None:
        return (None, None, None, None)

    has_value     = hasattr(SP_InventoryLedger, "Value")
    has_unitprice = hasattr(SP_InventoryLedger, "UnitPrice")
    if not has_value and not has_unitprice:
        return (None, None, None, None)

    cols = [SP_InventoryLedger.Qty, SP_InventoryLedger.DocDate]
    if has_unitprice: cols.append(SP_InventoryLedger.UnitPrice)
    if has_value:     cols.append(SP_InventoryLedger.Value)

    q = (model.query(*cols)
         .filter(
             SP_InventoryLedger.CustomerID == customer_id,
             SP_InventoryLedger.SKU_ID == sku_id,
             SP_InventoryLedger.MovementType == MOVT_SELLIN,
             SP_InventoryLedger.DocDate >= since_incl,   # inclusive
             SP_InventoryLedger.DocDate <= until_incl
         ))

    rows = q.order_by(SP_InventoryLedger.DocDate.asc()).all()
    if not rows:
        return (None, None, None, None)

    sum_qty = sum_pxq = 0.0
    hi = lo = last_price = None
    last_date = None

    for row in rows:
        if has_unitprice and has_value:
            qty, docdate, unit_price, total_value = row
        elif has_unitprice:
            qty, docdate, unit_price = row; total_value = None
        else:
            qty, docdate, total_value = row; unit_price = None

        qty = float(qty or 0.0)
        if unit_price is not None:
            up = float(unit_price)
        elif total_value is not None and qty:
            up = float(total_value) / qty
        else:
            continue

        if qty > 0:
            sum_qty += qty
            sum_pxq += qty * up

        hi = up if (hi is None or up > hi) else hi
        lo = up if (lo is None or up < lo) else lo
        if last_date is None or docdate > last_date:
            last_date = docdate; last_price = up

    avg_price = (sum_pxq / sum_qty) if sum_qty > 0 else (last_price or hi or lo)
    return (avg_price, hi, lo, last_price)


def _paginate(query, page: int, page_size: int):
    total = query.count()
    rows  = (query.offset((page-1)*page_size).limit(page_size).all())
    return total, rows


# Build candidate (CustomerID, SKU_ID) pairs to evaluate for negativity
def _candidate_pairs(brand: str | None, customer_id: int | None, category_id: int | None, catcode: str | None):
    pairs = set()

    # from snapshots (active)
    q_snap = (model.query(SP_SOH_Uploads.CustomerID, SP_SOH_Detail.SKU_ID)
              .join(SP_SOH_Detail, SP_SOH_Detail.SOHUploadID == SP_SOH_Uploads.SOHUploadID)
              .filter(SP_SOH_Detail.IsActive == True))
    if brand:
        q_snap = q_snap.filter(SP_SOH_Uploads.Brand == brand)
    if customer_id:
        q_snap = q_snap.filter(SP_SOH_Uploads.CustomerID == customer_id)
    if category_id or catcode:
        # need to join SKU + categories to filter
        q_snap = (q_snap.join(SP_SKU, SP_SKU.SKU_ID == SP_SOH_Detail.SKU_ID)
                        .outerjoin(SP_CategoriesMappingMain, SP_CategoriesMappingMain.ID == SP_SKU.CategoryMappingID))
        if category_id:
            q_snap = q_snap.filter(SP_SKU.CategoryMappingID == category_id)
        if catcode:
            q_snap = q_snap.filter(SP_CategoriesMappingMain.CatCode == catcode)

    for cid, sid in q_snap.distinct().all():
        pairs.add((int(cid), int(sid)))

    # from ledger
    q_led = model.query(SP_InventoryLedger.CustomerID, SP_InventoryLedger.SKU_ID)
    if customer_id:
        q_led = q_led.filter(SP_InventoryLedger.CustomerID == customer_id)
    if brand or category_id or catcode:
        q_led = q_led.join(SP_SKU, SP_SKU.SKU_ID == SP_InventoryLedger.SKU_ID)
        if brand:
            q_led = q_led.filter(SP_SKU.Brand == brand)
        if category_id:
            q_led = q_led.filter(SP_SKU.CategoryMappingID == category_id)
        if catcode:
            q_led = q_led.join(SP_CategoriesMappingMain, SP_CategoriesMappingMain.ID == SP_SKU.CategoryMappingID)\
                         .filter(SP_CategoriesMappingMain.CatCode == catcode)

    for cid, sid in q_led.distinct().all():
        pairs.add((int(cid), int(sid)))

    return list(pairs)


# ============== routes ==============

@bp.route("/", methods=["GET"])
def index():
    # Replace with your actual template if needed
    return render_template("./reports/overselling_report.html")


@bp.route("/api/list", methods=["POST"])
def list_rows():
    """
    JSON in:
    {
      "brand": "Pepsi",            // optional
      "customer_id": 123,          // optional
      "category_id": 7,            // optional
      "catcode": "ABC",            // optional
      "as_of": "2025-08-24",       // optional (defaults today)
      "page": 1, "page_size": 50   // optional
    }
    """
    data = request.get_json(force=True) if request.is_json else {}
    brand       = (data.get("brand") or "").strip() or None
    customer_id = data.get("customer_id", None)
    category_id = data.get("category_id", None)
    catcode     = (data.get("catcode") or "").strip() or None
    as_of       = _parse_date(data.get("as_of"), date.today())
    page        = max(1, int(data.get("page") or 1))
    page_size   = min(200, int(data.get("page_size") or 50))

    # 1) build candidates limited by filters
    pairs = _candidate_pairs(brand, customer_id, category_id, catcode)
    if not pairs:
        return jsonify(ok=True, total=0, items=[])

    # 2) prefetch SKU & Customer & Category & CustSKU maps for the pairs
    # 2) prefetch SKU & Customer & Category & CustSKU maps for the pairs
    sku_ids  = sorted({sid for (_cid, sid) in pairs})
    cust_ids = sorted({cid for (cid, _sid) in pairs})
    
    sku_by_id  = _load_skus_by_id(sku_ids)
    cust_by_id = _load_customers_by_id(cust_ids)
    
    cat_ids   = sorted({s.CategoryMappingID for s in sku_by_id.values() if s.CategoryMappingID})
    cat_by_id = _load_categories_by_id(cat_ids)
    
    # !!! THIS replaces the crashing query
    csku_map  = _load_custsku_map(cust_ids, sku_ids)
    

    # 3) compute SOH and totals; collect only negative
    rows = []
    for cid, sid in pairs:
        s = sku_by_id.get(sid)
        if not s:
            continue
    
        soh, sellin, sellout_abs, start_date = _current_soh(cid, sid, as_of)
        if start_date is None:
            # No ADJUST for this customer => calculations shouldn't run => skip
            continue
    
        avg_price, hi_price, lo_price, last_price = _sellin_price_stats(cid, sid, start_date, as_of)
        sellin_value = (float(sellin) * float(avg_price)) if (avg_price is not None) else None
    
        if soh < 0 - EPS:
            cust = cust_by_id.get(cid)
            cat  = cat_by_id.get(s.CategoryMappingID) if s.CategoryMappingID else None
            rows.append({
                "Brand": s.Brand or "",
                "Customer": f"{(cust.CustName if cust else '')} ({(cust.CustCode if cust else '')})",
                "Category": ("{} — {}".format(cat.CatName or "", cat.CatDesc or "").strip(" —") if cat else ""),
                "CustSKU": csku_map.get((cid, sid), ""),
                "MECSKU": s.ArticleCode or "",
                "SellIn": float(sellin),
                "SellOut": float(sellout_abs),
                "CurrentSOH": float(soh),
                "PriceAvg": float(avg_price) if avg_price is not None else None,
                "PriceHi":  float(hi_price)  if hi_price  is not None else None,
                "PriceLo":  float(lo_price)  if lo_price  is not None else None,
                "PriceLast":float(last_price)if last_price is not None else None,
                "SellInValue": float(sellin_value) if sellin_value is not None else None,
                "CustomerID": cid,
                "SKU_ID": sid
            })
    
    
    # 4) sort & paginate in-memory (safe because we've filtered to negatives only)
    rows.sort(key=lambda r: (r["CurrentSOH"], r["Brand"], r["Customer"]))
    total = len(rows)
    start = (page - 1) * page_size
    end   = start + page_size
    page_rows = rows[start:end]

    return jsonify(ok=True, total=total, page=page, page_size=page_size, items=page_rows)


@bp.route("/api/export", methods=["POST"])
def export_csv():
    """
    Export CSV using same filters as /api/list.
    Accepts same JSON payload.
    """
    data = request.get_json(force=True) if request.is_json else {}
    # Reuse the list endpoint logic but without pagination
    data["page"] = 1
    data["page_size"] = 1000000  # effectively all
    with bp.test_request_context(json=data):  # reuse handler
        resp_json = list_rows().json

    if not resp_json.get("ok"):
        return jsonify(ok=False, error="Failed to build export"), 400

    cols = ["Brand", "Customer", "Category", "CustSKU", "MECSKU", "SellIn", "SellOut", "CurrentSOH"]
    lines = [",".join(cols)]
    for r in resp_json.get("items", []):
        row = [
            str(r.get("Brand","")).replace(",", " "),
            str(r.get("Customer","")).replace(",", " "),
            str(r.get("Category","")).replace(",", " "),
            str(r.get("CustSKU","")).replace(",", " "),
            str(r.get("MECSKU","")).replace(",", " "),
            f'{r.get("SellIn",0):.2f}',
            f'{r.get("SellOut",0):.2f}',
            f'{r.get("CurrentSOH",0):.2f}',
        ]
        lines.append(",".join(row))

    csv_data = "\n".join(lines)
    fname = f"overselling_{datetime.now(TZ_RIYADH).strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        csv_data,
        headers={
            "Content-Type": "text/csv; charset=utf-8",
            "Content-Disposition": f'attachment; filename="{fname}"'
        }
    )
