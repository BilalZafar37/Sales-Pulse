# sales_pulse_general.py
from flask import Blueprint, request, jsonify, render_template, Response, current_app
from datetime import date, datetime, timedelta
from sqlalchemy import func
from zoneinfo import ZoneInfo
from config import STATIC_DIR
from itertools import islice

from models import (
    model, case, Date, cast,
    SP_InventoryLedger,
    SP_SOH_Uploads, SP_SOH_Detail,
    SP_Customer, SP_SKU, SP_Customer_SKU_Map,
    SP_CategoriesMappingMain,
)

bp = Blueprint("sales_pulse_general", __name__, static_folder=STATIC_DIR, url_prefix="/sales-pulse-general")

# ---- constants ----
TZ_RIYADH = ZoneInfo("Asia/Riyadh")
EPS = 1e-9
MOVT_SELLIN  = "SELLIN"
MOVT_SELLOUT = "SELLOUT"


# ================== helpers ==================

def _chunks(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk

def _docdate_date():
    return cast(SP_InventoryLedger.DocDate, Date)

def _parse_date(s: str | None) -> date | None:
    if not s: return None
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except Exception:
        return None

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

def _sum_signed_after_date(customer_id: int, sku_id: int, day: date, until_incl: date) -> float:
    """Sum SIGNED for DocDate > day and <= until_incl."""
    signed = _signed_qty_expr()
    D = _docdate_date()
    q = (model.query(func.coalesce(func.sum(signed), 0.0))
         .filter(SP_InventoryLedger.CustomerID == customer_id,
                 SP_InventoryLedger.SKU_ID == sku_id,
                 D > day, D <= until_incl))
    return float(q.scalar() or 0.0)

def _sum_signed_on_date_excl_adjust(customer_id: int, sku_id: int, day: date) -> float:
    """Sum SIGNED for DocDate == day, excluding ADJUST rows."""
    signed = _signed_qty_expr()
    D = _docdate_date()
    q = (model.query(func.coalesce(func.sum(signed), 0.0))
         .filter(SP_InventoryLedger.CustomerID == customer_id,
                 SP_InventoryLedger.SKU_ID == sku_id,
                 D == day,
                 SP_InventoryLedger.MovementType != 'ADJUST'))
    return float(q.scalar() or 0.0)

def _sum_signed_inclusive(customer_id: int, sku_id: int, start_incl: date, end_incl: date) -> float:
    """Sum SIGNED for start<=DocDate<=end."""
    signed = _signed_qty_expr()
    D = _docdate_date()
    q = (model.query(func.coalesce(func.sum(signed), 0.0))
         .filter(SP_InventoryLedger.CustomerID == customer_id,
                 SP_InventoryLedger.SKU_ID == sku_id,
                 D >= start_incl, D <= end_incl))
    return float(q.scalar() or 0.0)

def _sum_movement_abs(customer_id:int, sku_id:int,
                      start_date:date|None, end_date:date|None,
                      movement_type:str) -> float:
    """
    Sum ABS(Qty) for rows of a specific MovementType in [start_date, end_date].
    Safe even if data has mixed signs.
    """
    
    if movement_type == 'SELLIN':
        qsum = func.coalesce(func.sum(
            case((SP_InventoryLedger.Qty > 0, SP_InventoryLedger.Qty), else_=0.0)
        ), 0.0)
    else:
        qsum = func.coalesce(func.sum(func.abs(SP_InventoryLedger.Qty)), 0.0)
    
    # qabs = func.coalesce(func.sum(func.abs(SP_InventoryLedger.Qty)), 0.0)
    q = (model.query(qsum)
         .filter(SP_InventoryLedger.CustomerID==customer_id,
                 SP_InventoryLedger.SKU_ID==sku_id,
                 SP_InventoryLedger.MovementType==movement_type))
    if start_date:
        q = q.filter(SP_InventoryLedger.DocDate >= start_date)
    if end_date:
        q = q.filter(SP_InventoryLedger.DocDate <= end_date)
    return float(q.scalar() or 0.0)

def _sum_returns_abs(customer_id:int, sku_id:int,
                     start_date:date|None, end_date:date|None) -> float:
    q = (model.query(func.coalesce(func.sum(func.abs(SP_InventoryLedger.Qty)), 0.0))
         .filter(SP_InventoryLedger.CustomerID == customer_id,
                 SP_InventoryLedger.SKU_ID == sku_id,
                 SP_InventoryLedger.MovementType == 'SELLIN',
                 SP_InventoryLedger.Qty < 0))
    if start_date:
        q = q.filter(SP_InventoryLedger.DocDate >= start_date)
    if end_date:
        q = q.filter(SP_InventoryLedger.DocDate <= end_date)
    return float(q.scalar() or 0.0)

# def _debug_soh_parts(customer_id:int, sku_id:int, as_of:date):
#     anchor = _anchor_adjust_date_sku(customer_id, sku_id, as_of)
#     D = _docdate_date()
#     signed = _signed_qty_expr()
#     base_qty, had_snap = _snapshot_qty_on_date(customer_id, sku_id, anchor) if anchor else (0.0, False)
#     same_day_non_adj = _sum_signed_on_date_excl_adjust(customer_id, sku_id, anchor) if anchor else 0.0
#     after_anchor     = _sum_signed_after_date(customer_id, sku_id, anchor, as_of) if anchor else 0.0
#     return dict(anchor=anchor, base_qty=base_qty, had_snap=had_snap,
#                 same_day_non_adj=same_day_non_adj, after_anchor=after_anchor)

def _anchor_adjust_date_customer(customer_id: int, as_of: date) -> date | None:
    """Latest ADJUST date for any SKU of this customer on/before as_of."""
    return (model.query(func.max(cast(SP_InventoryLedger.DocDate, Date)))
            .filter(SP_InventoryLedger.CustomerID == customer_id,
                    SP_InventoryLedger.MovementType == 'ADJUST',
                    SP_InventoryLedger.DocDate <= as_of)
            .scalar())

def _anchor_adjust_date_sku_only(customer_id: int, sku_id: int, as_of: date) -> date | None:
    """Latest ADJUST date for this specific SKU on/before as_of."""
    return (model.query(func.max(cast(SP_InventoryLedger.DocDate, Date)))
            .filter(SP_InventoryLedger.CustomerID == customer_id,
                    SP_InventoryLedger.SKU_ID == sku_id,
                    SP_InventoryLedger.MovementType == 'ADJUST',
                    SP_InventoryLedger.DocDate <= as_of)
            .scalar())

def _effective_anchor_for_sku(customer_id: int, sku_id: int, as_of: date) -> date | None:
    """
    Effective anchor = max(customer_anchor, sku_anchor) if both exist.
    If only one exists, use it. If none exist, return None.
    """
    ca = _anchor_adjust_date_customer(customer_id, as_of)
    sa = _anchor_adjust_date_sku_only(customer_id, sku_id, as_of)
    if ca and sa:
        return max(ca, sa)
    return sa or ca  # whichever exists, or None


def _signed_qty_expr():
    mt = SP_InventoryLedger.MovementType
    q  = func.coalesce(SP_InventoryLedger.Qty, 0.0)
    return case(
        (mt == 'SELLOUT', -func.abs(q)),
        (mt == 'SELLIN',   q),
        (mt == 'ADJUST',   q),          # allow positive/negative adjustments
        else_=q                         # fallback: keep as-is
    )

def _sellin_price_stats_in_range(customer_id: int, sku_id: int,
                                 start_date: date | None, end_date: date | None):
    """
    Returns (avg_price, hi_price, lo_price, last_price) for SELLIN movements in [start_date, end_date].
    Average is *weighted* by positive quantities.
    Adjust field names if your ledger uses different ones than UnitPrice / Value.
    """
    # Adjust these two lines if your ledger uses different names:
    unit_price_col = getattr(SP_InventoryLedger, "UnitPrice", None)
    value_col      = getattr(SP_InventoryLedger, "Value", None)

    cols = [SP_InventoryLedger.Qty, SP_InventoryLedger.DocDate]
    if unit_price_col is not None: cols.append(unit_price_col.label("UnitPrice"))
    if value_col is not None:      cols.append(value_col.label("Value"))

    q = (model.query(*cols)
         .filter(SP_InventoryLedger.CustomerID==customer_id,
                 SP_InventoryLedger.SKU_ID==sku_id,
                 SP_InventoryLedger.MovementType==MOVT_SELLIN))
    if start_date:
        q = q.filter(SP_InventoryLedger.DocDate >= start_date)
    if end_date:
        q = q.filter(SP_InventoryLedger.DocDate <= end_date)

    rows = q.order_by(SP_InventoryLedger.DocDate.asc()).all()
    if not rows:
        return (None, None, None, None)

    sum_qty = 0.0
    sum_pxq = 0.0
    hi = None
    lo = None
    last_price = None
    last_dt = None

    for row in rows:
        # row unpacking based on columns list:
        # always: Qty, DocDate
        qty = float(row[0] or 0.0)
        docdate = row[1]
        unit_price = None
        total_value = None
        if unit_price_col is not None and value_col is not None and len(row) >= 4:
            unit_price = row[2]
            total_value = row[3]
        elif unit_price_col is not None and len(row) >= 3:
            unit_price = row[2]
        elif value_col is not None and len(row) >= 3:
            total_value = row[2]

        if unit_price is not None:
            up = float(unit_price)
        elif total_value is not None and qty:
            up = float(total_value) / qty
        else:
            # no pricing info -> skip line
            continue

        if qty > 0:
            sum_qty += qty
            sum_pxq += qty * up

        hi = up if (hi is None or up > hi) else hi
        lo = up if (lo is None or up < lo) else lo

        if (last_dt is None) or (docdate > last_dt):
            last_dt = docdate
            last_price = up

    avg_price = (sum_pxq / sum_qty) if sum_qty > 0 else (last_price or hi or lo)
    return (avg_price, hi, lo, last_price)

def _candidate_pairs(brand: str | None, customer_id: int | None,
                     category_id: int | None, catcode: str | None,
                     start_date: date | None, end_date: date | None):
    """
    Build set of (CustomerID, SKU_ID) pairs that:
      - Have any ledger movement inside [start_date, end_date], OR
      - Have an active snapshot on/before end_date (so we can show closing SOH),
    filtered by brand/category if provided.
    """
    pairs = set()

    # ledger inside window
    q_led = model.query(SP_InventoryLedger.CustomerID, SP_InventoryLedger.SKU_ID)
    if customer_id:
        q_led = q_led.filter(SP_InventoryLedger.CustomerID == customer_id)
    if start_date:
        q_led = q_led.filter(SP_InventoryLedger.DocDate >= start_date)
    if end_date:
        q_led = q_led.filter(SP_InventoryLedger.DocDate <= end_date)
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

    # snapshots for closing balance (up to end_date)
    if end_date:
        q_snap = (model.query(SP_SOH_Uploads.CustomerID, SP_SOH_Detail.SKU_ID)
                  .join(SP_SOH_Detail, SP_SOH_Detail.SOHUploadID == SP_SOH_Uploads.SOHUploadID)
                  .filter(SP_SOH_Detail.IsActive == True,
                          SP_SOH_Detail.SOHDate <= end_date))
        if brand:
            q_snap = q_snap.filter(SP_SOH_Uploads.Brand == brand)
        if customer_id:
            q_snap = q_snap.filter(SP_SOH_Uploads.CustomerID == customer_id)
        if category_id or catcode:
            q_snap = (q_snap.join(SP_SKU, SP_SKU.SKU_ID == SP_SOH_Detail.SKU_ID)
                           .outerjoin(SP_CategoriesMappingMain, SP_CategoriesMappingMain.ID == SP_SKU.CategoryMappingID))
            if category_id:
                q_snap = q_snap.filter(SP_SKU.CategoryMappingID == category_id)
            if catcode:
                q_snap = q_snap.filter(SP_CategoriesMappingMain.CatCode == catcode)

        for cid, sid in q_snap.distinct().all():
            pairs.add((int(cid), int(sid)))

    return list(pairs)

# def _anchor_adjust_date_sku(customer_id: int, sku_id: int, as_of: date) -> date | None:
#     return (model.query(func.max(cast(SP_InventoryLedger.DocDate, Date)))
#             .filter(SP_InventoryLedger.CustomerID == customer_id,
#                     SP_InventoryLedger.SKU_ID == sku_id,
#                     SP_InventoryLedger.MovementType == 'ADJUST',
#                     SP_InventoryLedger.DocDate <= as_of)
#             .scalar())

def _anchor_adjust_date_customer(customer_id: int, as_of: date) -> date | None:
    """Latest ADJUST date for *any* SKU of this customer on/before as_of."""
    return (model.query(func.max(cast(SP_InventoryLedger.DocDate, Date)))
            .filter(SP_InventoryLedger.CustomerID == customer_id,
                    SP_InventoryLedger.MovementType == 'ADJUST',
                    SP_InventoryLedger.DocDate <= as_of)
            .scalar())


def _snapshot_qty_on_date(customer_id:int, sku_id:int, snap_date:date) -> tuple[float, bool]:
    q = (model.query(SP_SOH_Detail.SOHQty)
         .join(SP_SOH_Uploads, SP_SOH_Uploads.SOHUploadID == SP_SOH_Detail.SOHUploadID)
         .filter(SP_SOH_Uploads.CustomerID == customer_id,
                 SP_SOH_Detail.SKU_ID == sku_id,
                 SP_SOH_Detail.IsActive == True,
                 cast(SP_SOH_Detail.SOHDate, Date) == snap_date)
         .order_by(SP_SOH_Uploads.SOHUploadID.desc()))
    row = q.first()
    return ((float(row[0] or 0.0), True) if row else (0.0, False))

def _initial_soh_in_window(customer_id: int, sku_id: int,
                           start_incl: date | None, end_incl: date | None) -> tuple[float | None, date | None]:
    """
    Return (InitialSOH, InitialSOHDate) for the earliest ADJUST in [start_incl, end_incl].
    If no ADJUST exists in the window, return (None, None).
    """
    if not start_incl or not end_incl:
        return (None, None)

    q = (model.query(SP_InventoryLedger.Qty, SP_InventoryLedger.DocDate)
         .filter(SP_InventoryLedger.CustomerID == customer_id,
                 SP_InventoryLedger.SKU_ID == sku_id,
                 SP_InventoryLedger.MovementType == 'ADJUST',
                 SP_InventoryLedger.DocDate >= start_incl,
                 SP_InventoryLedger.DocDate <= end_incl)
         .order_by(SP_InventoryLedger.DocDate.asc(), SP_InventoryLedger.LedgerID.asc())
        )
    row = q.first()
    return ((float(row[0] or 0.0), row[1]) if row else (None, None))

def _sum_consumers_since_anchor(customer_id:int, sku_id:int,
                                anchor_date:date, until_incl:date) -> float:
    D = _docdate_date()

    # 1) All SELLOUT (absolute)
    q1 = (model.query(func.coalesce(func.sum(func.abs(SP_InventoryLedger.Qty)), 0.0))
          .filter(SP_InventoryLedger.CustomerID == customer_id,
                  SP_InventoryLedger.SKU_ID == sku_id,
                  SP_InventoryLedger.MovementType == 'SELLOUT',
                  D >= anchor_date, D <= until_incl))
    selout_abs = float(q1.scalar() or 0.0)

    # 2) All RETURNS posted as negative SELLIN
    q2 = (model.query(func.coalesce(func.sum(func.abs(SP_InventoryLedger.Qty)), 0.0))
          .filter(SP_InventoryLedger.CustomerID == customer_id,
                  SP_InventoryLedger.SKU_ID == sku_id,
                  SP_InventoryLedger.MovementType == 'SELLIN',
                  SP_InventoryLedger.Qty < 0,   # negative SELLIN
                  D >= anchor_date, D <= until_incl))
    returns_abs = float(q2.scalar() or 0.0)

    return selout_abs + returns_abs

def _initial_bucket_numbers(customer_id: int, sku_id: int, as_of: date):
    """
    Initial bucket from the *effective* anchor. If the SKU has no snapshot or ADJUST on the
    anchor day, its Initial SOH = 0 at that anchor (as intended for SKUs without their own ADJUST).
    """
    anchor_date = _effective_anchor_for_sku(customer_id, sku_id, as_of)
    if not anchor_date:
        return {
            "InitialSOHDate": None,
            "InitialSOHTotal": None,
            "InitialSOHConsumed": None,
            "InitialSOHBalance": None,
            "SellOutSinceAnchor": None,
        }

    # Consumers since anchor (SELLOUT abs + negative SELLIN abs)
    consumers_abs = _sum_consumers_since_anchor(customer_id, sku_id, anchor_date, as_of)

    # Establish this SKU’s opening on the anchor day
    base_qty, had_snap = _snapshot_qty_on_date(customer_id, sku_id, anchor_date)
    if had_snap:
        init_total = base_qty + _sum_signed_on_date_excl_adjust(customer_id, sku_id, anchor_date)
    else:
        # If there was an ADJUST for this SKU *on* the anchor day, include it; else zero baseline
        D = _docdate_date()
        has_adj_on_anchor = (model.query(func.count())
                             .filter(SP_InventoryLedger.CustomerID == customer_id,
                                     SP_InventoryLedger.SKU_ID == sku_id,
                                     SP_InventoryLedger.MovementType == 'ADJUST',
                                     D == anchor_date).scalar() or 0) > 0
        init_total = _sum_signed_inclusive(customer_id, sku_id, anchor_date, anchor_date) if has_adj_on_anchor else 0.0

    consumed = min(consumers_abs, init_total)
    balance  = max(0.0, init_total - consumed)

    return {
        "InitialSOHDate": anchor_date,
        "InitialSOHTotal": float(init_total),
        "InitialSOHConsumed": float(consumed),
        "InitialSOHBalance": float(balance),
        "SellOutSinceAnchor": float(consumers_abs),
    }



# ================== routes ==================

@bp.route("/", methods=["GET"])
def index():
    """
    Render the report page (build a template at templates/sales_pulse_general/index.html).
    """
    return render_template("./reports/sales_pulse_general.html")


@bp.route("/api/list", methods=["POST"])
def list_rows():
    """
    JSON in:
    {
      "brand": "Pepsi",               // optional
      "customer_id": 123,             // optional
      "category_id": 7,               // optional
      "catcode": "ABC",               // optional
      "date_from": "2025-07-01",      // optional
      "date_to":   "2025-07-31",      // optional
      "page": 1, "page_size": 100     // optional
    }

    Notes:
    - Sell-in / Sell-out are summed *within* [date_from, date_to].
    - SOH is the closing stock as of date_to (snapshot<=date_to + ledger up to date_to).
    - Price stats (avg/high/low/last) come from SELLIN inside [date_from, date_to].
    """
    
    def _closing_soh_anchored(customer_id: int, sku_id: int, as_of: date) -> float:
        """
        SOH as of as_of using effective anchor (customer go-live clamped with SKU's own ADJUST).
        If no anchor at all: use latest per-SKU snapshot fallback.
        """
        anchor = _effective_anchor_for_sku(customer_id, sku_id, as_of)
        if anchor:
            base_qty, had_snap = _snapshot_qty_on_date(customer_id, sku_id, anchor)
            if had_snap:
                same_day_non_adjust = _sum_signed_on_date_excl_adjust(customer_id, sku_id, anchor)
                after_anchor        = _sum_signed_after_date(customer_id, sku_id, anchor, as_of)
                return float(base_qty) + float(same_day_non_adjust) + float(after_anchor)
            else:
                # Start from 0 baseline at anchor and add movements since then (includes any ADJUST >= anchor)
                return _sum_signed_inclusive(customer_id, sku_id, anchor, as_of)
    
        # No customer/SKU ADJUST at all → fallback to latest snapshot then movements after it
        snap_date, snap_qty = _latest_active_snapshot(customer_id, None, sku_id, as_of)
        if snap_date:
            D = _docdate_date()
            signed = _signed_qty_expr()
            delta = float((model.query(func.coalesce(func.sum(signed), 0.0))
                           .filter(SP_InventoryLedger.CustomerID == customer_id,
                                   SP_InventoryLedger.SKU_ID == sku_id,
                                   D > snap_date, D <= as_of)).scalar() or 0.0)
            return float(snap_qty) + delta
        return 0.0
    
    
    data = request.get_json(force=True) if request.is_json else {}
    brand       = (data.get("brand") or "").strip() or None
    customer_id = data.get("customer_id", None)
    category_id = data.get("category_id", None)
    catcode     = (data.get("catcode") or "").strip() or None
    date_from   = _parse_date(data.get("date_from"))
    date_to     = _parse_date(data.get("date_to")) or date.today()

    page      = max(1, int(data.get("page") or 1))
    page_size = min(500, int(data.get("page_size") or 100))

    # 1) candidate pairs based on filters and window
    pairs = _candidate_pairs(brand, customer_id, category_id, catcode, date_from, date_to)
    if not pairs:
        return jsonify(ok=True, total=0, items=[])

    # 2) prefetch (BATCHED to avoid SQL Server's 2100-parameter limit)
    MAX_PARAMS = 900  # headroom for safety; keep each IN-list well under 2100
    
    sku_ids  = sorted({sid for (cid, sid) in pairs})
    cust_ids = sorted({cid for (cid, sid) in pairs})
    
    # ---- SP_SKU ----
    skus = []
    for ch in _chunks(sku_ids, MAX_PARAMS):
        skus.extend(model.query(SP_SKU).filter(SP_SKU.SKU_ID.in_(ch)).all())
    sku_by_id = {s.SKU_ID: s for s in skus}
    
    # ---- SP_Customer ----
    customers = []
    for ch in _chunks(cust_ids, MAX_PARAMS):
        customers.extend(model.query(SP_Customer).filter(SP_Customer.CustomerID.in_(ch)).all())
    cust_by_id = {c.CustomerID: c for c in customers}
    
    # ---- Categories (from skus we already fetched) ----
    cat_ids = sorted({s.CategoryMappingID for s in skus if getattr(s, "CategoryMappingID", None)})
    cats = []
    for ch in _chunks(cat_ids, MAX_PARAMS):
        cats.extend(model.query(SP_CategoriesMappingMain).filter(SP_CategoriesMappingMain.ID.in_(ch)).all())
    cat_by_id = {c.ID: c for c in cats}
    
    # ---- SP_Customer_SKU_Map (cross-filtered: batch both sides) ----
    maps = []
    for cust_chunk in _chunks(cust_ids, MAX_PARAMS):
        for sku_chunk in _chunks(sku_ids, MAX_PARAMS):
            maps.extend(
                model.query(SP_Customer_SKU_Map)
                     .filter(SP_Customer_SKU_Map.CustomerID.in_(cust_chunk),
                             SP_Customer_SKU_Map.SKU_ID.in_(sku_chunk))
                     .all()
            )
    csku_map = {(m.CustomerID, m.SKU_ID): m.CustSKUCode for m in maps}

    # 3) compute metrics for each pair
    rows = []
    for cid, sid in pairs:
        s = sku_by_id.get(sid)
        if not s:
            continue

        # quantities inside the window
        sellin_qty  = _sum_movement_abs(cid, sid, date_from, date_to, MOVT_SELLIN)
        sellout_qty = _sum_movement_abs(cid, sid, date_from, date_to, MOVT_SELLOUT)

        # closing SOH as of date_to (brand matters for snapshot scoping)
        # closing_soh = _closing_soh_as_of(cid, getattr(s, "Brand", None), sid, date_to)
        # new:
        closing_soh = _closing_soh_anchored(cid, sid, date_to)

        # price stats & sell-in value (inside window)
        avg_p, hi_p, lo_p, last_p = _sellin_price_stats_in_range(cid, sid, date_from, date_to)
        sellin_value = (sellin_qty * avg_p) if (avg_p is not None) else None

        cust = cust_by_id.get(cid)
        cat  = cat_by_id.get(getattr(s, "CategoryMappingID", None)) if getattr(s, "CategoryMappingID", None) else None

        init_soh, init_soh_date = _initial_soh_in_window(cid, sid, date_from, date_to)
        
        # initial bucket metrics (independent of date_from; always from anchor -> date_to)
        init = _initial_bucket_numbers(cid, sid, date_to)
        init_date = init["InitialSOHDate"]
        
        returns_qty = _sum_returns_abs(cid, sid, date_from, date_to)
        
        rows.append({
            "Brand": s.Brand or "",
            "Customer": f"{(cust.CustName if cust else '')} ({(cust.CustCode if cust else '')})",
            "Category": ("{} — {}".format(cat.CatName or "", cat.CatDesc or "").strip(" —") if cat else ""),
            "CustSKU": csku_map.get((cid, sid), ""),
            "MECSKU": s.ArticleCode or "",
            "SellIn": float(sellin_qty),
            "SellOut": float(sellout_qty),
            "Returns": float(returns_qty),            # ← new column
            "SOH": float(closing_soh),

            "InitialSOH": (None if init_date is None else float(init["InitialSOHTotal"])),
            "InitialSOHDate": (init_date.isoformat() if init_date else None),
            
            "InitialSOHBalance": (None if init_date is None else float(init["InitialSOHBalance"])),
            "InitialSOHConsumed": (None if init_date is None else float(init["InitialSOHConsumed"])),
            
            # price block (inside window)
            "PriceAvg": float(avg_p) if avg_p is not None else None,
            "PriceHi":  float(hi_p)  if hi_p  is not None else None,
            "PriceLo":  float(lo_p)  if lo_p  is not None else None,
            "PriceLast":float(last_p) if last_p is not None else None,

            # value
            "SellInValue": float(sellin_value) if sellin_value is not None else None,

            # ids (handy for drilldowns)
            "CustomerID": cid,
            "SKU_ID": sid,
        })

        # parts = _debug_soh_parts(cid, sid, date_to)
        # print(f"DEBUG SOH parts for CustID={cid} SKU_ID={sid} as_of={date_to}: {parts}")
    # 4) optional: filter out rows that truly have no signal in the window
    def has_signal(r):
        return abs(r["SellIn"]) > EPS or abs(r["SellOut"]) > EPS or abs(r["SOH"]) > EPS

    rows = [r for r in rows if has_signal(r)]

    # 5) stable sort (by Customer, Brand, MECSKU)
    rows.sort(key=lambda r: (r["Customer"], r["Brand"], r["MECSKU"]))

    # 6) paginate
    total = len(rows)
    start = (page - 1) * page_size
    end   = start + page_size
    page_rows = rows[start:end]

    return jsonify(ok=True, total=total, page=page, page_size=page_size, items=page_rows)


@bp.route("/api/export", methods=["POST"])
def export_csv():
    """
    Export CSV with the same filters as /api/list (no pagination).
    Accepts the same JSON payload.
    """
    data = request.get_json(force=True) if request.is_json else {}
    data["page"] = 1
    data["page_size"] = 1_000_000

    # Use the *app* context, not blueprint:
    with current_app.test_request_context(
        "/sales-pulse-general/api/list", method="POST", json=data
    ):
        resp = list_rows()

    js = resp.get_json() if hasattr(resp, "get_json") else (getattr(resp, "json", None) or {})
    if not js.get("ok"):
        return jsonify(ok=False, error="Failed to build export"), 400

    cols = [
        "Brand","Customer","Category","CustSKU","MECSKU", "Initial SOH", "Initial SOH Date",
        "SellIn","Returns","NetSellIn","SellOut", "InitialSOHBalance","Current SOH",
        "PriceAvg","PriceHi","PriceLo","PriceLast",
        "SellInValue"
    ]
    lines = [",".join(cols)]

    def safe(s):  # minimal CSV safety
        return ("" if s is None else str(s)).replace(",", " ")

    for row in js.get("items", []):
        sellin  = float(row.get("SellIn", 0) or 0)
        returns = float(row.get("Returns", 0) or 0)   # your Returns is abs(negative SELLIN)
        net_si  = sellin - returns

        out = [
            safe(row.get("Brand")),
            safe(row.get("Customer")),
            safe(row.get("Category")),
            safe(row.get("CustSKU")),
            safe(row.get("MECSKU")),
            safe(row.get("InitialSOH")),
            safe(row.get("InitialSOHDate")),
            f"{sellin:.2f}",
            f"{returns:.2f}",
            f"{net_si:.2f}", 
            f'{float(row.get("SellOut",0) or 0):.2f}',
            safe(row.get("InitialSOHBalance")),
            f'{float(row.get("SOH",0) or 0):.2f}',
            "" if row.get("PriceAvg")  is None else f'{float(row["PriceAvg"]):.2f}',
            "" if row.get("PriceHi")   is None else f'{float(row["PriceHi"]):.2f}',
            "" if row.get("PriceLo")   is None else f'{float(row["PriceLo"]):.2f}',
            "" if row.get("PriceLast") is None else f'{float(row["PriceLast"]):.2f}',
            "" if row.get("SellInValue") is None else f'{float(row["SellInValue"]):.2f}',
        ]
        lines.append(",".join(out))

    csv_data = "\n".join(lines)
    fname = f"sales_pulse_general_{datetime.now(TZ_RIYADH).strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        csv_data,
        headers={
            "Content-Type": "text/csv; charset=utf-8",
            "Content-Disposition": f'attachment; filename="{fname}"'
        }
    )


@bp.route("/api/customer-anchor", methods=["GET"])
def customer_anchor():
    """Return customer-wide Go-Live (latest ADJUST≤today)."""
    try:
        customer_id = request.args.get("customer_id", type=int)
        as_of = _parse_date(request.args.get("as_of")) or date.today()
        if not customer_id:
            return jsonify(ok=False, error="customer_id required"), 400
        anchor = _anchor_adjust_date_customer(customer_id, as_of)  # uses your helper
        return jsonify(ok=True, anchor=(anchor.isoformat() if anchor else None))
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500
