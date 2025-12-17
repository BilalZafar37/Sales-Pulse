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
    SP_CategoriesMappingMain, SP_MCSI_SellIn
)

bp = Blueprint("sales_pulse_general", __name__, static_folder=STATIC_DIR, url_prefix="/sales-pulse-general")

# ---- constants ----
TZ_RIYADH = ZoneInfo("Asia/Riyadh")
EPS = 1e-9
MOVT_SELLIN  = "SELLIN"
MOVT_SELLOUT = "SELLOUT"
MOVT_ADJUST    = "ADJUST"
MOVT_SUPERCEED = "SUPERCEED"


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
    cust_to_ho = _cust_to_ho_subquery()
    D = _docdate_date()
    q = (model.query(func.coalesce(func.sum(signed), 0.0))
         .filter(
                cust_to_ho.c.HO_ID == customer_id,
                cust_to_ho.c.CID == SP_InventoryLedger.CustomerID,
                SP_InventoryLedger.SKU_ID == sku_id,
                D > day, D <= until_incl))
    return float(q.scalar() or 0.0)

def _sum_signed_on_date_excl_adjust(customer_id: int, sku_id: int, day: date) -> float:
    """Sum SIGNED for DocDate == day, excluding ADJUST / SUPERCEED rows."""
    signed = _signed_qty_expr()
    D = _docdate_date()
    cust_to_ho = _cust_to_ho_subquery()
    
    q = (model.query(func.coalesce(func.sum(signed), 0.0))
         .filter(
                cust_to_ho.c.HO_ID == customer_id,
                cust_to_ho.c.CID == SP_InventoryLedger.CustomerID,
                SP_InventoryLedger.SKU_ID == sku_id,
                D == day,
                ~SP_InventoryLedger.MovementType.in_([MOVT_ADJUST, MOVT_SUPERCEED])))
    return float(q.scalar() or 0.0)

def _sum_signed_inclusive(customer_id: int, sku_id: int, start_incl: date, end_incl: date) -> float:
    """Sum SIGNED for start<=DocDate<=end."""
    signed = _signed_qty_expr()
    D = _docdate_date()
    cust_to_ho = _cust_to_ho_subquery()
        
    q = (model.query(func.coalesce(func.sum(signed), 0.0))
         .filter(
                cust_to_ho.c.HO_ID == customer_id,
                cust_to_ho.c.CID == SP_InventoryLedger.CustomerID,
                SP_InventoryLedger.SKU_ID == sku_id,
                D >= start_incl, D <= end_incl))
    return float(q.scalar() or 0.0)

def _sum_movement_abs(customer_id:int, sku_id:int,
                      start_date:date|None, end_date:date|None,
                      movement_type:str) -> float:

    cust_to_ho = _cust_to_ho_subquery()

    if movement_type == MOVT_SELLIN:
        qsum = func.coalesce(
            func.sum(
                case(
                    (SP_InventoryLedger.Qty > 0, SP_InventoryLedger.Qty),
                    else_=0.0
                )
            ), 0.0
        )
    else:
        qsum = func.coalesce(func.sum(func.abs(SP_InventoryLedger.Qty)), 0.0)

    q = (
        model.query(qsum)
        .filter(
            cust_to_ho.c.HO_ID == customer_id,
            cust_to_ho.c.CID == SP_InventoryLedger.CustomerID,
            SP_InventoryLedger.SKU_ID == sku_id,
            SP_InventoryLedger.MovementType == movement_type
        )
    )

    if start_date:
        q = q.filter(SP_InventoryLedger.DocDate >= start_date)
    if end_date:
        q = q.filter(SP_InventoryLedger.DocDate <= end_date)

    return float(q.scalar() or 0.0)

def _sum_returns_abs(customer_id:int, sku_id:int,
                     start_date:date|None, end_date:date|None) -> float:
    cust_to_ho = _cust_to_ho_subquery()
    
    q = (model.query(func.coalesce(func.sum(func.abs(SP_InventoryLedger.Qty)), 0.0))
         .filter(
                cust_to_ho.c.HO_ID == customer_id,
                cust_to_ho.c.CID == SP_InventoryLedger.CustomerID,
                SP_InventoryLedger.SKU_ID == sku_id,
                SP_InventoryLedger.MovementType == 'SELLIN',
                SP_InventoryLedger.Qty < 0))
    if start_date:
        q = q.filter(SP_InventoryLedger.DocDate >= start_date)
    if end_date:
        q = q.filter(SP_InventoryLedger.DocDate <= end_date)
    return float(q.scalar() or 0.0)

def _signed_qty_expr():
    mt = SP_InventoryLedger.MovementType
    q  = func.coalesce(SP_InventoryLedger.Qty, 0.0)
    return case(
        (mt == MOVT_SELLOUT, -func.abs(q)),                         # SELLOUT always negative
        (mt == MOVT_SELLIN,   q),                                   # SELLIN as-is (can be negative for returns)
        (mt.in_([MOVT_ADJUST, MOVT_SUPERCEED]), q),                 # snapshots: full SOH
        else_=q                                                     # fallback for any other movement type
    )

from sqlalchemy import func

def _sellin_price_stats_from_mcsi(
    ho_customer_id: int,
    article: str,
    start_date,
    end_date
):
    """
    Pricing derived from SP_MCSI_SellIn
    - Maps SoldToParty (name) → SP_Customer → HO
    - Uses post-go-live window only
    - Excludes returns / negative rows
    """

    # Subquery: customer → HO
    cust_to_ho = _cust_to_ho_subquery()

    q = (
        model.query(
            SP_MCSI_SellIn.DocumentDate,
            SP_MCSI_SellIn.Net,
            SP_MCSI_SellIn.GrossSale
        )
        # Map SoldToParty (name) → customer master
        .join(
            SP_Customer,
            func.trim(func.lower(SP_Customer.CustName))
            == func.trim(func.lower(SP_MCSI_SellIn.SoldToParty))
        )
        # Roll up to HO
        .join(
            cust_to_ho,
            cust_to_ho.c.CID == SP_Customer.CustomerID
        )
        .filter(
            cust_to_ho.c.HO_ID == ho_customer_id,
            SP_MCSI_SellIn.Article == article,
            SP_MCSI_SellIn.GrossSale > 0,   # exclude returns
            SP_MCSI_SellIn.Net > 0              # exclude credit memos
        )
    )

    if start_date:
        q = q.filter(SP_MCSI_SellIn.DocumentDate >= start_date)
    if end_date:
        q = q.filter(SP_MCSI_SellIn.DocumentDate <= end_date)

    rows = q.order_by(SP_MCSI_SellIn.DocumentDate.asc()).all()

    if not rows:
        return (None, None, None, None, None)

    total_value = 0.0
    total_qty = 0.0
    hi = lo = last_price = None
    last_dt = None

    for dt, net, qty in rows:
        if not qty or qty <= 0:
            continue

        unit_price = float(net) / float(qty)

        total_value += float(net)
        total_qty += float(qty)

        hi = unit_price if hi is None or unit_price > hi else hi
        lo = unit_price if lo is None or unit_price < lo else lo

        if last_dt is None or dt > last_dt:
            last_dt = dt
            last_price = unit_price

    avg_price = (total_value / total_qty) if total_qty > 0 else last_price

    return (
        total_value,  # Sell-in Value
        avg_price,
        hi,
        lo,
        last_price
    )


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
    cust_to_ho = _cust_to_ho_subquery()

    q_led = (
        model.query(
            cust_to_ho.c.HO_ID.label("CustomerID"),
            SP_InventoryLedger.SKU_ID
        )
        .join(cust_to_ho, cust_to_ho.c.CID == SP_InventoryLedger.CustomerID)
    )

    if customer_id:
        q_led = q_led.filter(
            cust_to_ho.c.HO_ID == customer_id,
            cust_to_ho.c.CID == SP_InventoryLedger.CustomerID
        )
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
        cust_to_ho = _cust_to_ho_subquery()

        q_snap = (
            model.query(
                cust_to_ho.c.HO_ID.label("CustomerID"),
                SP_SOH_Detail.SKU_ID
            )
            .select_from(SP_SOH_Uploads)
            .join(cust_to_ho, cust_to_ho.c.CID == SP_SOH_Uploads.CustomerID)
            .join(SP_SOH_Detail, SP_SOH_Detail.SOHUploadID == SP_SOH_Uploads.SOHUploadID)
            .filter(
                SP_SOH_Detail.IsActive == True,
                SP_SOH_Detail.SOHDate <= end_date
            )
        )

        if brand:
            q_snap = q_snap.filter(SP_SOH_Uploads.Brand == brand)

        if customer_id:
            q_snap = q_snap.filter(cust_to_ho.c.HO_ID == customer_id)

        if category_id or catcode:
            q_snap = (
                q_snap
                .join(SP_SKU, SP_SKU.SKU_ID == SP_SOH_Detail.SKU_ID)
                .outerjoin(
                    SP_CategoriesMappingMain,
                    SP_CategoriesMappingMain.ID == SP_SKU.CategoryMappingID
                )
            )
            if category_id:
                q_snap = q_snap.filter(SP_SKU.CategoryMappingID == category_id)
            if catcode:
                q_snap = q_snap.filter(SP_CategoriesMappingMain.CatCode == catcode)

        for cid, sid in q_snap.distinct().all():
            pairs.add((int(cid), int(sid)))


    return list(pairs)

def _anchor_adjust_date_sku(customer_id: int, sku_id: int, as_of: date) -> date | None:
    """
    Latest ADJUST / SUPERCEED date for this specific SKU on/before as_of.
    This is the direct snapshot anchor for (customer, sku).
    """
    return (model.query(func.max(cast(SP_InventoryLedger.DocDate, Date)))
            .filter(
                SP_InventoryLedger.CustomerID == customer_id,
                SP_InventoryLedger.SKU_ID == sku_id,
                SP_InventoryLedger.MovementType.in_([MOVT_ADJUST, MOVT_SUPERCEED]),
                SP_InventoryLedger.DocDate <= as_of
            )
            .scalar())

def _anchor_adjust_date_brand(customer_id: int, brand: str, as_of: date) -> date | None:
    """
    Latest ADJUST / SUPERCEED date for *any* SKU of this (customer, brand) on/before as_of.
    This is the brand-level go-live date.
    """
    if not brand:
        return None

    return (model.query(func.max(cast(SP_InventoryLedger.DocDate, Date)))
            .join(SP_SKU, SP_SKU.SKU_ID == SP_InventoryLedger.SKU_ID)
            .filter(
                SP_InventoryLedger.CustomerID == customer_id,
                SP_InventoryLedger.MovementType.in_([MOVT_ADJUST, MOVT_SUPERCEED]),
                SP_SKU.Brand == brand,
                SP_InventoryLedger.DocDate <= as_of
            )
            .scalar())

def _anchor_adjust_date_customer(customer_id: int, as_of: date) -> date | None:
    """
    Latest ADJUST / SUPERCEED date for *any* SKU of this customer on/before as_of.
    Used by the /api/customer-anchor endpoint for UI info.
    """
    return (model.query(func.max(cast(SP_InventoryLedger.DocDate, Date)))
            .filter(
                SP_InventoryLedger.CustomerID == customer_id,
                SP_InventoryLedger.MovementType.in_([MOVT_ADJUST, MOVT_SUPERCEED]),
                SP_InventoryLedger.DocDate <= as_of
            )
            .scalar())
    
def _effective_anchor_for_sku(customer_id: int, sku_id: int, as_of: date) -> date | None:
    """
    Effective anchor for SOH logic:

    1) If this SKU has its own ADJUST/SUPERCEED -> use that (per-SKU snapshot).
    2) Else, if any SKU of the same brand has ADJUST/SUPERCEED -> use that
       brand-level go-live date (starting SOH = 0 for this SKU).
    3) Else, no anchor at all -> return None.
    """
    # 1) SKU-specific anchor
    sku_anchor = _anchor_adjust_date_sku(customer_id, sku_id, as_of)
    if sku_anchor:
        return sku_anchor

    # 2) Brand-level anchor as fallback
    brand = model.query(SP_SKU.Brand).filter(SP_SKU.SKU_ID == sku_id).scalar()
    if not brand:
        return None

    brand_anchor = _anchor_adjust_date_brand(customer_id, brand, as_of)
    return brand_anchor

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
    Return (InitialSOH, InitialSOHDate) for the earliest ADJUST/SUPERCEED in [start_incl, end_incl].
    If no snapshot exists in the window, return (None, None).
    """
    if not start_incl or not end_incl:
        return (None, None)

    q = (model.query(SP_InventoryLedger.Qty, SP_InventoryLedger.DocDate)
         .filter(
             SP_InventoryLedger.CustomerID == customer_id,
             SP_InventoryLedger.SKU_ID == sku_id,
             SP_InventoryLedger.MovementType.in_([MOVT_ADJUST, MOVT_SUPERCEED]),
             SP_InventoryLedger.DocDate >= start_incl,
             SP_InventoryLedger.DocDate <= end_incl
         )
         .order_by(SP_InventoryLedger.DocDate.asc(), SP_InventoryLedger.LedgerID.asc()))
    row = q.first()
    return ((float(row[0] or 0.0), row[1]) if row else (None, None))

def _sum_consumers_since_anchor(customer_id:int, sku_id:int,
                                anchor_date:date, until_incl:date) -> float:
    D = _docdate_date()
    cust_to_ho = _cust_to_ho_subquery()

    # 1) All SELLOUT (absolute)
    q1 = (model.query(func.coalesce(func.sum(func.abs(SP_InventoryLedger.Qty)), 0.0))
          .filter(
                cust_to_ho.c.HO_ID == customer_id,
                cust_to_ho.c.CID == SP_InventoryLedger.CustomerID,
                SP_InventoryLedger.SKU_ID == sku_id,
                SP_InventoryLedger.MovementType == 'SELLOUT',
                D >= anchor_date, D <= until_incl))
    selout_abs = float(q1.scalar() or 0.0)

    # 2) All RETURNS posted as negative SELLIN
    q2 = (model.query(func.coalesce(func.sum(func.abs(SP_InventoryLedger.Qty)), 0.0))
          .filter(
                cust_to_ho.c.HO_ID == customer_id,
                cust_to_ho.c.CID == SP_InventoryLedger.CustomerID,
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
        # If there was an ADJUST/SUPERCEED for this SKU *on* the anchor day, include it; else zero baseline
        D = _docdate_date()
        has_snap_on_anchor = (model.query(func.count())
                              .filter(
                                  SP_InventoryLedger.CustomerID == customer_id,
                                  SP_InventoryLedger.SKU_ID == sku_id,
                                  SP_InventoryLedger.MovementType.in_([MOVT_ADJUST, MOVT_SUPERCEED]),
                                  D == anchor_date
                              ).scalar() or 0) > 0
        init_total = _sum_signed_inclusive(customer_id, sku_id, anchor_date, anchor_date) if has_snap_on_anchor else 0.0


    consumed = min(consumers_abs, init_total)
    balance  = max(0.0, init_total - consumed)

    return {
        "InitialSOHDate": anchor_date,
        "InitialSOHTotal": float(init_total),
        "InitialSOHConsumed": float(consumed),
        "InitialSOHBalance": float(balance),
        "SellOutSinceAnchor": float(consumers_abs),
    }

def _cust_to_ho_subquery():
    """
    Maps any CustomerID (HO or Branch) → HO CustomerID
    """
    return (
        model.query(
            SP_Customer.CustomerID.label("CID"),
            func.coalesce(SP_Customer.ParentCustID, SP_Customer.CustomerID).label("HO_ID")
        )
    ).subquery()

# Debug code for HO mapping
def _debug_ho_mapping(customer_id: int):
    rows = (
        model.query(
            SP_Customer.CustomerID,
            SP_Customer.CustCode,
            SP_Customer.ParentCustID
        )
        .filter(
            (SP_Customer.CustomerID == customer_id) |
            (SP_Customer.ParentCustID == customer_id)
        )
        .all()
    )
    print(f"\n[DEBUG] HO mapping for customer_id={customer_id}")
    for r in rows:
        print(f"  CID={r.CustomerID}, Code={r.CustCode}, Parent={r.ParentCustID}")

def _debug_raw_sellin(customer_id: int, sku_id: int, start, end):
    cust_to_ho = _cust_to_ho_subquery()

    rows = (
        model.query(
            SP_InventoryLedger.CustomerID,
            SP_InventoryLedger.DocDate,
            SP_InventoryLedger.Qty
        )
        .filter(
            cust_to_ho.c.HO_ID == customer_id,
            cust_to_ho.c.CID == SP_InventoryLedger.CustomerID,
            SP_InventoryLedger.SKU_ID == sku_id,
            SP_InventoryLedger.MovementType == 'SELLIN'
        )
    )

    if start:
        rows = rows.filter(SP_InventoryLedger.DocDate >= start)
    if end:
        rows = rows.filter(SP_InventoryLedger.DocDate <= end)

    rows = rows.order_by(SP_InventoryLedger.DocDate).all()

    print(f"\n[DEBUG] RAW SELLIN rows for HO={customer_id}, SKU={sku_id}")
    if not rows:
        print("  ❌ NO SELLIN ROWS FOUND")
    for r in rows:
        print(f"  CID={r.CustomerID}, Date={r.DocDate}, Qty={r.Qty}")


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
        Closing SOH =
        Remaining Initial SOH
        + Total SELLIN after anchor
        """

        init = _initial_bucket_numbers(customer_id, sku_id, as_of)

        # No anchor → fallback to snapshot logic
        if init["InitialSOHDate"] is None:
            snap_date, snap_qty = _latest_active_snapshot(customer_id, None, sku_id, as_of)
            if snap_date:
                delta = _sum_signed_after_date(customer_id, sku_id, snap_date, as_of)
                return float(snap_qty) + float(delta)
            return 0.0

        anchor = init["InitialSOHDate"]

        # Remaining initial stock (already net of sell-out & returns)
        remaining_initial = init["InitialSOHBalance"] or 0.0

        # SELLIN only (positive qty) after anchor
        sellin_after_anchor = _sum_movement_abs(
            customer_id,
            sku_id,
            anchor,
            as_of,
            MOVT_SELLIN
        )

        return float(remaining_initial + sellin_after_anchor)

    
    data = request.get_json(force=True) if request.is_json else {}
    brand       = (data.get("brand") or "").strip() or None
    customer_id = data.get("customer_id", None)
    category_id = data.get("category_id", None)
    catcode     = (data.get("catcode") or "").strip() or None
    date_from   = _parse_date(data.get("date_from"))
    date_to     = _parse_date(data.get("date_to")) or date.today()

    page      = max(1, int(data.get("page") or 1))
    page_size = min(500, int(data.get("page_size") or 100))
    
    print("\n================ SALES PULSE DEBUG ================")
    print(f"[DEBUG] Filters → customer_id={customer_id}, brand={brand}, from={date_from}, to={date_to}")

    if customer_id:
        _debug_ho_mapping(customer_id)


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
        # sellin_qty  = _sum_movement_abs(cid, sid, date_from, date_to, MOVT_SELLIN)
        sellin_qty  = _sum_movement_abs(cid, sid, date_from, date_to, MOVT_SELLIN)

        if sellin_qty == 0:
            _debug_raw_sellin(cid, sid, date_from, date_to)
            print(f"[DEBUG] Computed SELLIN = {sellin_qty}")

        sellout_qty = _sum_movement_abs(cid, sid, date_from, date_to, MOVT_SELLOUT)

        # closing SOH as of date_to (brand matters for snapshot scoping)
        # closing_soh = _closing_soh_as_of(cid, getattr(s, "Brand", None), sid, date_to)
        # new:
        closing_soh = _closing_soh_anchored(cid, sid, date_to)

        # price stats & sell-in value (inside window)
        sellin_value, avg_p, hi_p, lo_p, last_p = _sellin_price_stats_from_mcsi(
            cid,
            s.ArticleCode,      # Article
            date_from,
            date_to
        )


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
