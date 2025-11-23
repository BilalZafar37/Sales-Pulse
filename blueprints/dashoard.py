# blueprints/dashboard.py
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

from flask import Blueprint, render_template, request, jsonify
from sqlalchemy import select, func, case, and_, or_, literal, Integer, Float, String
from sqlalchemy.orm import aliased

# Import your session + ORM classes
from models import (
    model,
    SP_MCSI_SellIn, SP_MCSI_SellOut,
    SP_SellOutUploads, SP_SellOutNegPreview, SP_SellOutApproval,
    SP_InventoryLedger,
    SP_SOH_Detail, SP_SOH_Uploads,
    SP_SKU, SP_CategoriesMappingMain,
    SP_Customer, SP_Customer_SKU_Map, literal_column, cast
)
from config import STATIC_DIR

bp = Blueprint("dashboard", __name__, static_folder=STATIC_DIR, url_prefix="/dashboard")

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------

# --- Eligible customers (have an ADJUST) + first ADJUST date per customer ---

def _sellin_join_customer(stmt):
    """
    Join SP_MCSI_SellIn to SP_Customer using the safest available key,
    *never* joining ec directly to Sell-In.
    Preference: SoldToPartyCode -> SoldToPartyName -> SoldToParty (via cast).
    """
    if hasattr(SP_MCSI_SellIn, "SoldToPartyCode"):
        return stmt.join(SP_Customer, SP_Customer.CustCode == SP_MCSI_SellIn.SoldToPartyCode)

    if hasattr(SP_MCSI_SellIn, "SoldToPartyName"):
        # Arabic / NVARCHAR-friendly, exact string match to CustName
        return stmt.join(SP_Customer, SP_Customer.CustName == SP_MCSI_SellIn.SoldToPartyName)

    if hasattr(SP_MCSI_SellIn, "SoldToParty"):
        # Last resort: treat SoldToParty as text and compare to CustomerID casted to NVARCHAR to avoid INT->NVARCHAR implicit casts
        return stmt.join(SP_Customer, cast(SP_Customer.CustomerID, String) == SP_MCSI_SellIn.SoldToParty)

    raise RuntimeError("SP_MCSI_SellIn needs SoldToPartyCode or SoldToPartyName or SoldToParty to map to SP_Customer.")


def _parse_date(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            d = datetime.strptime(s, fmt)
            return d
        except ValueError:
            continue
    return None

def _default_window() -> Tuple[datetime, datetime]:
    end = datetime.today().replace(hour=23, minute=59, second=59, microsecond=0)
    start = (end - timedelta(days=180)).replace(hour=0, minute=0, second=0, microsecond=0)
    return start, end

def _csv_arg(name: str) -> List[str]:
    v = (request.args.get(name) or "").strip()
    if not v:
        return []
    return [x.strip() for x in v.split(",") if x.strip()]

def _date_window() -> Tuple[datetime, datetime]:
    start, end = _default_window()
    f = _parse_date(request.args.get("from"))
    t = _parse_date(request.args.get("to"))
    if f: start = f.replace(hour=0, minute=0, second=0, microsecond=0)
    if t: end   = t.replace(hour=23, minute=59, second=59, microsecond=0)
    return start, end

def _eligible_customers_subq():
    return (
        select(
            SP_InventoryLedger.CustomerID.label("cid"),
            func.min(SP_InventoryLedger.DocDate).label("first_adjust_dt"),
        ).where(SP_InventoryLedger.MovementType == "ADJUST")
         .group_by(SP_InventoryLedger.CustomerID)
         .subquery("ec")
    )

def _apply_common_filters_for_sellin(stmt):
    """
    brand=, sales_group=, site=, channel=, sold_to=
    PLUS: restrict to eligible customers (have ADJUST) and
    start from each customer's first ADJUST date.
    """
    # attribute filters
    brands      = _csv_arg("brand")
    sales_group = _csv_arg("sales_group")
    site        = _csv_arg("site")
    channel     = _csv_arg("channel")
    sold_to     = _csv_arg("sold_to")

    if brands:
        stmt = stmt.where(SP_MCSI_SellIn.Brand.in_(brands))
    if sales_group:
        stmt = stmt.where(SP_MCSI_SellIn.SalesGroup.in_(sales_group))
    if channel and hasattr(SP_MCSI_SellIn, "DistributionChannel"):
        stmt = stmt.where(getattr(SP_MCSI_SellIn, "DistributionChannel").in_(channel))
    if site and hasattr(SP_MCSI_SellIn, "SalesOffice"):
        stmt = stmt.where(getattr(SP_MCSI_SellIn, "SalesOffice").in_(site))
    if sold_to and hasattr(SP_MCSI_SellIn, "SoldToParty"):
        stmt = stmt.where(SP_MCSI_SellIn.SoldToParty.in_(sold_to))

    # 🔒 Map Sell-In -> Customer (handle NVARCHAR / Arabic safely), then join ec on CustomerID (INT)
    stmt = _sellin_join_customer(stmt)
    ec = _eligible_customers_subq()
    stmt = (
        stmt.join(ec, ec.c.cid == SP_Customer.CustomerID)
            .where(SP_MCSI_SellIn.DocumentDate >= ec.c.first_adjust_dt)
    )
    return stmt

def _apply_common_filters_for_sellout(stmt):
    """
    Filter Sell-Out by upload header (Customer/Brand) and by SKU attributes,
    THEN restrict to eligible customers (have ADJUST) and only data from each
    customer's first ADJUST date onward.
    """
    brands       = _csv_arg("brand")
    customers    = _csv_arg("customer_id") or _csv_arg("sold_to")
    category_ids = [int(x) for x in _csv_arg("category_id") if x.isdigit()]
    sku_codes    = _csv_arg("sku")

    # Required joins for filters
    stmt = stmt.join(SP_SellOutUploads, SP_SellOutUploads.UploadID == SP_MCSI_SellOut.UploadID)
    stmt = stmt.join(SP_SKU, SP_SKU.SKU_ID == SP_MCSI_SellOut.SKU_ID)

    if brands:
        stmt = stmt.where(SP_SKU.Brand.in_(brands))
    if customers:
        stmt = stmt.where(SP_SellOutUploads.CustomerID.in_(customers))
    if category_ids:
        stmt = stmt.where(SP_SKU.CategoryMappingID.in_(category_ids))
    if sku_codes:
        stmt = stmt.where(SP_SKU.ArticleCode.in_(sku_codes))

    # 🔒 ADJUST eligibility + start date
    ec = _eligible_customers_subq()
    stmt = (
        stmt.join(ec, ec.c.cid == SP_SellOutUploads.CustomerID)
            .where(SP_MCSI_SellOut.DocumentDate >= ec.c.first_adjust_dt)
    )
    return stmt

def _apply_date_range(stmt, col):
    start, end = _date_window()
    return stmt.where(col >= start.date()).where(col <= end.date())

def _rows(exec_stmt):
    return [dict(r._mapping) for r in model.execute(exec_stmt).all()]

def _one(exec_stmt) -> Optional[Dict[str, Any]]:
    r = model.execute(exec_stmt).mappings().first()
    return dict(r) if r else None

# --------------------------------------------------------------------------------------
# Page (shell only; data is fetched via JSON endpoints)
# --------------------------------------------------------------------------------------

@bp.get("/")
def dashboard_page():
    return render_template("dashboard/dashboard.html")

# --------------------------------------------------------------------------------------
# SUMMARY KPIs
# --------------------------------------------------------------------------------------

@bp.get("/api/summary")
def api_summary():
    # ------------------ SELL-IN (value + qty) ------------------
    si_stmt = select(
        func.sum(SP_MCSI_SellIn.Net).label("sellin_value"),
        func.sum(SP_MCSI_SellIn.GrossSale).label("sellin_qty"),
    )
    si_stmt = _apply_common_filters_for_sellin(si_stmt)
    si_stmt = _apply_date_range(si_stmt, SP_MCSI_SellIn.DocumentDate)
    sellin = _one(si_stmt) or {"sellin_value": 0.0, "sellin_qty": 0.0}

    # ------------------ SELL-OUT (qty) ------------------
    so_stmt = select(func.sum(SP_MCSI_SellOut.SellOutQty).label("sellout_qty")).select_from(SP_MCSI_SellOut)
    so_stmt = _apply_common_filters_for_sellout(so_stmt)
    so_stmt = _apply_date_range(so_stmt, SP_MCSI_SellOut.DocumentDate)
    sellout = _one(so_stmt) or {"sellout_qty": 0.0}

    # ------------------ SELL-THROUGH ------------------
    si_qty = float(sellin.get("sellin_qty") or 0.0)
    so_qty = float(sellout.get("sellout_qty") or 0.0)
    sell_through = (so_qty / si_qty * 100.0) if si_qty > 0 else None

    # ------------------ Inventory balance (up to end date) ------------------
    end_dt = _date_window()[1]
    ec = _eligible_customers_subq()
    led_stmt = (
        select(func.sum(SP_InventoryLedger.Qty).label("balance"))
        .select_from(SP_InventoryLedger)
        .join(ec, ec.c.cid == SP_InventoryLedger.CustomerID)
        .join(SP_SKU, SP_SKU.SKU_ID == SP_InventoryLedger.SKU_ID)
        .where(
            SP_InventoryLedger.DocDate >= ec.c.first_adjust_dt,
            SP_InventoryLedger.DocDate <= end_dt.date(),
        )
    )
    
    brands       = _csv_arg("brand")
    customers    = _csv_arg("customer_id") or _csv_arg("sold_to")
    category_ids = [int(x) for x in _csv_arg("category_id") if x.isdigit()]
    sku_codes    = _csv_arg("sku")
    
    if brands:
        led_stmt = led_stmt.where(SP_SKU.Brand.in_(brands))
    if customers:
        led_stmt = led_stmt.where(SP_InventoryLedger.CustomerID.in_(customers))
    if category_ids:
        led_stmt = led_stmt.where(SP_SKU.CategoryMappingID.in_(category_ids))
    if sku_codes:
        led_stmt = led_stmt.where(SP_SKU.ArticleCode.in_(sku_codes))
    
    inv = _one(led_stmt) or {"balance": 0.0}

    # ------------------ Coverage & reporting ------------------
    # Customers reporting (do NOT pre-join here)
    cust_stmt = (
        select(func.count(func.distinct(SP_SellOutUploads.CustomerID)).label("customers_reporting"))
        .select_from(SP_MCSI_SellOut)
    )
    cust_stmt = _apply_common_filters_for_sellout(cust_stmt)
    cust_stmt = _apply_date_range(cust_stmt, SP_MCSI_SellOut.DocumentDate)
    custs = _one(cust_stmt) or {"customers_reporting": 0}

    # Active SKUs sold out in window (same pattern)
    sku_stmt = (
        select(func.count(func.distinct(SP_MCSI_SellOut.SKU_ID)).label("active_skus"))
        .select_from(SP_MCSI_SellOut)
    )
    sku_stmt = _apply_common_filters_for_sellout(sku_stmt)
    sku_stmt = _apply_date_range(sku_stmt, SP_MCSI_SellOut.DocumentDate)
    skus = _one(sku_stmt) or {"active_skus": 0}

    # Pending uploads (Draft/Submitted) within window
    pending_stmt = (
        select(func.count(SP_SellOutUploads.UploadID).label("pending_uploads"))
        .where(SP_SellOutUploads.Status.in_(["Draft", "Submitted"]))
    )
    start_dt, end_dt = _date_window()
    if hasattr(SP_SellOutUploads, "CreatedAt"):
        pending_stmt = pending_stmt.where(
            and_(SP_SellOutUploads.CreatedAt >= start_dt, SP_SellOutUploads.CreatedAt <= end_dt)
        )
    pend = _one(pending_stmt) or {"pending_uploads": 0}

    # Last upload (any status)
    last_up_stmt = select(func.max(SP_SellOutUploads.CreatedAt).label("last_upload_at"))
    last_up = _one(last_up_stmt) or {"last_upload_at": None}

    # Potential negatives in preview within window
    neg_stmt = (
        select(func.count().label("negatives"))
        .where(
            and_(
                SP_SellOutNegPreview.IsNegative == True,                        # noqa: E712
                SP_SellOutNegPreview.DocumentDate >= start_dt.date(),
                SP_SellOutNegPreview.DocumentDate <= end_dt.date()
            )
        )
    )
    negatives = _one(neg_stmt) or {"negatives": 0}

    return jsonify({
        "sell_in": {
            "value": float(sellin.get("sellin_value") or 0.0),
            "qty": float(sellin.get("sellin_qty") or 0.0),
        },
        "sell_out": { "qty": float(sellout.get("sellout_qty") or 0.0) },
        "sell_through_pct": round(sell_through, 2) if sell_through is not None else None,
        "inventory_balance": float(inv.get("balance") or 0.0),
        "coverage": {
            "customers_reporting": int(custs.get("customers_reporting") or 0),
            "active_skus": int(skus.get("active_skus") or 0),
        },
        "reporting": {
            "pending_uploads": int(pend.get("pending_uploads") or 0),
            "last_upload_at": str(last_up.get("last_upload_at") or "") if last_up.get("last_upload_at") else None,
            "potential_negatives": int(negatives.get("negatives") or 0),
        }
    })

# --------------------------------------------------------------------------------------
# CHARTS
# --------------------------------------------------------------------------------------

@bp.get("/api/charts")
def api_charts():
    start_dt, end_dt = _date_window()

    # ---------- Sell-In by month (value) ----------
    si_month = select(
        func.year(SP_MCSI_SellIn.DocumentDate).label("y"),
        func.month(SP_MCSI_SellIn.DocumentDate).label("m"),
        func.sum(SP_MCSI_SellIn.Net).label("v"),
    )
    si_month = _apply_common_filters_for_sellin(si_month)                 # 🔒
    si_month = _apply_date_range(si_month, SP_MCSI_SellIn.DocumentDate)
    si_month = si_month.group_by(
        func.year(SP_MCSI_SellIn.DocumentDate),
        func.month(SP_MCSI_SellIn.DocumentDate)
    )
    si_rows = _rows(si_month)

    # ---------- Sell-Out by month (qty) ----------
    so_month = select(
        func.year(SP_MCSI_SellOut.DocumentDate).label("y"),
        func.month(SP_MCSI_SellOut.DocumentDate).label("m"),
        func.sum(SP_MCSI_SellOut.SellOutQty).label("q"),
    ).select_from(SP_MCSI_SellOut)
    so_month = _apply_common_filters_for_sellout(so_month)   # adds Uploads + SKU joins
    so_month = _apply_date_range(so_month, SP_MCSI_SellOut.DocumentDate)
    so_month = so_month.group_by(
        func.year(SP_MCSI_SellOut.DocumentDate),
        func.month(SP_MCSI_SellOut.DocumentDate)
    )
    so_rows = _rows(so_month)

    # unify months
    month_keys = sorted({(r["y"], r["m"]) for r in si_rows} | {(r["y"], r["m"]) for r in so_rows})
    labels = [f"{y}-{m:02d}" for (y, m) in month_keys]
    si_series, so_series, st_series = [], [], []
    si_map = {(r["y"], r["m"]): float(r["v"] or 0) for r in si_rows}
    so_map = {(r["y"], r["m"]): float(r["q"] or 0) for r in so_rows}

    # Sell-In qty (for sell-through denominator)
    si_qty_stmt = select(
        func.year(SP_MCSI_SellIn.DocumentDate).label("y"),
        func.month(SP_MCSI_SellIn.DocumentDate).label("m"),
        func.sum(SP_MCSI_SellIn.GrossSale).label("qty")
    )
    si_qty_stmt = _apply_common_filters_for_sellin(si_qty_stmt)
    # si_qty_stmt = _force_sellin_ec_join(si_qty_stmt)             # 🔒
    si_qty_stmt = _apply_date_range(si_qty_stmt, SP_MCSI_SellIn.DocumentDate)
    si_qty_stmt = si_qty_stmt.group_by(
        func.year(SP_MCSI_SellIn.DocumentDate),
        func.month(SP_MCSI_SellIn.DocumentDate)
    )
    si_qty_rows = _rows(si_qty_stmt)
    si_qty_map = {(r["y"], r["m"]): float(r["qty"] or 0) for r in si_qty_rows}

    for y, m in month_keys:
        si_series.append(si_map.get((y, m), 0.0))
        so_series.append(so_map.get((y, m), 0.0))
        denom = si_qty_map.get((y, m), 0.0)
        st_series.append(round((so_map.get((y, m), 0.0) / denom * 100.0), 2) if denom > 0 else None)

    # ---------- Category mix (Sell-Out qty) ----------
    # Build minimal base -> helper adds Uploads + SKU joins -> then add category outer join
    cat_base = select(literal(1)).select_from(SP_MCSI_SellOut)
    cat_base = _apply_common_filters_for_sellout(cat_base)
    cat_base = _apply_date_range(cat_base, SP_MCSI_SellOut.DocumentDate)
    
    # Use one identical expression for SELECT and GROUP BY
    uncat = literal_column("N'Uncategorized'")  # embed constant (no separate bind params)
    label_expr = func.isnull(SP_CategoriesMappingMain.CatName, uncat)
    
    cat_stmt = (
        cat_base
        .with_only_columns(
            label_expr.label("label"),
            func.sum(SP_MCSI_SellOut.SellOutQty).label("qty"),
        )
        .outerjoin(
            SP_CategoriesMappingMain,
            SP_CategoriesMappingMain.ID == SP_SKU.CategoryMappingID
        )
        .group_by(label_expr)
    )
    
    cats = _rows(cat_stmt)
    cat_labels = [r["label"] for r in cats]
    cat_values = [float(r["qty"] or 0.0) for r in cats]
    

    # ---------- Brand mix (Sell-Out qty) ----------
    brand_base = select(literal(1)).select_from(SP_MCSI_SellOut)
    brand_base = _apply_common_filters_for_sellout(brand_base)
    brand_base = _apply_date_range(brand_base, SP_MCSI_SellOut.DocumentDate)
    brand_stmt = (
        brand_base
        .with_only_columns(
            SP_SKU.Brand.label("label"),
            func.sum(SP_MCSI_SellOut.SellOutQty).label("qty"),
        )
        .group_by(SP_SKU.Brand)
    )
    brands = _rows(brand_stmt)
    brand_labels = [r["label"] or "—" for r in brands]
    brand_values = [float(r["qty"] or 0.0) for r in brands]

    # ---------- Repeat vs New customers ----------
    ec = _eligible_customers_subq()
    
    # first sell-out date per eligible customer, but not before their first ADJUST
    first_stmt = (
        select(
            SP_SellOutUploads.CustomerID.label("cid"),
            func.min(SP_MCSI_SellOut.DocumentDate).label("first_dt")
        )
        .select_from(SP_MCSI_SellOut)
        .join(SP_SellOutUploads, SP_SellOutUploads.UploadID == SP_MCSI_SellOut.UploadID)
        .join(ec, ec.c.cid == SP_SellOutUploads.CustomerID)
        .where(SP_MCSI_SellOut.DocumentDate >= ec.c.first_adjust_dt)
        .group_by(SP_SellOutUploads.CustomerID)
    )
    first_map = {r["cid"]: r["first_dt"] for r in _rows(first_stmt)}
    
    active_stmt = select(func.distinct(SP_SellOutUploads.CustomerID).label("cid")).select_from(SP_MCSI_SellOut)
    active_stmt = _apply_common_filters_for_sellout(active_stmt)   # already joins ec and enforces date >= first_adjust_dt
    active_stmt = _apply_date_range(active_stmt, SP_MCSI_SellOut.DocumentDate)
    active_cids = [r["cid"] for r in _rows(active_stmt)]
    
    rep = sum(1 for cid in active_cids if first_map.get(cid) and first_map[cid] < start_dt.date())
    new = len(active_cids) - rep
    

    return jsonify({
        "monthly": {
            "labels": labels,
            "sellin_value": si_series,
            "sellout_qty": so_series,
            "sellthrough_pct": st_series,
        },
        "category_mix": { "labels": cat_labels, "values": cat_values },
        "brand_mix":    { "labels": brand_labels, "values": brand_values },
        "repeat_vs_new": { "labels": ["Repeat", "New"], "values": [rep, new] },
    })
# --------------------------------------------------------------------------------------
# TABLES
# --------------------------------------------------------------------------------------

@bp.get("/api/table/customers")
def api_table_customers():
    """
    Top customers by Sell-Out qty in the window with last reported date and
    count of uploads (acts like 'orders').
    """
    # Aggregate at UploadID first to emulate "documents"
    base = (
        select(
            SP_SellOutUploads.CustomerID.label("cid"),
            func.count(func.distinct(SP_MCSI_SellOut.UploadID)).label("orders"),
            func.sum(SP_MCSI_SellOut.SellOutQty).label("total_qty"),
            func.max(SP_MCSI_SellOut.DocumentDate).label("last_dt"),
        )
        .select_from(SP_MCSI_SellOut)
    )
    base = _apply_common_filters_for_sellout(base)
    base = _apply_date_range(base, SP_MCSI_SellOut.DocumentDate)
    base = base.group_by(SP_SellOutUploads.CustomerID)
    rows = _rows(base)

    # Map Customer metadata
    cids = [r["cid"] for r in rows]
    if cids:
        meta_stmt = select(
            SP_Customer.CustomerID, SP_Customer.CustCode, SP_Customer.CustName, SP_Customer.LevelType
        ).where(SP_Customer.CustomerID.in_(cids))
        meta = {r["CustomerID"]: r for r in _rows(meta_stmt)}
    else:
        meta = {}

    out = []
    for r in rows:
        m = meta.get(r["cid"], {})
        out.append({
            "id": r["cid"],
            "name": m.get("CustName") or m.get("CustCode") or r["cid"],
            "level": m.get("LevelType") or "",
            "orders": int(r["orders"] or 0),
            "total_qty": float(r["total_qty"] or 0),
            "last_reported": str(r["last_dt"]) if r["last_dt"] else None,
        })

    # Sort by total_qty desc, cap 100
    out.sort(key=lambda x: x["total_qty"], reverse=True)
    return jsonify({ "rows": out[:100] })

@bp.get("/api/table/products")
def api_table_products():
    """
    Top SKUs by Sell-Out qty in the window + latest SOH snapshot per SKU (<= end date).
    """
    # Sell-Out aggregation per SKU
    agg = (
        select(
            SP_MCSI_SellOut.SKU_ID,
            func.sum(SP_MCSI_SellOut.SellOutQty).label("qty"),
            func.max(SP_MCSI_SellOut.DocumentDate).label("last_dt"),
        )
        .select_from(SP_MCSI_SellOut)
    )
    agg = _apply_common_filters_for_sellout(agg)
    agg = _apply_date_range(agg, SP_MCSI_SellOut.DocumentDate)
    agg = agg.group_by(SP_MCSI_SellOut.SKU_ID)
    so_rows = _rows(agg)

    sku_ids = [r["SKU_ID"] for r in so_rows]
    sku_info = {}
    if sku_ids:
        sku_stmt = (
            select(
                SP_SKU.SKU_ID, SP_SKU.ArticleCode, SP_SKU.Description, SP_SKU.Brand,
                SP_CategoriesMappingMain.CatName.label("category")
            )
            .select_from(SP_SKU)
            .outerjoin(SP_CategoriesMappingMain, SP_CategoriesMappingMain.ID == SP_SKU.CategoryMappingID)
            .where(SP_SKU.SKU_ID.in_(sku_ids))
        )
        sku_info = {r["SKU_ID"]: r for r in _rows(sku_stmt)}

    # Latest SOH per SKU (<= end date)
    # Stock from ledger since each customer's first ADJUST, up to end date
    _, end_dt = _date_window()
    ec = _eligible_customers_subq()
    
    stock_stmt = (
        select(
            SP_InventoryLedger.SKU_ID,
            func.sum(SP_InventoryLedger.Qty).label("stock"),
        )
        .select_from(SP_InventoryLedger)
        .join(ec, ec.c.cid == SP_InventoryLedger.CustomerID)
        .where(
            SP_InventoryLedger.DocDate >= ec.c.first_adjust_dt,
            SP_InventoryLedger.DocDate <= end_dt.date(),
        )
    )
    
    brands       = _csv_arg("brand")
    customers    = _csv_arg("customer_id") or _csv_arg("sold_to")
    category_ids = [int(x) for x in _csv_arg("category_id") if x.isdigit()]
    sku_codes    = _csv_arg("sku")
    
    # Join SKU only if needed for attribute filters
    if brands or category_ids or sku_codes:
        stock_stmt = stock_stmt.join(SP_SKU, SP_SKU.SKU_ID == SP_InventoryLedger.SKU_ID)
    
    if brands:
        stock_stmt = stock_stmt.where(SP_SKU.Brand.in_(brands))
    if customers:
        stock_stmt = stock_stmt.where(SP_InventoryLedger.CustomerID.in_(customers))
    if category_ids:
        stock_stmt = stock_stmt.where(SP_SKU.CategoryMappingID.in_(category_ids))
    if sku_codes:
        stock_stmt = stock_stmt.where(SP_SKU.ArticleCode.in_(sku_codes))
    
    stock_stmt = stock_stmt.group_by(SP_InventoryLedger.SKU_ID)
    stock_map = {r["SKU_ID"]: float(r["stock"] or 0.0) for r in _rows(stock_stmt)}

    out = []
    for r in so_rows:
        s = sku_info.get(r["SKU_ID"], {})
        out.append({
            "id": r["SKU_ID"],
            "sku": s.get("ArticleCode") or r["SKU_ID"],
            "product": s.get("Description") or "",
            "brand": s.get("Brand") or "",
            "category": s.get("category") or "—",
            "sold_qty": float(r["qty"] or 0.0),
            "last_sellout": str(r["last_dt"]) if r["last_dt"] else None,
            "stock": float(stock_map.get(r["SKU_ID"], 0.0)),
        })

    out.sort(key=lambda x: x["sold_qty"], reverse=True)
    return jsonify({ "rows": out[:100] })

# --------------------------------------------------------------------------------------
# Health
# --------------------------------------------------------------------------------------

@bp.get("/api/ping")
def api_ping():
    return jsonify({"ok": True})
