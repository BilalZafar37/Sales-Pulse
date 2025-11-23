# blueprints/daily_report.py
from __future__ import annotations
from flask import Blueprint, request, jsonify, render_template, Response, session
from datetime import date, datetime
from sqlalchemy import select, func, case, literal, and_, or_, Float, text, union_all
import io, csv, os

from models import (
    model,
    SP_InventoryLedger, SP_Customer, SP_SKU, SP_MCSI_SellIn
)

from config import STATIC_DIR

daily_bp = Blueprint(
    "daily_report",
    __name__,
    static_folder=STATIC_DIR,
    url_prefix="/daily"
)

# ---------- helpers ----------
def _parse_date(s, default=None):
    if not s:
        return default
    if isinstance(s, datetime): return s.date()
    if isinstance(s, date):     return s
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
        try: return datetime.strptime(s, fmt).date()
        except Exception: pass
    return default

def _parse_int(s):
    try: return int(s) if s not in (None, "", "null") else None
    except Exception: return None

def _listify_csv(s):
    if not s: return []
    return [x.strip() for x in str(s).split(",") if x.strip()]

def _brand_access_guard(sku_tbl):
    """Enforce brand access using session['user_brand_access'] and optional brand filter."""
    user_access = session.get("user_brand_access") or []
    brand_param = (request.args.get("brand") or "").strip() or None
    pred = literal(True)
    if brand_param:
        pred = and_(pred, sku_tbl.c.Brand == brand_param)
        if user_access:
            pred = and_(pred, sku_tbl.c.Brand.in_(user_access))
    elif user_access:
        pred = and_(pred, sku_tbl.c.Brand.in_(user_access))
    return pred

# ---------- core query ----------
def build_daily_stmt(
    customer_id: int,
    brand_name: str | None,
    date_from: date | None,
    date_to: date | None,
    limit: int | None,
    offset: int | None,
    *,
    article_code: str | None = None,
    sales_office: list[str] | None = None,
    sales_group: list[str] | None = None,
    export_scope: str | None = None,   # None | 'customer' | 'all_brands'
):
    L = SP_InventoryLedger.__table__
    C = SP_Customer.__table__
    S = SP_SKU.__table__
    SI = SP_MCSI_SellIn.__table__

    if not customer_id:
        raise ValueError("customer is required")
    # brand mandatory except for special export scopes
    brand_required = bool(brand_name)

    # brand guard + scope handling
    base_brand_guard = _brand_access_guard(S)  # respects user access
    sku_pred = [base_brand_guard]
    if export_scope is None:
        if not brand_required:
            raise ValueError("brand is required")
        sku_pred.append(S.c.Brand == brand_name)
    elif export_scope in ("customer", "all_brands"):
        pass
    else:
        raise ValueError("invalid export scope")

    si_date_pred, led_date_pred = [literal(True)], [literal(True)]
    if date_from:
        si_date_pred.append(SI.c.DocumentDate >= date_from)
        led_date_pred.append(L.c.DocDate >= date_from)
    if date_to:
        si_date_pred.append(SI.c.DocumentDate <= date_to)
        led_date_pred.append(L.c.DocDate <= date_to)

    # optional dimension filters
    if article_code:
        sku_pred.append(S.c.ArticleCode == article_code)

    # optional “EXISTS Sell-In that matches SO/SOH rows” for SalesOffice/Group
    # (narrows to SKUs & days that are relevant for those dimensions)
    exists_pred_for_dims = literal(True)
    if (sales_office or sales_group):
        exists = select(literal(1)).select_from(
            SI.join(C, C.c.CustName == SI.c.SoldToParty).join(S, S.c.ArticleCode == SI.c.Article)
        ).where(
            and_(
                C.c.CustomerID == customer_id,
                *si_date_pred,
                *sku_pred,
                *( [SI.c.SalesOffice.in_(sales_office)] if sales_office else [] ),
                *( [SI.c.SalesGroup.in_(sales_group)]  if sales_group  else [] ),
            )
        ).exists()
        exists_pred_for_dims = exists

    # ===== Daily Sell-In & returns =====
    SI_day = (
        select(
            C.c.CustomerID.label("CustomerID"),
            S.c.SKU_ID.label("SKU_ID"),
            S.c.ArticleCode.label("ArticleCode"),
            S.c.Description.label("Description"),
            S.c.Brand.label("Brand"),
            SI.c.DocumentDate.label("Day"),
            func.coalesce(func.sum(SI.c.GrossSale.cast(Float)), 0.0).label("si_qty"),
            func.coalesce(func.sum(SI.c.Net.cast(Float)),        0.0).label("si_val"),
            func.coalesce(func.sum(SI.c.ReturnQty.cast(Float)),  0.0).label("ret_qty"),
            func.coalesce(func.sum(SI.c.RetnValue.cast(Float)),  0.0).label("ret_val"),
        )
        .select_from(SI.join(C, C.c.CustName == SI.c.SoldToParty)
                      .join(S, S.c.ArticleCode == SI.c.Article))
        .where(and_(
            C.c.CustomerID == customer_id,
            *si_date_pred,
            *sku_pred,
            *( [SI.c.SalesOffice.in_(sales_office)] if sales_office else [] ),
            *( [SI.c.SalesGroup.in_(sales_group)]  if sales_group  else [] ),
        ))
        .group_by(C.c.CustomerID, S.c.SKU_ID, S.c.ArticleCode, S.c.Description, S.c.Brand, SI.c.DocumentDate)
        .cte("SI_day")
    )

    # ===== Daily Sell-Out (qty) =====
    SO_day = (
        select(
            L.c.CustomerID.label("CustomerID"),
            L.c.SKU_ID.label("SKU_ID"),
            S.c.ArticleCode.label("ArticleCode"),
            S.c.Description.label("Description"),
            S.c.Brand.label("Brand"),
            L.c.DocDate.label("Day"),
            func.coalesce(func.sum(case((L.c.Qty < 0, -L.c.Qty), else_=0.0)), 0.0).label("so_qty")
        )
        .select_from(L.join(S, S.c.SKU_ID == L.c.SKU_ID))
        .where(and_(
            L.c.CustomerID == customer_id,
            L.c.MovementType == "SELLOUT",
            *led_date_pred,
            *sku_pred,
            exists_pred_for_dims,
        ))
        .group_by(L.c.CustomerID, L.c.SKU_ID, S.c.ArticleCode, S.c.Description, S.c.Brand, L.c.DocDate)
        .cte("SO_day")
    )

    # ===== Avg unit price from SI (for Sell-Out value) =====
    Price = (
        select(
            C.c.CustomerID.label("CustomerID"),
            S.c.SKU_ID.label("SKU_ID"),
            (func.sum(SI.c.Net.cast(Float)) / func.nullif(func.sum(SI.c.GrossSale.cast(Float)), 0.0)).label("avg_unit_price")
        )
        .select_from(SI.join(C, C.c.CustName == SI.c.SoldToParty).join(S, S.c.ArticleCode == SI.c.Article))
        .where(and_(
            C.c.CustomerID == customer_id,
            *si_date_pred,
            *sku_pred,
            *( [SI.c.SalesOffice.in_(sales_office)] if sales_office else [] ),
            *( [SI.c.SalesGroup.in_(sales_group)]  if sales_group  else [] ),
        ))
        .group_by(C.c.CustomerID, S.c.SKU_ID)
        .cte("Price")
    )

    # ===== Daily movements & cumulative SOH =====
    DayMoves = (
        select(
            L.c.CustomerID.label("CustomerID"),
            L.c.SKU_ID.label("SKU_ID"),
            S.c.ArticleCode.label("ArticleCode"),
            S.c.Description.label("Description"),
            S.c.Brand.label("Brand"),
            L.c.DocDate.label("Day"),
            func.coalesce(func.sum(L.c.Qty.cast(Float)), 0.0).label("day_qty")
        )
        .select_from(L.join(S, S.c.SKU_ID == L.c.SKU_ID))
        .where(and_(
            L.c.CustomerID == customer_id,
            *led_date_pred,
            *sku_pred,
            exists_pred_for_dims,
        ))
        .group_by(L.c.CustomerID, L.c.SKU_ID, S.c.ArticleCode, S.c.Description, S.c.Brand, L.c.DocDate)
        .cte("DayMoves")
    )

    soh_cume = func.sum(DayMoves.c.day_qty).over(
        partition_by=[DayMoves.c.CustomerID, DayMoves.c.SKU_ID],
        order_by=[DayMoves.c.Day]
    ).label("soh_qty")

    SOH_by_day = select(
        DayMoves.c.CustomerID, DayMoves.c.SKU_ID, DayMoves.c.ArticleCode,
        DayMoves.c.Description, DayMoves.c.Brand, DayMoves.c.Day, soh_cume
    ).cte("SOH_by_day")
       
    # ===== distinct keys across sources =====
    keys_union = union_all(
        select(
            SI_day.c.CustomerID, SI_day.c.SKU_ID,
            SI_day.c.ArticleCode, SI_day.c.Description, SI_day.c.Brand,
            SI_day.c.Day
        ),
        select(
            SO_day.c.CustomerID, SO_day.c.SKU_ID,
            SO_day.c.ArticleCode, SO_day.c.Description, SO_day.c.Brand,
            SO_day.c.Day
        ),
        select(
            SOH_by_day.c.CustomerID, SOH_by_day.c.SKU_ID,
            SOH_by_day.c.ArticleCode, SOH_by_day.c.Description, SOH_by_day.c.Brand,
            SOH_by_day.c.Day
        ),
    ).subquery("Keys")
    
    Dist = (
        select(
            keys_union.c.CustomerID, keys_union.c.SKU_ID,
            keys_union.c.ArticleCode, keys_union.c.Description, keys_union.c.Brand,
            keys_union.c.Day
        )
        .group_by(
            keys_union.c.CustomerID, keys_union.c.SKU_ID,
            keys_union.c.ArticleCode, keys_union.c.Description, keys_union.c.Brand,
            keys_union.c.Day
        )
        .cte("Dist")
    )

    stmt = (
        select(
            Dist.c.Day.label("Date"),
            Dist.c.ArticleCode, Dist.c.Description, Dist.c.Brand,
            func.coalesce(SI_day.c.si_qty, 0.0).label("sellin_qty"),
            func.coalesce(SI_day.c.si_val, 0.0).label("sellin_val"),
            func.coalesce(SI_day.c.ret_qty, 0.0).label("return_qty"),
            func.coalesce(SI_day.c.ret_val, 0.0).label("return_val"),
            func.coalesce(SO_day.c.so_qty, 0.0).label("sellout_qty"),
            (func.coalesce(SO_day.c.so_qty, 0.0) * func.coalesce(Price.c.avg_unit_price, 0.0)).label("sellout_val"),
            func.coalesce(SOH_by_day.c.soh_qty, 0.0).label("current_soh"),
        )
        .select_from(
            Dist
            .join(SI_day, and_(SI_day.c.CustomerID==Dist.c.CustomerID, SI_day.c.SKU_ID==Dist.c.SKU_ID, SI_day.c.Day==Dist.c.Day), isouter=True)
            .join(SO_day, and_(SO_day.c.CustomerID==Dist.c.CustomerID, SO_day.c.SKU_ID==Dist.c.SKU_ID, SO_day.c.Day==Dist.c.Day), isouter=True)
            .join(SOH_by_day, and_(SOH_by_day.c.CustomerID==Dist.c.CustomerID, SOH_by_day.c.SKU_ID==Dist.c.SKU_ID, SOH_by_day.c.Day==Dist.c.Day), isouter=True)
            .join(Price, and_(Price.c.CustomerID==Dist.c.CustomerID, Price.c.SKU_ID==Dist.c.SKU_ID), isouter=True)
        )
        .where(Dist.c.CustomerID == customer_id)
        .order_by(Dist.c.Day.asc(), Dist.c.ArticleCode.asc())
    )

    if limit is not None:  stmt = stmt.limit(limit)
    if offset is not None: stmt = stmt.offset(offset)
    return stmt

# ---------- routes ----------
@daily_bp.route("/", methods=["GET"])
def page():
    return render_template("./reports/daily_report.html")

@daily_bp.route("/data", methods=["GET"])
def data():
    # mandatory via names (searchable selects)
    brand_name   = (request.args.get("brand") or "").strip()
    # customer may come as id (select value) or name (fallback)
    customer_id  = _parse_int(request.args.get("customer_id"))
    customer_nm  = (request.args.get("customer_name") or "").strip() or None

    if not customer_id and customer_nm:
        row = model.query(SP_Customer.CustomerID).filter(SP_Customer.CustName == customer_nm).first()
        customer_id = row[0] if row else None

    date_from    = _parse_date(request.args.get("date_from"))
    date_to      = _parse_date(request.args.get("date_to"))
    limit        = _parse_int(request.args.get("limit"))  or 100
    offset       = _parse_int(request.args.get("offset")) or 0
    article_code = (request.args.get("article_code") or "").strip() or None
    sales_office = _listify_csv(request.args.get("sales_office"))
    sales_group  = _listify_csv(request.args.get("sales_group"))

    try:
        stmt = build_daily_stmt(
            customer_id=customer_id,
            brand_name=brand_name,
            date_from=date_from, 
            date_to=date_to,
            limit=limit, offset=offset,
            article_code=article_code,
            sales_office=sales_office or None,
            sales_group =sales_group  or None,
            export_scope=None
        )
    except ValueError as ve:
        return jsonify(ok=False, error=str(ve)), 400

    rows = model.execute(stmt).mappings().all()
    # probe for more
    next_stmt = build_daily_stmt(
        customer_id, brand_name, date_from, date_to,
        limit=1, offset=offset + len(rows),
        article_code=article_code,
        sales_office=sales_office or None,
        sales_group =sales_group  or None,
        export_scope=None
    )
    has_more = bool(model.execute(next_stmt).mappings().all())

    # header label
    cust = model.query(SP_Customer.CustName).filter(SP_Customer.CustomerID==customer_id).first()
    return jsonify(
        ok=True,
        items=[dict(r) for r in rows],
        next_offset=offset + len(rows),
        has_more=has_more,
        header={"brand": brand_name, "customer": (cust[0] if cust else customer_nm or "")}
    )

@daily_bp.route("/export.csv", methods=["GET"])
def export_csv():
    brand_name  = (request.args.get("brand") or "").strip()
    customer_id = _parse_int(request.args.get("customer_id"))
    customer_nm = (request.args.get("customer_name") or "").strip() or None
    if not customer_id and customer_nm:
        row = model.query(SP_Customer.CustomerID).filter(SP_Customer.CustName == customer_nm).first()
        customer_id = row[0] if row else None

    date_from   = _parse_date(request.args.get("date_from"))
    date_to     = _parse_date(request.args.get("date_to"))
    scope       = (request.args.get("scope") or "filtered").lower()
    article_code= (request.args.get("article_code") or "").strip() or None
    sales_office= _listify_csv(request.args.get("sales_office"))
    sales_group = _listify_csv(request.args.get("sales_group"))

    export_scope = None
    if scope == "filtered":
        export_scope = None
    elif scope == "customer":
        export_scope = "customer"
    elif scope == "all_brands":
        export_scope = "all_brands"
    else:
        return jsonify(ok=False, error="invalid scope"), 400

    stmt = build_daily_stmt(
        customer_id=customer_id,
        brand_name=brand_name,
        date_from=date_from, date_to=date_to,
        limit=None, offset=None,
        article_code=article_code,
        sales_office=sales_office or None,
        sales_group =sales_group  or None,
        export_scope=export_scope
    )
    rows = model.execute(stmt).mappings().all()
    data = [dict(r) for r in rows]

    si = io.StringIO()
    w = csv.writer(si)
    headers = ["Date","ArticleCode","Description","Brand",
               "sellin_qty","sellin_val","return_qty","return_val",
               "sellout_qty","sellout_val","current_soh"]
    w.writerow(headers)
    for r in data:
        w.writerow([r.get(h) for h in headers])

    fname = f"daily_{(date_from or date.today()).isoformat()}_{(date_to or date.today()).isoformat()}_{scope}.csv"
    return Response(si.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={fname}"})

# ---------- searchable choices (Select2) ----------
@daily_bp.route("/choices/brands", methods=["GET"])
def choices_brands():
    q = (request.args.get("q") or "").strip()
    page = int(request.args.get("page") or 1); page_size = 30
    S = SP_SKU.__table__
    pred = _brand_access_guard(S)
    stmt = select(S.c.Brand).where(and_(pred, S.c.Brand.isnot(None)))
    if q: stmt = stmt.where(S.c.Brand.ilike(f"%{q}%"))
    stmt = stmt.group_by(S.c.Brand).order_by(S.c.Brand).offset((page-1)*page_size).limit(page_size+1)
    rows = model.execute(stmt).fetchall()
    items = [{"id": r[0], "text": r[0]} for r in rows[:page_size]]
    return jsonify(results=items, pagination={"more": len(rows) > page_size})

@daily_bp.route("/choices/customers", methods=["GET"])
def choices_customers():
    q = (request.args.get("q") or "").strip()
    page = int(request.args.get("page") or 1); page_size = 30
    stmt = model.query(SP_Customer.CustomerID, SP_Customer.CustName).order_by(SP_Customer.CustName)
    if q:
        stmt = stmt.filter(SP_Customer.CustName.ilike(f"%{q}%"))
    rows = stmt.offset((page-1)*page_size).limit(page_size+1).all()
    items = [{"id": cid, "text": name} for (cid, name) in rows[:page_size]]
    return jsonify(results=items, pagination={"more": len(rows) > page_size})

@daily_bp.route("/choices/articles", methods=["GET"])
def choices_articles():
    q = (request.args.get("q") or "").strip()
    page = int(request.args.get("page") or 1); page_size = 30
    S = SP_SKU.__table__
    pred = _brand_access_guard(S)
    stmt = select(S.c.ArticleCode).where(and_(pred, S.c.ArticleCode.isnot(None)))
    if q: stmt = stmt.where(S.c.ArticleCode.ilike(f"%{q}%"))
    stmt = stmt.group_by(S.c.ArticleCode).order_by(S.c.ArticleCode).offset((page-1)*page_size).limit(page_size+1)
    rows = model.execute(stmt).fetchall()
    items = [{"id": r[0], "text": r[0]} for r in rows[:page_size]]
    return jsonify(results=items, pagination={"more": len(rows) > page_size})

@daily_bp.route("/choices/sales_office", methods=["GET"])
def choices_sales_office():
    q = (request.args.get("q") or "").strip()
    page = int(request.args.get("page") or 1); page_size = 30
    SI = SP_MCSI_SellIn.__table__
    stmt = select(SI.c.SalesOffice).where(SI.c.SalesOffice.isnot(None))
    if q: stmt = stmt.where(SI.c.SalesOffice.ilike(f"%{q}%"))
    stmt = stmt.group_by(SI.c.SalesOffice).order_by(SI.c.SalesOffice).offset((page-1)*page_size).limit(page_size+1)
    rows = model.execute(stmt).fetchall()
    items = [{"id": r[0], "text": r[0]} for r in rows[:page_size]]
    return jsonify(results=items, pagination={"more": len(rows) > page_size})

@daily_bp.route("/choices/sales_group", methods=["GET"])
def choices_sales_group():
    q = (request.args.get("q") or "").strip()
    page = int(request.args.get("page") or 1); page_size = 30
    SI = SP_MCSI_SellIn.__table__
    stmt = select(SI.c.SalesGroup).where(SI.c.SalesGroup.isnot(None))
    if q: stmt = stmt.where(SI.c.SalesGroup.ilike(f"%{q}%"))
    stmt = stmt.group_by(SI.c.SalesGroup).order_by(SI.c.SalesGroup).offset((page-1)*page_size).limit(page_size+1)
    rows = model.execute(stmt).fetchall()
    items = [{"id": r[0], "text": r[0]} for r in rows[:page_size]]
    return jsonify(results=items, pagination={"more": len(rows) > page_size})
