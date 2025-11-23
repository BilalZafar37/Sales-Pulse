# blueprints/Sell-In.py
from flask import (
    render_template,
    request,
    redirect,
    url_for,
    flash,
    jsonify,
    current_app,
    Blueprint,
    abort,
    send_file,
    send_from_directory,
)
import uuid
from sqlalchemy.engine import Engine
from collections        import defaultdict
from dateutil import parser
import datetime
from models             import(model, SP_SellInFilters, RTOS_MCSI, SP_MCSI_SellIn, Brands, Integer, Numeric, Float, String, Date, DateTime, func, UserGridPrefs, SP_InventoryLedger, SP_Customer, SP_SKU   )
import io, csv, json
import os
from typing import Iterable

from sqlalchemy.exc import IntegrityError
from sqlalchemy import types as SAT
import random
import string
from decimal import Decimal

from dateutil import parser as dateparser
from datetime import date, datetime

from config import STATIC_DIR, BASE_DIR, TZ_RIYADH

from apscheduler.schedulers.background import BackgroundScheduler

import pyodbc

bp = Blueprint('sell_in', __name__, static_folder=STATIC_DIR, url_prefix='/sell-in')
PAGE_KEY = "sellin_captures"

# Which columns we’ll offer multi-selects for:
ALLOWED_DISTINCT_FIELDS = {
    "SalesOffice","SalesGroup","SoldToParty","Payer",
    "ProductHierarchy1","ProductHierarchy2","Article","Brand",
    "BillingDocument"
}

@bp.route('/view', methods=['GET'])
def view_sellin():
    # column catalog (edit labels/types as needed)
    columns_meta = [
        {"key":"ID","label":"ID","type":"string","default_visible":False,"filterable":True},
        {"key":"SalesOffice","label":"Sales Office","type":"string","default_visible":True,"filterable":True},
        {"key":"SalesGroup","label":"Sales Group","type":"string","default_visible":True,"filterable":True},
        {"key":"SoldToParty","label":"Sold To Party","type":"string","default_visible":True,"filterable":True},
        {"key":"Payer","label":"Payer","type":"string","default_visible":False,"filterable":True},
        {"key":"ProductHierarchy1","label":"Hierarchy 1","type":"string","default_visible":True,"filterable":True},
        {"key":"ProductHierarchy2","label":"Hierarchy 2","type":"string","default_visible":True,"filterable":True},
        {"key":"Article","label":"Article","type":"string","default_visible":True,"filterable":True},
        {"key":"BillingDocument","label":"Billing Doc","type":"string","default_visible":True,"filterable":True},
        {"key":"Brand","label":"Brand","type":"string","default_visible":True,"filterable":True},
        {"key":"DocumentDate","label":"Document Date","type":"date","default_visible":True,"filterable":True},
        {"key":"GrInvSls","label":"GrInvSls","type":"numeric","default_visible":False,"filterable":True},
        {"key":"ProdDisc","label":"ProdDisc","type":"numeric","default_visible":False,"filterable":True},
        {"key":"RetnValue","label":"RetnValue","type":"numeric","default_visible":True,"filterable":True},
        {"key":"ReturnQty","label":"ReturnQty","type":"numeric","default_visible":True,"filterable":True},
        {"key":"CredMemos","label":"CredMemos","type":"numeric","default_visible":False,"filterable":True},
        {"key":"Net","label":"Net","type":"numeric","default_visible":True,"filterable":True},
        {"key":"GrossSale","label":"GrossSale","type":"numeric","default_visible":True,"filterable":True},
        {"key":"CreatedAt","label":"Created At","type":"date","default_visible":True,"filterable":True},
        {"key":"CapturedAt","label":"Captured At","type":"datetime","default_visible":True,"filterable":True},
    ]

    # load user prefs if any
    uid = request.cookies.get('uid') or 0  # replace with real user id from session
    pref = (model.query(UserGridPrefs)
            .filter_by(UserID=uid, PageKey=PAGE_KEY)
            .order_by(UserGridPrefs.UpdatedAt.desc())
            .first())
    user_prefs = {
        "visibleColumns": json.loads(pref.VisibleColumns) if pref and pref.VisibleColumns else [],
        "hiddenFilters":  json.loads(pref.HiddenFilters)  if pref and pref.HiddenFilters  else [],
        "perPage":        pref.PerPage if pref and pref.PerPage else 50
    }
    return render_template(
        './sellin/sellin.html',
        columns_meta_json=json.dumps(columns_meta),
        user_prefs_json=json.dumps(user_prefs)
    )
    
def _apply_filters(q, filters: dict):
    for key, f in (filters or {}).items():
        col = getattr(SP_MCSI_SellIn, key, None)
        if col is None:
            continue

        op = (f.get('op') or '').lower()

        if op == 'in':
            vals = f.get('values') or []
            if vals:
                q = q.filter(col.in_(vals))

        elif op == 'like':
            v = (f.get('value') or '').strip()
            if v:
                q = q.filter(col.ilike(f'%{v}%'))

        elif op in ('>=','>','=','<=','<'):
            v = f.get('value')
            if v is None:
                continue

            # Coerce by SQL type
            t = col.type
            if isinstance(t, (SAT.Integer, SAT.Numeric, SAT.Float, SAT.DECIMAL, SAT.BigInteger, SAT.SmallInteger)):
                comp_val = float(v)
            elif isinstance(t, (SAT.Date,)):
                comp_val = dateparser.isoparse(v).date()
            elif isinstance(t, (SAT.DateTime, SAT.TIMESTAMP)):
                comp_val = dateparser.isoparse(v)
            else:
                comp_val = v  # fallback: string compare

            expr = {
                '>=': col >= comp_val,
                '>':  col >  comp_val,
                '=':  col == comp_val,
                '<=': col <= comp_val,
                '<':  col <  comp_val,
            }[op]
            q = q.filter(expr)

        elif op == 'range':
            mn = f.get('min'); mx = f.get('max')
            if mn is not None:
                q = q.filter(col >= float(mn))
            if mx is not None:
                q = q.filter(col <= float(mx))

        elif op == 'between':
            a = f.get('from'); b = f.get('to')
            t = col.type
            if a:
                a_val = dateparser.isoparse(a).date() if isinstance(t, SAT.Date) else dateparser.isoparse(a)
                q = q.filter(col >= a_val)
            if b:
                b_val = dateparser.isoparse(b).date() if isinstance(t, SAT.Date) else dateparser.isoparse(b)
                q = q.filter(col <= b_val)

    return q

@bp.route('/captures/data', methods=['POST'])
def captures_data():
    try:
        payload = request.get_json(force=True)
        filters = payload.get('filters') or {}
        columns = payload.get('columns') or []
        offset  = int(payload.get('offset') or 0)
        limit   = min(int(payload.get('limit') or 50), 1000)  # cap
        sort    = payload.get('sort')  # {"key":"Net","dir":"asc"}

        q = model.query(SP_MCSI_SellIn)
        q = _apply_filters(q, filters)

        if sort and getattr(SP_MCSI_SellIn, sort.get('key',''), None):
            col = getattr(SP_MCSI_SellIn, sort['key'])
            q = q.order_by(col.asc() if sort.get('dir')=='asc' else col.desc())
        else:
            q = q.order_by(SP_MCSI_SellIn.CapturedAt.desc())

        total = q.count()
        rows  = q.offset(offset).limit(limit).all()

        # serialize only requested columns
        out_rows = []
        for r in rows:
          d = {}
          for k in columns:
            v = getattr(r, k, None)
            if isinstance(v, datetime):
              d[k] = v.strftime('%Y-%m-%d %H:%M:%S')
            # if isinstance(v, Float):
            #     d[k] = float(v.d) if v is not None else None
            else:
              d[k] = str(v) if (v is not None and not isinstance(v,(int,float))) else v
          out_rows.append(d)

        next_offset = offset + len(rows)
        return jsonify(ok=True, rows=out_rows, next_offset=next_offset, has_more=(next_offset < total))
    except Exception as e:
        model.rollback()
        return jsonify(ok=False, error=str(e)), 500

@bp.route('/captures/distinct', methods=['GET'])
def captures_distinct():
    field = (request.args.get('field') or '').strip()
    term  = (request.args.get('q') or '').strip()
    page  = int(request.args.get('page') or 1)
    if not field:
        return jsonify(items=[], more=False)

    # (Optional) harden which fields can be queried
    if ALLOWED_DISTINCT_FIELDS and field not in ALLOWED_DISTINCT_FIELDS:
        return jsonify(items=[], more=False)

    col = getattr(SP_MCSI_SellIn, field, None)
    if col is None:
        return jsonify(items=[], more=False)

    # Base query: DISTINCT non-null / non-empty values
    q = model.query(col)\
             .filter(col.isnot(None))\
             .filter(func.nullif(func.trim(col), '') != None)

    # Case-insensitive search that works on SQL Server/others
    if term:
        q = q.filter(func.lower(col).like(f"%{term.lower()}%"))

    # Ordering (alphabetical)
    q = q.distinct().order_by(col)

    # Pagination
    page_size = 30
    offs = (page - 1) * page_size
    # fetch one extra to detect "more"
    rows = q.offset(offs).limit(page_size + 1).all()

    # coerce to strings and strip
    items = []
    for r in rows[:page_size]:
        v = getattr(r, field, None)
        if v is None:
            continue
        if isinstance(v, (datetime, date)):
            v = v.isoformat()
        else:
            v = str(v).strip()
        if v:
            items.append(v)

    more = len(rows) > page_size
    return jsonify(items=items, more=more)

# Export (CSV). scope: 'current' or 'all'
@bp.route('/captures/export', methods=['POST'])
def captures_export():
    try:
        payload = json.loads(request.form.get('payload') or '{}')
        filters = payload.get('filters') or {}
        columns = payload.get('columns') or []
        scope   = payload.get('scope') or 'current'
        offset  = int(payload.get('offset') or 0)
        limit   = int(payload.get('limit') or 1000)
        sort    = payload.get('sort')

        q = model.query(SP_MCSI_SellIn)
        q = _apply_filters(q, filters)
        if sort and getattr(SP_MCSI_SellIn, sort.get('key',''), None):
            col = getattr(SP_MCSI_SellIn, sort['key'])
            q = q.order_by(col.asc() if sort.get('dir')=='asc' else col.desc())

        if scope == 'current':
            q = q.offset(offset).limit(limit)

        rows = q.all()
        si = io.StringIO()
        w = csv.writer(si)
        w.writerow(columns)
        for r in rows:
            w.writerow([getattr(r, k, '') for k in columns])

        bio = io.BytesIO(si.getvalue().encode('utf-8'))
        fname = f"sellin_export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        return send_file(bio, as_attachment=True, download_name=fname, mimetype='text/csv')
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

# Preferences save/load
@bp.route('/gridprefs/save', methods=['POST'])
def gridprefs_save():
    try:
        data = request.get_json(force=True)
        page_key = data['page_key']
        uid = request.cookies.get('uid') or 0  # replace with session user id
        pref = (model.query(UserGridPrefs)
                .filter_by(UserID=uid, PageKey=page_key)
                .first())
        if not pref:
            pref = UserGridPrefs(UserID=uid, PageKey=page_key)
            model.add(pref)
        pref.VisibleColumns = json.dumps(data.get('visible_columns') or [])
        pref.HiddenFilters  = json.dumps(data.get('hidden_filters') or [])
        pref.PerPage        = int(data.get('per_page') or 50)
        pref.UpdatedAt      = datetime.utcnow()
        model.commit()
        return jsonify(ok=True)
    except Exception as e:
        model.rollback()
        return jsonify(ok=False, error=str(e)), 500

@bp.route('/filters', methods=['GET', 'POST'])
def set_user_filters():
    # --- HANDLE FORM SUBMISSION ----------------
    if request.method == 'POST':
        # disable all old filters
        model.query(SP_SellInFilters).update({ SP_SellInFilters.IsActive: False })
        model.commit()

        # parse rows: we expect form data like filters-0-field, filters-0-op, filters-0-value…
        i = 0
        while True:
            field = request.form.get(f'filters-{i}-field')
            op    = request.form.get(f'filters-{i}-op')
            vals  = request.form.getlist(f'filters-{i}-value')
            if not field or not op:
                break
            # allow single free-text or list
            payload = vals if len(vals)>1 or field in ALLOWED_DISTINCT_FIELDS else vals[0]
            rec = SP_SellInFilters(
                FieldName=field,
                Operator=op,
                FieldValues=json.dumps(payload),
                IsActive=True
            )
            model.add(rec)
            i += 1

        model.commit()
        flash(f"{i} filters saved.", "success")
        return redirect(url_for('.set_user_filters'))

    # --- PRELOAD DATA FOR TEMPLATE -------------
    # 1) Distinct values for multi-select columns
    options = {}
    for col in ALLOWED_DISTINCT_FIELDS:
        if col == 'Brand':
            opts = [b.BrandName for b in model.query(Brands).distinct(Brands.BrandName)]
        else:
            opts = [
                getattr(row, col)
                for row in model
                              .query(getattr(RTOS_MCSI, col))
                              .distinct()
                              .order_by(getattr(RTOS_MCSI, col))
                              .all()
            ]
        options[col] = sorted([o for o in opts if o is not None])

    # 2) Existing active filters
    existing = []
    for f in model.query(SP_SellInFilters).filter_by(IsActive=True).all():
        try:
            vals = json.loads(f.FieldValues)
        except Exception:
            vals = f.FieldValues
        # normalize to list for the template
        if not isinstance(vals, list):
            vals = [vals]
        existing.append({
            'field':    f.FieldName,
            'operator': f.Operator,
            'values_list':   vals
        })

    # operator list
    ops = ['IN', 'NOT IN', '=', '>', '<', '>=', '<=']   
    
    def looks_numeric(val):
        # allow floats, ints, null/empty
        if val in (None, ''):
            return True
        try:
            float(val)
            return True
        except ValueError:
            return False
    
    # Build metadata for each field: its type + its distinct values
    field_meta = {}
    for col, vals in options.items():
        # grab the underlying Column object
        col_obj = getattr(RTOS_MCSI, col).property.columns[0]
        col_type = col_obj.type
        is_sql_numeric = isinstance(col_type, (Integer, Numeric, Float, Date, DateTime))

        # Check actual distinct values: if all non-null values parse as float, treat as numeric
        all_numeric = all(looks_numeric(v) for v in vals)

        ftype = 'numeric' if (is_sql_numeric or all_numeric) else 'string'

        field_meta[col] = {
            'type':   ftype,
            'values': vals
        }

    return render_template(
        './sellin/sellinFilters.html',
        field_meta_json=json.dumps(field_meta),
        options=options,
        existing=existing,
        all_fields=list(options.keys()),
        operators=ops
    )

# -----------------------------
# Small helpers (new / updated)
# -----------------------------
def _rand_alnum(n=6):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))

def _auto_billing_doc(prefix="FAKE"):
    return f"{prefix}-{datetime.utcnow().strftime('%Y%m%d')}-{_rand_alnum(4)}"

def _coerce_date(s, fallback=None):
    if not s: return fallback
    try: return dateparser.isoparse(s).date()
    except Exception: return fallback

def _coerce_float(s, fallback=None):
    try: return float(s) if s not in (None, "", "null") else fallback
    except Exception: return fallback

def _safe_commit():
    try:
        model.commit()
        return True, None
    except IntegrityError as ie:
        model.rollback()
        return False, str(ie.orig)
    except Exception as e:
        model.rollback()
        return False, str(e)

def _resolve_customer_id(sold_to_party: str):
    """Map SoldToParty (name) -> CustomerID."""
    if not sold_to_party:
        return None
    row = (model.query(SP_Customer.CustomerID)
           .filter(SP_Customer.CustName == sold_to_party)
           .first())
    return row[0] if row else None

def _resolve_sku_id(article_code: str):
    """Map Article -> SKU_ID."""
    if not article_code:
        return None
    row = (model.query(SP_SKU.SKU_ID)
           .filter(SP_SKU.ArticleCode == article_code)
           .first())
    return row[0] if row else None

def _idempotency_key_for_si(article: str, billing_doc: str, doc_date, sold_to: str):
    """Stable key so re-captures don't duplicate ledger rows."""
    if isinstance(doc_date, datetime):
        doc_date = doc_date.date()
    dd = doc_date.isoformat() if isinstance(doc_date, date) else str(doc_date or "")
    return f"SI:{article}:{billing_doc}:{sold_to}:{dd}"

def _ensure_ledger_for_si_row(si_row: SP_MCSI_SellIn):
    cust_id = _resolve_customer_id(si_row.SoldToParty)
    sku_id  = _resolve_sku_id(si_row.Article)
    if not cust_id or not sku_id:
        return (False, "Mapping not found")

    # Build common idempotency base
    idem = _idempotency_key_for_si(
        article=si_row.Article,
        billing_doc=si_row.BillingDocument or "",
        doc_date=si_row.DocumentDate,
        sold_to=si_row.SoldToParty or ""
    )

    posted_any = False

    # 1) Main SELLIN if GrossSale > 0
    qty = float(si_row.GrossSale or 0.0)
    if qty > 0:
        exists = (model.query(SP_InventoryLedger)
                  .filter(SP_InventoryLedger.IdempotencyKey == idem)
                  .first())
        if not exists:
            model.add(SP_InventoryLedger(
                CustomerID=cust_id,
                SKU_ID=sku_id,
                DocDate=si_row.DocumentDate,
                MovementType='SELLIN',
                MovementSubType=None,
                Qty=qty,  # positive receipt
                UploadID=None,
                RefTable='SP_MCSI_SellIn',
                RefID=f"{si_row.Article}/{si_row.BillingDocument or ''}",
                IdempotencyKey=idem,
                CreatedAt=datetime.now(TZ_RIYADH)
            ))
            posted_any = True

    # 2) Return handling (run regardless of GrossSale)
    ret_q_raw = si_row.ReturnQty
    try:
        ret_q = float(ret_q_raw) if ret_q_raw not in (None, "", "null") else 0.0
    except Exception:
        ret_q = 0.0

    if ret_q != 0.0:
        # normalize to NEGATIVE to reduce stock
        ret_qty = ret_q if ret_q < 0 else -ret_q

        idem_ret = f"{idem}:RET"
        exists_ret = (model.query(SP_InventoryLedger)
                      .filter(SP_InventoryLedger.IdempotencyKey == idem_ret)
                      .first())
        if not exists_ret:
            model.add(SP_InventoryLedger(
                CustomerID=cust_id,
                SKU_ID=sku_id,
                DocDate=si_row.DocumentDate,
                MovementType='SELLIN',  
                MovementSubType='RETURN',
                Qty=ret_qty,                     # negative to reduce customer stock
                UploadID=None,
                RefTable='SP_MCSI_SellIn',
                RefID=f"{si_row.Article}/{si_row.BillingDocument or ''}:RET",
                IdempotencyKey=idem_ret,
                CreatedAt=datetime.now(TZ_RIYADH)
            ))
            posted_any = True

    return (posted_any, None if posted_any else "No rows posted")



# -------------------------------
# Manual capture/delete endpoints
# -------------------------------

@bp.route('/capture-now', methods=['POST'])
def capture_now():
    from_date_str = request.form.get('from_date')
    to_date_str   = request.form.get('to_date')
    try:
        if from_date_str and to_date_str:
            start = parser.isoparse(from_date_str).date()
            end   = parser.isoparse(to_date_str).date()
        else:
            today = datetime.date.today()
            start = end = today

        filters = get_user_filters()

        # Ensure a clean slate in the capture table for that date window (your existing behavior)
        model.query(SP_MCSI_SellIn) \
             .filter(SP_MCSI_SellIn.DocumentDate >= start) \
             .filter(SP_MCSI_SellIn.DocumentDate <= end) \
             .delete(synchronize_session=False)
        model.commit()

        # Also remove any SELLIN ledger rows for that window created from SP_MCSI_SellIn (by RefTable)
        model.query(SP_InventoryLedger) \
             .filter(SP_InventoryLedger.MovementType == 'SELLIN') \
             .filter(SP_InventoryLedger.RefTable == 'SP_MCSI_SellIn') \
             .filter(SP_InventoryLedger.DocDate >= start) \
             .filter(SP_InventoryLedger.DocDate <= end) \
             .delete(synchronize_session=False)
        model.commit()

        # Re-capture and re-build ledger
        capture_filtered_sellin(filters, from_date=start, to_date=end, also_write_ledger=True)
        flash(f"Sell-In capture + ledger posted for {start} to {end}", "success")
    except Exception as e:
        current_app.logger.exception("Error in manual capture")
        flash(f"Error running capture: {e}", "danger")
    return redirect(url_for('sell_in.set_user_filters'))

@bp.route('/delete-sellin', methods=['POST'])
def delete_sellin():
    from_date_str = request.form.get('from_date')
    to_date_str   = request.form.get('to_date')
    try:
        if from_date_str and to_date_str:
            start = parser.isoparse(from_date_str).date()
            end   = parser.isoparse(to_date_str).date()
        else:
            today = datetime.date.today()
            start = end = today
    except Exception as exc:
        flash(f"Invalid date format: {exc}", "danger")
        return redirect(url_for('sell_in.set_user_filters'))

    try:
        model.query(SP_MCSI_SellIn) \
             .filter(SP_MCSI_SellIn.DocumentDate >= start) \
             .filter(SP_MCSI_SellIn.DocumentDate <= end) \
             .delete(synchronize_session=False)
        # Remove matching SELLIN ledger rows we created from Sell-In
        model.query(SP_InventoryLedger) \
             .filter(SP_InventoryLedger.MovementType == 'SELLIN') \
             .filter(SP_InventoryLedger.RefTable == 'SP_MCSI_SellIn') \
             .filter(SP_InventoryLedger.DocDate >= start) \
             .filter(SP_InventoryLedger.DocDate <= end) \
             .delete(synchronize_session=False)
        model.commit()
        flash(f"Deleted Sell-In + ledger from {start} to {end}", "success")
    except Exception as e:
        current_app.logger.exception("Error deleting sell-in data")
        flash(f"Error deleting Sell-In data: {e}", "danger")
    return redirect(url_for('sell_in.set_user_filters'))


# --------------------------------------------------
# Backfill ledger for already-captured Sell-In rows
# --------------------------------------------------
@bp.route('/backfill-ledger', methods=['POST'])
def backfill_ledger():
    """
    Create missing SELLIN ledger rows for SP_MCSI_SellIn in a date window.
    Useful after deploying this change the first time.
    """
    from_date_str = request.form.get('from_date')
    to_date_str   = request.form.get('to_date')
    try:
        if from_date_str and to_date_str:
            start = parser.isoparse(from_date_str).date()
            end   = parser.isoparse(to_date_str).date()
        else:
            today = datetime.date.today()
            start = end = today
    except Exception as exc:
        return jsonify(ok=False, error=f"Invalid date: {exc}"), 400

    rows = (model.query(SP_MCSI_SellIn)
            .filter(SP_MCSI_SellIn.DocumentDate >= start)
            .filter(SP_MCSI_SellIn.DocumentDate <= end)
            .order_by(SP_MCSI_SellIn.DocumentDate.asc())
            .all())

    created = skipped = missing = 0
    for si in rows:
        made, msg = _ensure_ledger_for_si_row(si)
        if made:
            created += 1
        else:
            if msg == "Mapping not found":
                missing += 1
            else:
                skipped += 1

    ok, err = _safe_commit()
    if not ok:
        return jsonify(ok=False, error=f"Commit failed: {err}"), 500

    return jsonify(ok=True, created=created, skipped=skipped, missing_mapping=missing)


# -----------------------------------------
# Capture pipeline — now writes LEDGER too
# -----------------------------------------
def get_user_filters():
    filters = []
    active = (model.query(
                SP_SellInFilters.FieldName,
                SP_SellInFilters.Operator,
                SP_SellInFilters.FieldValues)
              .filter(SP_SellInFilters.IsActive == True).all())
    for field_name, operator, field_values in active:
        try:
            values = json.loads(field_values)
        except json.JSONDecodeError:
            continue
        if not isinstance(values, list):
            filters.append((field_name, operator, values))
        else:
            filters.append((field_name, operator, values))
    return filters

def capture_filtered_sellin(filters, from_date=None, to_date=None, also_write_ledger=True):
    """
    Pull rows from RTOS_MCSI matching active filters (and optional date override),
    write into SP_MCSI_SellIn, and (optionally) create SELLIN ledger entries
    consumed by FIFO-aging & other blueprints. Idempotent on ledger via key.
    """
    query = model.query(RTOS_MCSI)

    for field, operator, values in (filters or []):
        column = getattr(RTOS_MCSI, field, None)
        if not column:
            continue

        if field == "DocumentDate" and not from_date and not to_date and operator in {">", "<", "=", ">=", "<="}:
            dt = parser.isoparse(values if isinstance(values, str) else values[0])
            if operator == ">":  query = query.filter(column > dt)
            elif operator == "<": query = query.filter(column < dt)
            elif operator in ("=", "=="): query = query.filter(column == dt)
            elif operator == ">=": query = query.filter(column >= dt)
            elif operator == "<=": query = query.filter(column <= dt)
        elif isinstance(values, list):
            if operator.upper() == "IN":
                query = query.filter(column.in_(values))
            elif operator.upper() == "NOT IN":
                query = query.filter(~column.in_(values))

    if from_date and to_date:
        query = query.filter(RTOS_MCSI.DocumentDate >= from_date,
                             RTOS_MCSI.DocumentDate <= to_date)

    rows = query.all()

    # Write captured Sell-In rows (idempotency at DB level depends on your PK/constraints)
    captured_rows = []
    for r in rows:
        rec = SP_MCSI_SellIn(
            ID=r.ID,
            SalesOffice=r.SalesOffice,
            SalesGroup=r.SalesGroup,
            SoldToParty=r.SoldToParty,
            Payer=r.Payer,
            ProductHierarchy1=r.ProductHierarchy1,
            ProductHierarchy2=r.ProductHierarchy2,
            Article=r.Article,
            BillingDocument=r.BillingDocument,
            Brand=r.Brand,
            DocumentDate=r.DocumentDate,
            GrInvSls=r.GrInvSls,
            ProdDisc=r.ProdDisc,
            RetnValue=r.RetnValue,
            ReturnQty=r.ReturnQty,
            CredMemos=r.CredMemos,
            Net=r.Net,
            GrossSale=r.GrossSale,
            CreatedAt=r.CreatedAt,
            CapturedAt=func.now()
        )
        model.add(rec)
        captured_rows.append(rec)

    ok, err = _safe_commit()
    if not ok:
        # Not fatal for ledger generation—log and proceed to try ledger for those that did insert
        current_app.logger.warning(f"Capture commit issue (some rows may have inserted): {err}")

    # For SKU # NEW
    # NEW: insert any newly introduced SKUs from the captured rows
    try:
        print("Upserting SKUs from Sell-In…")
        codes = [r.Article for r in captured_rows if r.Article]
        
        # Option A: run inside the same Session transaction (recommended if you want atomicity)
        add_missing_skus_via_tvp(model.get_bind(), codes)
        
        current_app.logger.info(f"SKUs upserted from Sell-In")
    except Exception:
        current_app.logger.exception("Failed to upsert SKUs from Sell-In rows")
    
    if also_write_ledger:
        created, skipped, missing = 0, 0, 0
        for si in captured_rows:
            try:
                made, msg = _ensure_ledger_for_si_row(si)
                if made:
                    created += 1
                else:
                    if msg == "Mapping not found":
                        missing += 1
                    else:
                        skipped += 1
            except Exception as e:
                current_app.logger.exception("Ledger add failed for Sell-In row")
        ok2, err2 = _safe_commit()
        if not ok2:
            current_app.logger.error(f"Ledger commit failed: {err2}")

        current_app.logger.info(
            f"Sell-In capture -> ledger: created={created}, skipped={skipped}, missing_map={missing}"
        )

# For SKU upset while api runs
def add_missing_skus_via_tvp(sess_or_engine, codes_iterable):
    # normalize + de-dup
    seen, rows = set(), []
    for c in codes_iterable:
        if not c:
            continue
        c2 = str(c).strip()
        if not c2 or c2 in seen:
            continue
        seen.add(c2)
        rows.append((c2,))

    if not rows:
        return

    # get raw connection
    if isinstance(sess_or_engine, Engine):
        raw = sess_or_engine.raw_connection()
        manage_commit = True
    else:
        conn = sess_or_engine.connection()
        raw = conn.connection
        manage_commit = False

    try:
        cur = raw.cursor()

        # TVP fast path if available
        if hasattr(pyodbc, "SQL_SS_TABLE"):
            tvp_cols = (('ArticleCode', pyodbc.SQL_WVARCHAR, 100, 0, False),)
            cur.setinputsizes([(pyodbc.SQL_SS_TABLE, tvp_cols)])
            tvp_value = ('dbo.ArticleCodeList', rows)
            cur.execute("{CALL dbo.usp_AddMissingSkusFromTVP (?)}", (tvp_value,))
        else:
            # Fallback: unique temp table name per call
            tmp = f"#Codes_{uuid.uuid4().hex[:8]}"
            cur.execute(f"CREATE TABLE {tmp} (ArticleCode NVARCHAR(100) NOT NULL PRIMARY KEY);")
            cur.fast_executemany = True
            cur.executemany(f"INSERT INTO {tmp}(ArticleCode) VALUES (?);", rows)
            cur.execute(f"""
                INSERT INTO dbo.SP_SKU(ArticleCode)
                SELECT c.ArticleCode
                FROM {tmp} AS c
                LEFT JOIN dbo.SP_SKU AS t WITH (INDEX(IX_SP_SKU_ArticleCode))
                       ON t.ArticleCode = c.ArticleCode
                WHERE t.ArticleCode IS NULL;
            """)
            # be nice and clean up
            cur.execute(f"DROP TABLE {tmp};")

        if manage_commit:
            raw.commit()
    finally:
        try: raw.close()
        except Exception: pass
  
# ---------------- Fake Sell-In helpers ---------------- #

def _rand_alnum(n=6):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))

def _auto_billing_doc(prefix="FAKE"):
    # Example: FAKE-20250820-AB12
    return f"{prefix}-{datetime.utcnow().strftime('%Y%m%d')}-{_rand_alnum(4)}"

def _coerce_date(s, fallback=None):
    if not s:
        return fallback
    try:
        return dateparser.isoparse(s).date()
    except Exception:
        return fallback

def _coerce_float(s, fallback=None):
    try:
        return float(s) if s not in (None, "", "null") else fallback
    except Exception:
        return fallback

def _safe_commit():
    try:
        model.commit()
        return True, None
    except IntegrityError as ie:
        model.rollback()
        return False, str(ie.orig)
    except Exception as e:
        model.rollback()
        return False, str(e)

# ---------------- Fake Sell-In: UI ---------------- #

@bp.route('/fake', methods=['GET'])
def fake_form():
    """
    Render a small form to create a fake Sell-In record.
    Only the provided fields will be used; the rest are auto-filled.
    """
    # You can preload brand list if you like; else leave blank.
    brands = [b.BrandName for b in model.query(Brands).distinct(Brands.BrandName)]
    return render_template('./sellin/fake_sellin.html', brands=brands, today=date.today().isoformat())

@bp.route('/fake', methods=['POST'])
def fake_create():
    """
    Create a single fake SP_MCSI_SellIn row (and optional matching ledger SELLIN).
    """
    # ---- collect user inputs (optional) ----
    sold_to_party  = (request.form.get('SoldToParty') or '').strip() or None
    article_code   = (request.form.get('Article') or '').strip() or None
    brand          = (request.form.get('Brand') or '').strip() or None
    sales_office   = (request.form.get('SalesOffice') or '').strip() or "TEST_OFFICE"
    sales_group    = (request.form.get('SalesGroup') or '').strip() or "TEST_GROUP"
    document_date  = _coerce_date(request.form.get('DocumentDate'), fallback=date.today())
    created_at     = _coerce_date(request.form.get('CreatedAt'),   fallback=document_date)
    billing_doc    = (request.form.get('BillingDocument') or '').strip() or _auto_billing_doc()
    gr_qty         = _coerce_float(request.form.get('GrossSale'),  fallback=10.0)   # quantity
    net_val        = _coerce_float(request.form.get('Net'),        fallback=1000.0) # value
    prod_disc      = _coerce_float(request.form.get('ProdDisc'),   fallback=0.0)
    cred_memos     = _coerce_float(request.form.get('CredMemos'),  fallback=0.0)
    retn_value     = _coerce_float(request.form.get('RetnValue'),  fallback=0.0)
    return_qty     = _coerce_float(request.form.get('ReturnQty'),  fallback=0.0)
    also_ledger    = request.form.get('also_ledger') == '1'

    # ---- defaults for any missing "required" Sell-In columns ----
    if not article_code:
        # fabricate a fake article code
        article_code = f"ART-{_rand_alnum(5)}"
    if not sold_to_party:
        sold_to_party = f"FAKE CUSTOMER {_rand_alnum(3)}"
    if not brand:
        brand = "TEST_BRAND"

    # ---- build row ----
    rec = SP_MCSI_SellIn(
        ID=_rand_alnum(8),                     # arbitrary ID (your table accepts string key)
        SalesOffice=sales_office,
        SalesGroup=sales_group,
        SoldToParty=sold_to_party,
        Payer=sold_to_party,                   # or leave None
        ProductHierarchy1="PH1-TEST",
        ProductHierarchy2="PH2-TEST",
        Article=article_code,
        BillingDocument=billing_doc,
        Brand=brand,
        DocumentDate=document_date,
        GrInvSls=net_val,         # if you track gross invoiced sales separately, adjust
        ProdDisc=prod_disc,
        RetnValue=retn_value,
        ReturnQty=return_qty,
        CredMemos=cred_memos,
        Net=net_val,
        GrossSale=gr_qty,
        CreatedAt=created_at,
        CapturedAt=func.now()
    )
    model.add(rec)
    ok, err = _safe_commit()
    if not ok:
        # retry with a different BillingDocument if PK conflict on (Article,BillingDocument,CreatedAt)
        if 'CK' in (err or '') or 'PRIMARY KEY' in (err or '') or 'duplicate' in (err or '').lower():
            rec.BillingDocument = _auto_billing_doc()
            model.add(rec)
            ok2, err2 = _safe_commit()
            if not ok2:
                flash(f"Failed to insert fake Sell-In: {err2}", "danger")
                return redirect(url_for('sell_in.fake_form'))
        else:
            flash(f"Failed to insert fake Sell-In: {err}", "danger")
            return redirect(url_for('sell_in.fake_form'))

    # ---- optional: add SELLIN to SP_InventoryLedger so FIFO aging sees it ----
    if also_ledger:
        # Try to map customer name -> ID, and article code -> SKU_ID
        cust = model.query(SP_Customer).filter(SP_Customer.CustName == sold_to_party).first()
        sku  = model.query(SP_SKU).filter(SP_SKU.ArticleCode == article_code).first()
        if cust and sku:
            led = SP_InventoryLedger(
                CustomerID=cust.CustomerID,
                SKU_ID=sku.SKU_ID,
                DocDate=document_date,
                MovementType='SELLIN',
                Qty=gr_qty,              # positive in (received by customer)
                UploadID=None
            )
            model.add(led)
            ok3, err3 = _safe_commit()
            if not ok3:
                flash(f"Fake Sell-In saved, but failed to add ledger entry: {err3}", "warning")
        else:
            flash("Fake Sell-In saved, but ledger entry skipped (Customer/SKU mapping not found).", "warning")

    flash("Fake Sell‑In created successfully.", "success")
    return redirect(url_for('sell_in.view_sellin'))

# ---------------- Fake Sell-In: JSON API ---------------- #

@bp.route('/fake/json', methods=['POST'])
def fake_create_json():
    """
    JSON API. Accepts optional keys:
      SoldToParty, Article, Brand, SalesOffice, SalesGroup, DocumentDate, CreatedAt,
      BillingDocument, GrossSale, Net, ProdDisc, CredMemos, RetnValue, ReturnQty,
      also_ledger (bool)
    """
    data = request.get_json(force=True) or {}

    sold_to_party  = (data.get('SoldToParty') or '').strip() or None
    article_code   = (data.get('Article') or '').strip() or None
    brand          = (data.get('Brand') or '').strip() or None
    sales_office   = (data.get('SalesOffice') or '').strip() or "TEST_OFFICE"
    sales_group    = (data.get('SalesGroup') or '').strip() or "TEST_GROUP"
    document_date  = _coerce_date(data.get('DocumentDate'), fallback=date.today())
    created_at     = _coerce_date(data.get('CreatedAt'),    fallback=document_date)
    billing_doc    = (data.get('BillingDocument') or '').strip() or _auto_billing_doc()
    gr_qty         = _coerce_float(data.get('GrossSale'),   fallback=10.0)
    net_val        = _coerce_float(data.get('Net'),         fallback=1000.0)
    prod_disc      = _coerce_float(data.get('ProdDisc'),    fallback=0.0)
    cred_memos     = _coerce_float(data.get('CredMemos'),   fallback=0.0)
    retn_value     = _coerce_float(data.get('RetnValue'),   fallback=0.0)
    return_qty     = _coerce_float(data.get('ReturnQty'),   fallback=0.0)
    also_ledger    = bool(data.get('also_ledger'))

    if not article_code:
        article_code = f"ART-{_rand_alnum(5)}"
    if not sold_to_party:
        sold_to_party = f"FAKE CUSTOMER {_rand_alnum(3)}"
    if not brand:
        brand = "TEST_BRAND"

    rec = SP_MCSI_SellIn(
        ID=_rand_alnum(8),
        SalesOffice=sales_office,
        SalesGroup=sales_group,
        SoldToParty=sold_to_party,
        Payer=sold_to_party,
        ProductHierarchy1="PH1-TEST",
        ProductHierarchy2="PH2-TEST",
        Article=article_code,
        BillingDocument=billing_doc,
        Brand=brand,
        DocumentDate=document_date,
        GrInvSls=net_val,
        ProdDisc=prod_disc,
        RetnValue=retn_value,
        ReturnQty=return_qty,
        CredMemos=cred_memos,
        Net=net_val,
        GrossSale=gr_qty,
        CreatedAt=created_at,
        CapturedAt=func.now()
    )
    model.add(rec)
    ok, err = _safe_commit()
    if not ok:
        if 'PRIMARY KEY' in (err or '') or 'duplicate' in (err or '').lower():
            rec.BillingDocument = _auto_billing_doc()
            model.add(rec)
            ok2, err2 = _safe_commit()
            if not ok2:
                return jsonify(ok=False, error=err2), 400
        else:
            return jsonify(ok=False, error=err), 400

    ledger_info = None
    if also_ledger:
        cust = model.query(SP_Customer).filter(SP_Customer.CustName == sold_to_party).first()
        sku  = model.query(SP_SKU).filter(SP_SKU.ArticleCode == article_code).first()
        if cust and sku:
            led = SP_InventoryLedger(
                CustomerID=cust.CustomerID,
                SKU_ID=sku.SKU_ID,
                DocDate=document_date,
                MovementType='SELLIN',
                Qty=gr_qty,
                UploadID=None
            )
            model.add(led)
            ok3, err3 = _safe_commit()
            if not ok3:
                ledger_info = {"ok": False, "error": err3}
            else:
                ledger_info = {"ok": True}
        else:
            ledger_info = {"ok": False, "error": "Customer/SKU mapping not found"}

    return jsonify(ok=True, billing_doc=rec.BillingDocument, article=rec.Article, created_at=str(created_at), ledger=ledger_info)


def schedule_capture_tasks():
    scheduler = BackgroundScheduler(timezone="Asia/Riyadh")
    scheduler.add_job(lambda: capture_filtered_sellin(get_user_filters()), 'cron', hour=10, minute=0)
    scheduler.add_job(lambda: capture_filtered_sellin(get_user_filters()), 'cron', hour=13, minute=15)
    scheduler.add_job(lambda: capture_filtered_sellin(get_user_filters()), 'cron', hour=17, minute=30)
    scheduler.start()