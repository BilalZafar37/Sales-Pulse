# blueprints/user_admin.py
from collections import defaultdict
from datetime import datetime
from functools import wraps

from flask import (
    Blueprint, render_template, request, redirect,
    url_for, flash, jsonify, abort, session
)
from flask_login import login_required, current_user
from sqlalchemy import select, delete
from sqlalchemy.exc import IntegrityError
from werkzeug.security import generate_password_hash

# === import your Session and models ===
from models import (
    model, func,
    SP_Users, SP_UserBrand, SP_UserCategory, SP_UserCustomer,
    Brands, SP_Customer, SP_CategoriesMappingMain
)

import re, secrets, string

from config import STATIC_DIR, BASE_DIR, Config
from utils.emailing import send_email
import html

bp = Blueprint("user_admin", __name__, static_folder=STATIC_DIR, url_prefix="/admin/users")

# --- tiny role gate (admin or developer only for user admin) ---
def require_role(*allowed):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            r = getattr(current_user, "role", None) or request.cookies.get("role") or None
            # if your app stores 'role' in session, you can also read from session['role'] here
            if not current_user.is_authenticated or (allowed and r not in set(allowed)):
                return abort(403)
            return fn(*args, **kwargs)
        return wrapper
    return deco

# =========================
# User Creation (GET/POST)
# =========================

@bp.get("/create")
@login_required
@require_role("admin", "developer")
def users_create_form():
    """Render the Create User form with brand/category/customer lists."""
    brands = model.execute(select(Brands).order_by(Brands.BrandName)).scalars().all()
    categories = model.execute(select(SP_CategoriesMappingMain).order_by(SP_CategoriesMappingMain.ID)).scalars().all()
    customers = model.execute(select(SP_Customer).order_by(SP_Customer.CustCode)).scalars().all()
    
    creator_role = getattr(current_user, "role", None) or session.get("role") or ""

    return render_template(
        "UserCreation.html",
        brands=brands,
        categories=categories,
        customers=customers,
        creator_role=creator_role,   # <<<<<<<<<<<<<<<<<<
    )



def _gen_password(length=12):
    alphabet = string.ascii_letters + string.digits + "!@#%^*-_=+?"
    return "".join(secrets.choice(alphabet) for _ in range(length))

def _slug_username_from_email(email):
    local = (email.split("@", 1)[0] or "").strip()
    local = re.sub(r"[^A-Za-z0-9._-]+", ".", local)
    local = re.sub(r"\.{2,}", ".", local).strip("._-")
    return (local or "user")[:30]

def _ensure_unique_username(base):
    candidate = base
    i = 2
    while model.execute(select(SP_Users.UserID).where(SP_Users.Username == candidate)).first():
        suffix = f"-{i}"
        candidate = base[: (30 - len(suffix))] + suffix
        i += 1
    return candidate

def _fetch_access_names(brand_ids, cust_ids, cat_ids):
    brands = []
    customers = []
    categories = []
    if brand_ids:
        brands = model.execute(
            select(Brands.BrandID, Brands.BrandName).where(Brands.BrandID.in_(brand_ids))
        ).all()
    if cust_ids:
        customers = model.execute(
            select(SP_Customer.CustomerID, SP_Customer.CustCode, SP_Customer.CustName)
            .where(SP_Customer.CustomerID.in_(cust_ids))
        ).all()
    if cat_ids:
        categories = model.execute(
            select(
                SP_CategoriesMappingMain.ID,
                getattr(SP_CategoriesMappingMain, "CatCode"),
                getattr(SP_CategoriesMappingMain, "CatName"),
                getattr(SP_CategoriesMappingMain, "CatDesc"),
            ).where(SP_CategoriesMappingMain.ID.in_(cat_ids))
        ).all()
    return brands, customers, categories


@bp.post("/create")
@login_required
@require_role("admin", "developer")
def users_create_submit():
    f = request.form

    # The creator (current logged-in user)
    creator_role = getattr(current_user, "role", None) or session.get("role") or ""

    # Core fields
    username   = (f.get("username") or "").strip()
    fullname   = (f.get("fullname") or "").strip() or None
    email      = (f.get("email") or "").strip()
    role       = (f.get("role") or "").strip()
    company    = (f.get("company") or "").strip() or None
    department = (f.get("department") or "").strip() or None
    is_active  = 1 if f.get("is_active") in ("on", "true", "1") else 0

    pwd  = (f.get("password") or "").strip()
    pwd2 = (f.get("confirm_password") or "").strip()

    if not email or not role:
        flash("Email and Role are required.", "danger")
        return redirect(url_for("user_admin.users_create_form"))

    # Username fallback and uniqueness
    if not username:
        username = _slug_username_from_email(email)
    username = _ensure_unique_username(username)

    # Access scopes
    try:
        brand_ids = {int(x) for x in f.getlist("brands[]")}
        cat_ids   = {int(x) for x in f.getlist("categories[]")}
        cust_ids  = {int(x) for x in f.getlist("customers[]")}
    except ValueError:
        flash("Access lists contain invalid IDs.", "danger")
        return redirect(url_for("user_admin.users_create_form"))

    total_scopes = len(brand_ids) + len(cat_ids) + len(cust_ids)
    scoped_role = role in {"user", "brand_manager"}

    # Validate based on creator
    if creator_role == "developer":
        # Password optional; if both provided must match
        if (pwd or pwd2) and pwd != pwd2:
            flash("Passwords do not match.", "danger")
            return redirect(url_for("user_admin.users_create_form"))
        # If target is scoped, enforce at least one scope
        if scoped_role and total_scopes == 0:
            flash("For User/Brand Manager, select at least one access (Brand/Category/Customer).", "danger")
            return redirect(url_for("user_admin.users_create_form"))
        # Auto-generate if missing
        auto_generated = False
        if not pwd:
            pwd = _gen_password()
            auto_generated = True
    else:
        # Non-dev creators: password required + match
        if not pwd:
            flash("Password is required.", "danger")
            return redirect(url_for("user_admin.users_create_form"))
        if pwd != pwd2:
            flash("Passwords do not match.", "danger")
            return redirect(url_for("user_admin.users_create_form"))
        auto_generated = False
        if scoped_role and total_scopes == 0:
            flash("Please select at least one access scope: Brand, Category, or Customer.", "danger")
            return redirect(url_for("user_admin.users_create_form"))

    # Optional: validate FK existence for nicer feedback
    def _missing_ids(col, ids):
        if not ids:
            return set()
        existing = {r[0] for r in model.execute(select(col).where(col.in_(ids))).all()}
        return ids - existing

    miss = {}
    m = _missing_ids(Brands.BrandID, brand_ids)
    if m: miss["brands"] = sorted(m)
    m = _missing_ids(SP_Customer.CustomerID, cust_ids)
    if m: miss["customers"] = sorted(m)
    m = _missing_ids(SP_CategoriesMappingMain.ID, cat_ids)
    if m: miss["categories"] = sorted(m)
    if miss:
        flash(f"Some access IDs do not exist: {miss}", "danger")
        return redirect(url_for("user_admin.users_create_form"))

    # Persist
    try:
        # make sure we’re not in a leftover transaction from the read phase
        if model.in_transaction():
            model.rollback()
    
        u = SP_Users(
            Username=username,
            Password=generate_password_hash(pwd),
            Role=role,
            Email=email,
            Fullname=fullname,
            IsActive=bool(is_active),
            Company=company,
            Department=department,
            CreatedAt=func.now(),
        )
        model.add(u)
        model.flush()  # to get u.UserID
    
        if brand_ids:
            model.add_all([SP_UserBrand(UserID=u.UserID, BrandID=bid) for bid in brand_ids])
        if cat_ids:
            model.add_all([SP_UserCategory(UserID=u.UserID, CategoryID=cid) for cid in cat_ids])
        if cust_ids:
            model.add_all([SP_UserCustomer(UserID=u.UserID, CustomerID=ccid) for ccid in cust_ids])
    
        model.commit()

        # Optionally email credentials
        if f.get("send_credentials"):
            # Build access lists for the email body (names, not just IDs)
            provided_brands, provided_customers, provided_categories = _fetch_access_names(
                brand_ids, cust_ids, cat_ids
            )

            login_url =Config.SALES_PULSE_LOGIN_URL
            subject   = "Sales Pulse — Your Account Credentials"
            sender    = Config.SMTP_SENDER
            recipients = [email]

            # NOTE: Sending passwords by email is risky; do this only if org policy allows it.
            # Escape to avoid accidental HTML issues with special chars.
            esc_user = html.escape(username)
            esc_pass = html.escape(pwd)
            esc_role = html.escape(role)

            html_content = f"""
            <div style="font-family:Arial,Helvetica,sans-serif; font-size:14px; color:#222">
              <p>Hello {esc_user},</p>
              <p>Your new account has been created in <strong>Sales Pulse</strong>:</p>
              <ul>
                <li><strong>Username:</strong> {esc_user}</li>
                <li><strong>Password:</strong> {esc_pass}</li>
                <li><strong>Role:</strong> {esc_role}</li>
              </ul>
              <p>Login: <a href="{login_url}" target="_blank">{login_url}</a></p>
            """

            if provided_brands:
                html_content += "<p><strong>Brand Access:</strong></p><ul>"
                for bid, bname in provided_brands:
                    html_content += f"<li>{html.escape(str(bname))}</li>"
                html_content += "</ul>"

            if provided_customers:
                html_content += "<p><strong>Customer Access:</strong></p><ul>"
                for cid, code, name in provided_customers:
                    html_content += f"<li>{html.escape(code)} — {html.escape(name)}</li>"
                html_content += "</ul>"

            if provided_categories:
                html_content += "<p><strong>Category Access:</strong></p><ul>"
                for cid, ccode, cname, cdesc in provided_categories:
                    label = " — ".join([x for x in [ccode, cname, cdesc] if x])
                    html_content += f"<li>{html.escape(label)}</li>"
                html_content += "</ul>"

            html_content += """
              <p>If you have any questions or issues, reply to this email.</p>
              <p>Best regards,<br>Modern Electronics — Sales Pulse Team</p>
            </div>
            """

            # SMTP config (no hardcoding; use env or Flask config)
            smtp_server = Config.SMTP_SERVER
            smtp_port   = int(Config.SMTP_PORT or 587)
            smtp_user   = Config.SMTP_USERNAME
            smtp_pass   = Config.SMTP_PASSWORD

            try:
                send_email(subject, smtp_user, recipients, html_content,
                           smtp_server=smtp_server, smtp_port=smtp_port,
                           smtp_username=smtp_user, smtp_password=smtp_pass)
                if creator_role == "developer" and auto_generated:
                    flash(f"User '{username}' created. Credentials emailed (auto-generated password).", "success")
                else:
                    flash(f"User '{username}' created and credentials emailed.", "success")
            except Exception as mail_err:
                # Don’t fail creation if email fails
                if creator_role == "developer" and auto_generated:
                    flash(f"User '{username}' created. Email failed; share this password securely: {pwd}", "warning")
                else:
                    flash(f"User '{username}' created, but sending email failed- ERROR: {mail_err}", "warning")
        else:
            # No email requested
            if creator_role == "developer" and auto_generated:
                flash(f"User '{username}' created. Auto-generated password: {pwd}", "success")
            else:
                flash(f"User '{username}' created successfully.", "success")

        return redirect(url_for("user_admin.users_create_form"))

    except IntegrityError:
        model.rollback()
        flash("Could not create user. Username or Email already exists.", "danger")
        return redirect(url_for("user_admin.users_create_form"))
    except Exception as e:
        model.rollback()
        flash(f"Unexpected error occurred while creating user: ERROR: {e}", "danger")
        return redirect(url_for("user_admin.users_create_form"))

# ===================================
# Access Management (GET + Save POST)
# ===================================

@bp.get("/access")
@login_required
@require_role("admin", "developer")
def users_access_page():
    """
    Render the UI with:
      - Users: list with data-* attributes prefilled (brand_ids, customer_ids, category_ids)
      - Also pass full lookups so the template can render JS arrays via Jinja, if desired.
    """
    # Lookups
    brands = model.execute(select(Brands).order_by(Brands.BrandName)).scalars().all()
    customers = model.execute(
        select(SP_Customer).order_by(SP_Customer.CustCode, SP_Customer.CustName)
    ).scalars().all()
    categories = model.execute(
        select(SP_CategoriesMappingMain).order_by(SP_CategoriesMappingMain.ID)
    ).scalars().all()
    
    
    brands_json = [{"id": b.BrandID, "BrandName": b.BrandName} for b in brands]
    customers_json = [
        {"id": c.CustomerID, "CustCode": c.CustCode, "CustName": c.CustName}
        for c in customers
    ]
    categories_json = [
        {
            "id": k.ID,
            "CatName": getattr(k, "CatName", None) or "",
            "CatDesc": getattr(k, "CatDesc", None) or "",
        }
        for k in categories
    ]

    # Prefetch all mappings once, group by user
    all_brand_map = defaultdict(list)
    all_cust_map = defaultdict(list)
    all_cat_map = defaultdict(list)

    for row in model.execute(select(SP_UserBrand.UserID, SP_UserBrand.BrandID)).all():
        all_brand_map[row[0]].append(row[1])
    for row in model.execute(select(SP_UserCustomer.UserID, SP_UserCustomer.CustomerID)).all():
        all_cust_map[row[0]].append(row[1])
    for row in model.execute(select(SP_UserCategory.UserID, SP_UserCategory.CategoryID)).all():
        all_cat_map[row[0]].append(row[1])

    # Build a light dict for each user so template can do:
    #   {{ u.brand_ids|join(',') }} etc
    user_rows = model.execute(select(SP_Users).order_by(SP_Users.Username)).scalars().all()
    users = []
    for u in user_rows:
        users.append({
            "UserID": u.UserID,
            "Username": u.Username,
            "Email": u.Email or "",
            "Role": u.Role,
            "brand_ids": all_brand_map.get(u.UserID, []),
            "customer_ids": all_cust_map.get(u.UserID, []),
            "category_ids": all_cat_map.get(u.UserID, []),
        })

    # Render
    return render_template(
        "./auth/User_access.html",
        users=users,
        brands=brands_json,
        customers=customers_json,
        categories=categories_json,
    )


@bp.post("/access/save")
@login_required
@require_role("admin", "developer")
def users_access_save():
    """
    Save access sets from the drag-and-drop UI.
    Accepts JSON from the client with structure like:
      {
        "user_id": 2,
        "brands": {"assigned":[1,2], "available":[...]},
        "customers": {"assigned":[101,104], "available":[...]},
        "categories": {"assigned":[10,12], "available":[...]}
      }
    Returns a small JSON message; the page itself stays rendered as-is.
    """
    payload = request.get_json(silent=True) or {}
    user_id = payload.get("user_id")
    if not user_id:
        return jsonify({"ok": False, "msg": "Missing user_id"}), 400

    # Validate user exists
    u = model.get(SP_Users, int(user_id))
    if not u:
        return jsonify({"ok": False, "msg": "User not found"}), 404

    # Extract assigned sets (ignore "available")
    brand_ids = set(payload.get("brands", {}).get("assigned", []))
    cust_ids  = set(payload.get("customers", {}).get("assigned", []))
    cat_ids   = set(payload.get("categories", {}).get("assigned", []))

    try:
        # Replace strategy: delete old → insert new (fast & predictable)
        model.execute(delete(SP_UserBrand).where(SP_UserBrand.UserID == u.UserID))
        model.execute(delete(SP_UserCustomer).where(SP_UserCustomer.UserID == u.UserID))
        model.execute(delete(SP_UserCategory).where(SP_UserCategory.UserID == u.UserID))

        if brand_ids:
            model.add_all([SP_UserBrand(UserID=u.UserID, BrandID=bid) for bid in brand_ids])
        if cust_ids:
            model.add_all([SP_UserCustomer(UserID=u.UserID, CustomerID=cid) for cid in cust_ids])
        if cat_ids:
            model.add_all([SP_UserCategory(UserID=u.UserID, CategoryID=cid) for cid in cat_ids])

        model.commit()
        return jsonify({"ok": True, "msg": "Access updated."})

    except IntegrityError:
        model.rollback()
        return jsonify({"ok": False, "msg": "Constraint error. Check IDs exist."}), 400
    except Exception:
        model.rollback()
        return jsonify({"ok": False, "msg": "Unexpected error."}), 500
