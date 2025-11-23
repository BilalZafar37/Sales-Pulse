from flask import Blueprint, render_template, redirect, url_for, flash, request, session, abort, jsonify
from flask_login import login_user, logout_user, login_required, UserMixin, current_user
from extensions import login_manager
from models import model, SP_Users, SP_UserBrand, SP_UserCategory, SP_UserCustomer, Brands, SP_CategoriesMappingMain, SP_Customer
from werkzeug.security import check_password_hash, generate_password_hash
from urllib.parse import quote
from datetime import datetime, timedelta
import os
from functools import wraps
from config import STATIC_DIR


bp = Blueprint('auth', __name__, url_prefix='/auth',
               template_folder='../templates/auth', static_folder=STATIC_DIR)



class User(UserMixin):
    def __init__(self, user_id, username, role, email, fullname):
        self.id = user_id
        self.username = username
        self.role = role
        self.email = email
        self.fullname = fullname

@login_manager.user_loader
def load_user(user_id):
    u = model.query(SP_Users).get(user_id)
    if not u or not u.IsActive:
        return None
    return User(u.UserID, u.Username, u.Role, u.Email, u.Fullname)

@bp.route("/sign-in", methods=["GET", "POST"])  # SIGN-IN /LOGIN PAGE
@bp.route("/login", methods=["GET", "POST"])
def login():
    if "username" in session:
        session.pop("username")
        logout_user()
    if request.method == "POST":
        if "login_attempts" not in session:
            session["login_attempts"] = 0
            session["lockout_time"] = None  # No lockout initially

        # Check if the user is locked out
        if session["login_attempts"] >= 10:
            # Check if lockout time has passed
            if session["lockout_time"]:
                lockout_end = datetime.strptime(
                    session["lockout_time"], "%Y-%m-%d %H:%M:%S"
                ) + timedelta(minutes=5)
                if datetime.now() > lockout_end:
                    # Reset attempts after 5 minutes
                    session["login_attempts"] = 0
                    session["lockout_time"] = None
                else:
                    return redirect(url_for("auth.no_access"))  # Still locked out
            else:
                # Set lockout time if not already set
                session["lockout_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                return redirect(url_for("auth.no_access"))  # Initial lockout

        
        username = request.form.get("username","")
        password = request.form.get("password","")

        u = model.query(SP_Users).filter_by(Username=username).first()
        if u and u.IsActive and check_password_hash(u.Password, password):
            user_obj = User(u.UserID, u.Username, u.Role, u.Email, u.Fullname)
            login_user(user_obj)

            # role + identity
            session["role"]      = u.Role
            session["username"]  = u.Username
            session["user_id"]   = u.UserID

            # ---- ACCESS LISTS ----
            # BRANDS -> names
            brand_rows = (model.query(Brands.BrandName)
                          .join(SP_UserBrand, SP_UserBrand.BrandID==Brands.BrandID)
                          .filter(SP_UserBrand.UserID==u.UserID).all())
            session["user_brand_access"] = [b[0] for b in brand_rows]

            # CATEGORIES -> IDs from mapping table
            cat_rows = (model.query(SP_UserCategory.CategoryID)
                        .filter(SP_UserCategory.UserID==u.UserID).all())
            session["user_category_access_ids"] = [cid[0] for cid in cat_rows]

            # CUSTOMERS -> IDs
            cust_rows = (model.query(SP_UserCustomer.CustomerID)
                         .filter(SP_UserCustomer.UserID==u.UserID).all())
            session["user_customer_access_ids"] = [cid[0] for cid in cust_rows]

            # Optional: send first-timers to your filter page
            return redirect(url_for("dashboard.dashboard_page"))
        else:
            # Increment attempt count
            session["login_attempts"] += 1

        return render_template("auth/sign-in.html", error="Invalid Credentials")
    return render_template("auth/sign-in.html")


def require_role(*allowed):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            r = session.get("role")
            if not current_user.is_authenticated or (allowed and r not in set(allowed)):
                return abort(403)
            return fn(*args, **kwargs)
        return wrapper
    return deco

@bp.route("/no-access")
def no_access():
    return render_template("auth/no-access.html")


# Optional: for APIs, return JSON 401 instead of HTML redirect
@login_manager.unauthorized_handler
def _unauth():
    wants_json = request.accept_mimetypes.best == "application/json" or request.path.startswith("/api/")
    if wants_json:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    # preserve 'next' so user returns to the page after login
    nxt = quote(request.full_path if request.query_string else request.path, safe="")
    return redirect(url_for("auth.login", next=nxt))

# ---- GLOBAL GUARD (applies to ALL blueprints) ----
@bp.before_app_request
def require_login_globally():
    """Force login for every request except whitelisted endpoints/paths."""
    # 1) Allow static files and favicons/robots
    if request.endpoint in (None, "static"):
        return
    if request.path.startswith(("/static/", "/favicon.ico", "/robots.txt")):
        return

    ep = (request.endpoint or "")
    # Skip static endpoints (global static and blueprint static)
    if (ep.endswith(".static")) or (ep == "static") or ep.endswith("static"):
        return  # allow static through without touching DB
    
    # 2) Allow auth pages themselves
    if request.endpoint.startswith("auth."):
        return


    # 4) Everything else requires login
    if not current_user.is_authenticated:
        return login_manager.unauthorized()

