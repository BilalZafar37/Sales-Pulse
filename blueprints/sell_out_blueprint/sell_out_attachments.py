# blueprints/sell_out_attachments.py (or inside your existing sell_out blueprint)
from flask import Blueprint, request, send_from_directory, current_app, abort, Response
from werkzeug.utils import secure_filename
from uuid import uuid4
import os
import re
from config import STATIC_DIR, BASE_DIR
from models import model, SP_Customer, SP_SellOutUploadFile, SP_SellOutUploads  # to resolve customer_id → name

bp = Blueprint("sell_out_attachments", __name__, url_prefix="/sell_out_uploads")

# ---- config ----
ALLOWED_EXTS = {"pdf", "png", "jpg", "jpeg", "gif", "webp", "tif", "tiff", "xlsx", "xls", 'CSV'}
UPLOAD_ROOT  = os.path.normpath(os.path.join(STATIC_DIR, "uploads"))

def _slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return re.sub(r"-+", "-", s).strip("-") or "unknown"

def _allowed(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTS

def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def _resolve_customer_name(customer_id: int | None, customer_name: str | None) -> str:
    if customer_name:
        return customer_name
    if customer_id:
        row = model.query(SP_Customer.CustName).filter(SP_Customer.CustomerID == customer_id).scalar()
        if row:
            return row
    return "UnknownCustomer"

# === FilePond: PROCESS ===
@bp.route("/upload-attachment", methods=["POST"])
def fp_process():
    """
    Expects:
      - file field name: 'filepond'
      - either 'customer_id' (int) or 'customer_name' in form data
    Returns:
      - plain text "serverId" that FilePond stores (we use "<slug>/<filename>")
    """
    f = request.files.get("filepond")
    if not f or f.filename == "":
        return ("No file uploaded", 400)

    if not _allowed(f.filename):
        return ("Unsupported file type", 415)

    # REQUIRED: the Sell-Out UploadID to tie this file to
    upload_id = request.form.get("upload_id", type=int)
    if not upload_id:
        return ("Missing upload_id", 400)
    # Validate UploadID exists (and is yours, if you enforce ownership)
    exists = model.query(SP_SellOutUploads.UploadID).filter(SP_SellOutUploads.UploadID == upload_id).scalar()
    if not exists:
        return ("Invalid upload_id", 404)
    
    # Resolve customer name and make folder
    customer_id   = request.form.get("customer_id", type=int)
    customer_name = (request.form.get("customer_name") or "").strip() or None
    cust_name     = _resolve_customer_name(customer_id, customer_name)
    cust_slug     = _slug(cust_name)

    save_dir = os.path.join(UPLOAD_ROOT, cust_slug)
    _ensure_dir(save_dir)

    # Unique file name
    orig_name = secure_filename(f.filename)
    uid       = uuid4().hex
    server_fn = f"{uid}__{orig_name}"
    save_path = os.path.join(save_dir, server_fn)
    f.save(save_path)

    # FilePond expects a plain text ID; we’ll encode relative path "<slug>/<filename>"
    server_id = f"{cust_slug}/{server_fn}"
    
    try:
        size_bytes = os.path.getsize(save_path)
    except Exception:
        size_bytes = None

    model.add(SP_SellOutUploadFile(
        UploadID     = upload_id,
        ServerID     = server_id,
        OriginalName = orig_name,
        MimeType     = f.mimetype or None,
        SizeBytes    = size_bytes,
        UploadedBy   = (request.form.get("actor") or request.headers.get("X-User") or None),
    ))
    model.commit()

    
    return Response(server_id, mimetype="text/plain")

# === FilePond: REVERT (delete by serverId) ===
@bp.route("/upload-attachment/revert", methods=["DELETE", "POST"])
def fp_revert():
    """
    FilePond sends the serverId in the request body (raw text) or form.
    We support both.
    """
    server_id = request.data.decode().strip() or request.form.get("serverId", "").strip()
    if not server_id:
        return ("Missing serverId", 400)

    # server_id pattern: "<slug>/<filename>"
    rel_path = server_id.replace("..", "")
    abs_path = os.path.join(UPLOAD_ROOT, rel_path)
    if os.path.isfile(abs_path):
        try:
            os.remove(abs_path)
        except Exception as e:
            current_app.logger.exception("Failed to delete uploaded file")
            return (str(e), 500)
    
     # delete DB row
    try:
        model.query(SP_SellOutUploadFile).filter(SP_SellOutUploadFile.ServerID == server_id).delete()
        model.commit()
    except Exception as e:
        model.rollback()
        current_app.logger.exception("Failed to delete DB row for attachment")
        return (str(e), 500)


    # FilePond expects 200 with empty body on success
    return ("", 200)

# === FilePond: LOAD (serve the file by serverId) ===
@bp.route("/upload-attachment/load/<path:server_id>", methods=["GET"])
def fp_load(server_id):
    """
    Serve the file back to FilePond by the serverId we previously returned.
    """
    rel_path = server_id.replace("..", "")
    parts = rel_path.split("/", 1)
    if len(parts) != 2:
        abort(404)

    cust_slug, filename = parts
    directory = os.path.join(UPLOAD_ROOT, cust_slug)
    return send_from_directory(directory, filename, as_attachment=False)

# === FilePond: FETCH (optional: proxy remote URLs) ===
@bp.route("/upload-attachment/fetch/<path:url>", methods=["GET"])
def fp_fetch(url):
    """
    Optional; if you load remote URLs. For now, reject (or implement a proxy if you need).
    """
    return ("Remote fetch not enabled", 501)
