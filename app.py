# app.py
from flask import Flask, request, redirect, url_for
from extensions import login_manager
from models import model
from blueprints.auth import bp as auth_bp, current_user
from blueprints.dashoard import bp as dashboard_bp
from blueprints.sell_in import bp as sell_in_bp
from blueprints.sell_out_blueprint.sell_out import bp as sell_out_bp
from blueprints.sell_out_blueprint.sell_out_attachments import bp as sell_out_uploads_bp
from blueprints.sell_out_blueprint.sell_out_approvals import bp as sell_out_apr_bp
from blueprints.cust_profile import bp as cust_profile_bp
from blueprints.soh import bp as soh_bp
from blueprints.reports_blueprint.fifo_aging_sqlalchemy import fifo_aging_bp
from blueprints.reports_blueprint.overselling_report import bp as overselling_bp
from blueprints.reports_blueprint.sales_pulse_general import bp as sales_pulse_general_bp
from blueprints.reports_blueprint.daily_report import daily_bp as daily_report_bp
from blueprints.masters_bp import bp as masters_bp
from blueprints.user_admin import bp as user_admin_bp
from blueprints.customer_management import bp as customer_mgmt_bp
from datetime import datetime, date
from config import Config
from datetime import timedelta


# def create_app():
app = Flask(__name__)

app.config.from_object(Config)
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=1)
login_manager.init_app(app)

from werkzeug.serving import WSGIRequestHandler

# keep a reference to the original
_orig_log_request = WSGIRequestHandler.log_request

def log_request_no_static(self, code='-', size='-'):
    # self.path is the raw path + querystring, e.g. "/static/js/app.js?…"
    path = self.path.split('?', 1)[0]

    # skip any you don't want to see
    if path.startswith((
        '/static/',
        '/.well-known/',
        '/favicon.ico',
    )):
        return

    # otherwise fall back to the normal logger
    return _orig_log_request(self, code, size)

# install our little patch
WSGIRequestHandler.log_request = log_request_no_static


# register blueprints

@app.route('/')
def home_redirect():
    return redirect(url_for('dashboard.dashboard_page'))

app.register_blueprint(auth_bp)             # /auth/login
app.register_blueprint(dashboard_bp)        # /dashboard/
app.register_blueprint(sell_in_bp)          # /sell-in/filters
app.register_blueprint(sell_out_bp)         # /sell-out/
app.register_blueprint(sell_out_uploads_bp) # /sell_out_uploads/ (filepond)
app.register_blueprint(sell_out_apr_bp)     # /sell-out-approvals/ (3 pages)
app.register_blueprint(cust_profile_bp)     # /sellout/profiles/
app.register_blueprint(soh_bp)              # /soh/
app.register_blueprint(fifo_aging_bp)       # /fifo_aging_bp/
app.register_blueprint(overselling_bp)      # /overselling/
app.register_blueprint(sales_pulse_general_bp)  # /sales_pulse_general/
app.register_blueprint(daily_report_bp)     # /daily_report/
app.register_blueprint(masters_bp)          # /masters/
app.register_blueprint(user_admin_bp)
app.register_blueprint(customer_mgmt_bp)   # /customer-mgmt/


# Register  filters
@app.template_filter('attr')
def attr_filter(obj, name):
    return getattr(obj, name, '')

@app.template_filter('usd')
def usd(value, places=2):
    try:
        num = float(value)
    except (TypeError, ValueError):
        return ''
    return f"{num:,.{places}f} USD"

@app.template_filter('pretty_date')
def pretty_date(value):
    if not value:
        return ""
    # handle both datetime.datetime and datetime.date
    if isinstance(value, (datetime, date)):
        day   = value.day
        month = value.strftime('%b').lower()  # e.g. “Apr” → “apr”
        year  = value.year
        return f"{day}-{month}-{year}"

    # if it’s a string in ISO form, try to parse it
    if isinstance(value, str):
        try:
            # date.fromisoformat will work if it’s “YYYY-MM-DD”
            d = date.fromisoformat(value)
            return f"{d.day}-{d.strftime('%b').lower()}-{d.year}"
        except ValueError:
            pass

    # fallback: render as-is
    return value




if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
