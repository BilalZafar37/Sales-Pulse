# config.py

import os
from zoneinfo import ZoneInfo

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', 'AsauDaisFdoijaGosancKanc')
    
    SMTP_SERVER="smtp.office365.com"
    SMTP_PORT=587
    SMTP_SENDER = "Sales Pulse"
    SALES_PULSE_LOGIN_URL = "http://localhost:5000/auth/login"
    SMTP_USERNAME = 'podms@modern-electronics.com'
    SMTP_PASSWORD = 'wzgspmbnkdvshyyy'
    
    DRIVER = "ODBC Driver 17 for SQL Server"
    USERNAME = "mec_wh"
    PSSWD = "mecwh@(2023)"
    SERVERNAME = "172.16.2.96"
    DATABASE = "MEC_WH_inventory_Management"
    
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
STATIC_DIR = os.path.join(BASE_DIR, ".", "static")

TZ_RIYADH = ZoneInfo("Asia/Riyadh") 