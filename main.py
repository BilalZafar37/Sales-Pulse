import pandas as pd
# from models             import *
# from app               import app
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import *
from config import Config
import urllib.parse


def db_connection():
    DRIVER     = "ODBC Driver 17 for SQL Server"
    USERNAME   = Config.USERNAME
    PSSWD      = Config.PSSWD
    SERVERNAME = Config.SERVERNAME
    DATABASE   = Config.DATABASE

    # Build the raw ODBC connection string
    odbc_str = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVERNAME};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PSSWD};"
        f"MARS_Connection=Yes"
    )

    # URL‐encode it for embedding as odbc_connect
    connect_arg = urllib.parse.quote_plus(odbc_str)

    # Now create the engine using the odbc_connect parameter
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={connect_arg}",
        pool_size=30,
        max_overflow=10,
        pool_timeout=30,
        pool_pre_ping=True,
        fast_executemany=True,
        pool_recycle=3600
    )
    return engine
engine = db_connection()



SessionLocal = sessionmaker(bind=engine)

Base = declarative_base()

class SP_Customer(Base):
    __tablename__ = "SP_Customer"
    CustomerID = Column(Integer, primary_key=True)
    CustCode   = Column(String(50), unique=True, nullable=False)
    CustName   = Column(Unicode(255), nullable=False)
    LevelType  = Column(String(10), nullable=False)  # 'HO'/'Branch'
    ParentCustID = Column(Integer, ForeignKey("SP_Customer.CustomerID"))

    # New FK to status (the default constraint is in DB; ORM just reflects)
    # StatusID     = Column(Integer, ForeignKey("SP_Status.StatusID"), nullable=False, server_default=text("1"))

    # Relationship to status dictionary
    # status = relationship("SP_Status", back_populates="customers")



def load_c2h_to_sp_customer(session, xlsx_path: str, sheet_name: str = "Sheet1"):
    # 1) Read and normalize Excel
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name)
    df = df.rename(columns=lambda c: str(c).strip())
    required = ["Customer Number", "Customer Name", "H.O. Code", "H.O Name"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in '{sheet_name}': {missing}")

    df = df[required].copy()
    df.columns = ["CustCode", "CustName", "HOCode", "HOName"]

    # Clean strings
    for c in ["CustCode", "CustName", "HOCode", "HOName"]:
        df[c] = df[c].astype(str).str.strip()

    # Unique list of customers (as-is)
    custs = df[["CustCode", "CustName"]].drop_duplicates(subset=["CustCode"]).reset_index(drop=True)

    # Which codes are HOs? (any code that appears in HOCode)
    ho_codes = set(df["HOCode"].unique())

    # Build a simple cust -> HOCode mapping (first occurrence wins)
    # If a customer has itself as HO (CustCode == HOCode), we'll treat parent as NULL.
    cust_to_hocode = (
        df.drop_duplicates(subset=["CustCode"])[["CustCode", "HOCode"]]
        .set_index("CustCode")["HOCode"]
        .to_dict()
    )

    try:
        session.query(SP_Customer).delete() 
        session.flush()

        # 3) Insert all customers "as is" (set LevelType based on whether code is an HO somewhere)
        objs = []
        for _, r in custs.iterrows():
            code = r["CustCode"]
            name = (r["CustName"] or "")[:100]
            level = "HO" if code in ho_codes else "Branch"
            objs.append(SP_Customer(
                CustCode=code,
                CustName=name,
                LevelType=level,     # required non-null in your model
                ParentCustID=None    # set in next step
            ))
        session.add_all(objs)
        session.flush()  # assign CustomerID

        # Map code -> id for quick parent lookup
        code_to_id = {c.CustCode: c.CustomerID for c in session.query(SP_Customer).all()}

        # 4) Set ParentCustID from HOCode (NULL if HOCode missing or equals self)
        for c in session.query(SP_Customer).all():
            hocode = cust_to_hocode.get(c.CustCode)
            if hocode and hocode != c.CustCode:
                parent_id = code_to_id.get(hocode)
                c.ParentCustID = parent_id if parent_id else None
            else:
                c.ParentCustID = None  # self-HO or no mapping → no parent

        session.commit()
        print(f"Inserted customers: {len(objs)}. Parent IDs populated from HOCode.")
    except Exception:
        session.rollback()
        raise





if __name__ == "__main__":
    session = SessionLocal()
    try:
        load_c2h_to_sp_customer(session, r"./Related Documents/C2H Mapping.xlsx", "Sheet1")
    finally:
        session.close()