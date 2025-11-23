from sqlalchemy import *
# from sqlalchemy import coalesce
import urllib.parse
from datetime import datetime
from sqlalchemy.orm import (
        foreign, joinedload,  Mapped, mapped_column, 
        sessionmaker, scoped_session, with_loader_criteria, 
        declarative_base, aliased , relationship, backref
    )
# from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy import event, literal_column
from config import Config
from flask import session
from sqlalchemy.dialects import mssql

import pyodbc


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

# initialize extensions
Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
model = Session()
Base = declarative_base()
@event.listens_for(model, "do_orm_execute")
def _add_rbac_filters(execute_state):
    if not execute_state.is_select:
        return

    role = session.get("role") or ""
    # elevated roles see all data
    if role in {"developer", "admin", "finance_manager"}:
        return

    brands   = set(session.get("user_brand_access") or [])
    cats     = set(session.get("user_category_access_ids") or [])
    cust_ids = set(session.get("user_customer_access_ids") or [])

    opts = []

    # BRAND: apply to any mapped class having a Brand column
    if brands:
        for m in Base.registry.mappers:
            cls = m.class_
            if hasattr(cls, "Brand"):
                opts.append(
                    with_loader_criteria(
                        cls, lambda c: c.Brand.in_(brands), include_aliases=True
                    )
                )

    # CUSTOMER: apply to any mapped class having a CustomerID column
    if cust_ids:
        for m in Base.registry.mappers:
            cls = m.class_
            if hasattr(cls, "CustomerID"):
                opts.append(
                    with_loader_criteria(
                        cls, lambda c: c.CustomerID.in_(cust_ids), include_aliases=True
                    )
                )

    # CATEGORY: if a class has CategoryMappingID, filter directly; if it has SKU_ID,
    # filter via EXISTS(select SP_SKU where SP_SKU.SKU_ID=cls.SKU_ID AND Category in allowed)
    if cats:
        # 1) direct category-bearing tables
        for m in Base.registry.mappers:
            cls = m.class_
            if hasattr(cls, "CategoryMappingID"):
                opts.append(
                    with_loader_criteria(
                        cls, lambda c: c.CategoryMappingID.in_(cats), include_aliases=True
                    )
                )
        # 2) SKU_ID-based tables (e.g., SP_MCSI_SellOut)
        from sqlalchemy.sql import exists, select, and_
        for m in Base.registry.mappers:
            cls = m.class_
            if hasattr(cls, "SKU_ID"):
                opts.append(
                    with_loader_criteria(
                        cls,
                        lambda c: exists(
                            select(SP_SKU.SKU_ID).where(
                                and_(SP_SKU.SKU_ID == c.SKU_ID,
                                     SP_SKU.CategoryMappingID.in_(cats))
                            )
                        ),
                        include_aliases=True
                    )
                )

    if opts:
        execute_state.statement = execute_state.statement.options(*opts)

# --- NEW: Users & access maps (simplest shape) ------------------------------
class SP_Users(Base):
    __tablename__ = "SP_Users"
    UserID    = Column(Integer, primary_key=True, autoincrement=True)
    Username  = Column(String(100), nullable=False, unique=True, index=True)
    Password  = Column(String(255), nullable=False)     # store hash here
    Role      = Column(String(30),  nullable=False, index=True)  # 'developer'|'admin'|'brand_manager'|'finance_manager'|'user'
    Email     = Column(String(150), nullable=False, unique=True, index=True)
    Fullname  = Column(String(150))
    IsActive  = Column(Boolean, nullable=False, server_default=text("1"))
    Company   = Column(String(200))
    Department= Column(String(200))
    CreatedAt = Column(DateTime, server_default=func.now())

class Brands(Base):
    __tablename__ = 'RTOS_Brands'

    BrandID = Column(Integer, primary_key=True, autoincrement=True)
    BrandName = Column(String(100), nullable=False)

# brand/category/customer mapping tables
class SP_UserBrand(Base):
    __tablename__ = "SP_UserBrand"
    ID      = Column(Integer, primary_key=True, autoincrement=True)
    UserID  = Column(Integer, ForeignKey("SP_Users.UserID"), nullable=False)
    BrandID = Column(Integer, ForeignKey("RTOS_Brands.BrandID"), nullable=False)
    __table_args__ = (UniqueConstraint("UserID", "BrandID", name="UQ_UserBrand"),)

class SP_UserCategory(Base):
    __tablename__ = "SP_UserCategory"
    ID       = Column(Integer, primary_key=True, autoincrement=True)
    UserID   = Column(Integer, ForeignKey("SP_Users.UserID"), nullable=False)
    CategoryID = Column(Integer, ForeignKey("SP_CategoriesMappingMain.ID"), nullable=False)
    __table_args__ = (UniqueConstraint("UserID", "CategoryID", name="UQ_UserCategory"),)

class SP_UserCustomer(Base):
    __tablename__ = "SP_UserCustomer"
    ID         = Column(Integer, primary_key=True, autoincrement=True)
    UserID     = Column(Integer, ForeignKey("SP_Users.UserID"), nullable=False)
    CustomerID = Column(Integer, ForeignKey("SP_Customer.CustomerID"), nullable=False)
    __table_args__ = (UniqueConstraint("UserID", "CustomerID", name="UQ_UserCustomer"),)

# ------------------------------------------------------------
# A) Status dictionary
# ------------------------------------------------------------
class SP_Status(Base):
    __tablename__ = "SP_Status"

    StatusID   = Column(Integer, primary_key=True, autoincrement=True)
    StatusName = Column(String(100), nullable=False, unique=True)

    # backref to customers (optional but handy)
    customers = relationship("SP_Customer", back_populates="status")

    __table_args__ = (
        # mirrors: CREATE UNIQUE INDEX UX_SP_Status_StatusName ON SP_Status(StatusName);
        Index("UX_SP_Status_StatusName", "StatusName", unique=True),
    )


# ------------------------------------------------------------
# E) Global config (thresholds etc.)
# ------------------------------------------------------------
class SP_GlobalConfig(Base):
    __tablename__ = "SP_GlobalConfig"

    Key       = Column(String(100), primary_key=True)      # [Key]
    Value     = Column(String(100), nullable=False)         # [Value]
    UpdatedAt = Column(DateTime, nullable=False, server_default=text("SYSUTCDATETIME()"))



class SP_CustomerStatusTag(Base):
    __tablename__ = "SP_CustomerStatusTag"
    CustomerID = Column(Integer, ForeignKey("SP_Customer.CustomerID"), primary_key=True)
    StatusID   = Column(Integer, ForeignKey("SP_Status.StatusID"), primary_key=True)
    UpdatedAt  = Column(DateTime, nullable=False, server_default=text("SYSUTCDATETIME()"))


# ------------------------------------------------------------
# Generic user audit
# ------------------------------------------------------------
class SP_UserAudit(Base):
    __tablename__ = "SP_UserAudit"

    AuditID        = Column(Integer, primary_key=True, autoincrement=True)

    # WHO
    UserID         = Column(Integer, nullable=True)
    Username       = Column(NVARCHAR(100), nullable=True)

    # WHAT
    TableName      = Column(String(128), nullable=False)  # SYSNAME ~ NVARCHAR(128)
    EntityPK       = Column(NVARCHAR(200), nullable=False)
    Action         = Column(NVARCHAR(20),  nullable=False)

    # DETAILS
    ColumnsChanged = Column(NVARCHAR(4000), nullable=True)
    PreviousValue  = Column(NVARCHAR(None), nullable=True)  # NVARCHAR(MAX)
    NewValue       = Column(NVARCHAR(None), nullable=True)  # NVARCHAR(MAX)
    Reason         = Column(NVARCHAR(500),  nullable=True)
    Source         = Column(NVARCHAR(20),   nullable=False, server_default=text("N'APP'"))

    # CONTEXT
    CorrelationID  = Column(mssql.UNIQUEIDENTIFIER, nullable=False,
                            server_default=text("NEWSEQUENTIALID()"))
    RequestIP      = Column(String(45), nullable=True)
    UserAgent      = Column(NVARCHAR(400), nullable=True)
    AppVersion     = Column(NVARCHAR(50),  nullable=True)
    Success        = Column(Boolean, nullable=False, server_default=text("1"))
    ErrorMessage   = Column(NVARCHAR(1000), nullable=True)

    # WHEN
    At             = Column(DateTime, nullable=False, server_default=text("SYSUTCDATETIME()"))

    # __table_args__ = (
    #     # Helpful indexes (match your DDL)
    #     Index("IX_Audit_Entity",  "TableName", "EntityPK", "At", mssql_with={"SORT_IN_TEMPDB": "OFF"}, mssql_include=[]),
    #     Index("IX_Audit_User",    "UserID",    "At"),
    #     Index("IX_Audit_Corr",    "CorrelationID"),
    #     # Status quick index with INCLUDE columns
    #     Index("IX_Audit_StatusQuick", "TableName", "Action", "At",
    #           mssql_include=["EntityPK", "PreviousValue", "NewValue", "Source", "Username"]),
    # )




# Main MCSI that Api will take data from
class RTOS_MCSI(Base):
    __tablename__ = 'TB_WH_B2B_SO'

    ID = Column('Id', String(20), key = 'ID')
    SalesOffice = Column(String(100))
    SalesGroup = Column(String(100))
    SoldToParty = Column('SoldToPartyName', String(100), key = 'SoldToParty')
    Payer = Column('PayerName', String(100), key='Payer')
    ProductHierarchy1 = Column(String(100))
    ProductHierarchy2 = Column(String(100))
    Article = Column(String(100))
    BillingDocument = Column(String(100))
    Brand = Column('BrandName', String(100), key= 'Brand')
    DocumentDate = Column('Date', Date, key='DocumentDate')  # Storing dates
    GrInvSls = Column('InvoicedGrossSales', Float, key='GrInvSls')     # Using float for numeric fields
    ProdDisc = Column('ProductDiscount', Float, key='ProdDisc')
    RetnValue = Column('ReturnsValue', Float, key='RetnValue')
    ReturnQty = Column(Integer)
    CredMemos = Column('CreditMemos', Float, key='CredMemos')
    Net = Column('NetValue', Float, key='Net')
    GrossSale = Column('GrossSalesQty', Float, key='GrossSale')
    CreatedAt = Column(Date)
    
    # Define the composite primary key constraint
    __table_args__ = (
        PrimaryKeyConstraint('Article', 'BillingDocument', 'CreatedAt'),
    )

# ----SELL-Related----
# My sell-in table that will be populated by API (above data will come here 4 times a day) 
class SP_MCSI_SellIn(Base):
    __tablename__ = 'SP_MCSI_SellIn'

    ID = Column('Id', String(20), key='ID')
    SalesOffice = Column(String(100))
    SalesGroup = Column(String(100))
    SoldToParty = Column('SoldToPartyName', String(100), key='SoldToParty')
    Payer = Column('PayerName', String(100), key='Payer')
    ProductHierarchy1 = Column(String(100))
    ProductHierarchy2 = Column(String(100))
    Article = Column(String(100))
    BillingDocument = Column(String(100))
    Brand = Column('BrandName', String(100), key='Brand')
    DocumentDate = Column('Date', Date, key='DocumentDate')
    GrInvSls = Column('InvoicedGrossSales', Float, key='GrInvSls')
    ProdDisc = Column('ProductDiscount', Float, key='ProdDisc')
    RetnValue = Column('ReturnsValue', Float, key='RetnValue')
    ReturnQty = Column(Integer)
    CredMemos = Column('CreditMemos', Float, key='CredMemos')
    Net = Column('NetValue', Float, key='Net')
    GrossSale = Column('GrossSalesQty', Float, key='GrossSale')
    CreatedAt = Column(Date)
    CapturedAt = Column(DateTime, default=func.now())  # Capture timestamp

    __table_args__ = (
        PrimaryKeyConstraint('Article', 'BillingDocument', 'CreatedAt'),
    )   

class SP_SellInFilters(Base):
    __tablename__ = 'SP_SellInFilters'

    FilterID = Column(Integer, primary_key=True, autoincrement=True)
    UserID = Column(String(50))
    FilterName = Column(String(100))
    FieldName = Column(String(100))           # e.g., "SalesGroup"
    Operator = Column(String(10))             # '=', 'IN', 'NOT IN', etc.
    FieldValues = Column(Text)                # JSON-encoded list of values
    CreatedAt = Column(DateTime, default=func.getdate())  # For SQL Server
    IsActive = Column(Boolean, default=True)

# User UI for filter and columns at Sell IN page
class UserGridPrefs(Base):
    __tablename__ = 'SP_UserGridPrefs'
    PrefID         = Column(Integer, primary_key=True, autoincrement=True)
    UserID         = Column(Integer, nullable=False)
    PageKey        = Column(String(100), nullable=False)       # e.g. 'sellin_captures'
    VisibleColumns = Column(Text)                              # JSON array of column keys
    HiddenFilters  = Column(Text)                              # JSON array of column keys
    PerPage        = Column(Integer, default=50)
    CreatedAt      = Column(DateTime, default=datetime.utcnow)
    UpdatedAt      = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
#------END


class SP_Customer(Base):
    __tablename__ = "SP_Customer"
    CustomerID = Column(Integer, primary_key=True)
    CustCode   = Column(String(50), unique=True, nullable=False)
    CustName   = Column(Unicode(255), nullable=False)
    LevelType  = Column(String(10), nullable=False)  # 'HO'/'Branch'
    ParentCustID = Column(Integer, ForeignKey("SP_Customer.CustomerID"))

    # New FK to status (the default constraint is in DB; ORM just reflects)
    StatusID     = Column(Integer, ForeignKey("SP_Status.StatusID"), nullable=False, server_default=text("1"))
    
    StatusDate   = Column(Date)

    # Relationship to status dictionary
    status = relationship("SP_Status", back_populates="customers")
    
class SP_SKU(Base):
    __tablename__ = "SP_SKU"
    SKU_ID      = Column(Integer, primary_key=True)
    ArticleCode = Column(String(100), unique=True, nullable=False) # Company Article Name (TEXT)
    Description = Column(String(255))
    Brand       = Column(String(100))
    CategoryMappingID   = Column(Integer, ForeignKey("SP_CategoriesMappingMain.ID"), nullable=True)

    __table_args__ = (
        Index("IX_SP_SKU_Brand_CategoryMappingID", "Brand", "CategoryMappingID"),
    )

class SP_Customer_SKU_Map(Base):
    __tablename__ = "SP_Customer_SKU_Map"
    MapID      = Column(Integer, primary_key=True)
    SKU_ID     = Column(Integer, ForeignKey("SP_SKU.SKU_ID"), nullable=False)
    CustomerID = Column(Integer, ForeignKey("SP_Customer.CustomerID"), nullable=False)
    CustSKUCode= Column(String(100), nullable=False) #Customer's Article name (TEXT)
    
    sku      = relationship("SP_SKU", backref=backref("customer_links", cascade="all, delete"))
    customer = relationship("SP_Customer", backref=backref("sku_links", cascade="all, delete"))

    __table_args__ = (
        UniqueConstraint("CustomerID", "SKU_ID", name="UQ_Cust_SKU"),
        Index("IX_CustSKUCode", "CustomerID", "CustSKUCode"),
    )

class SP_CategoriesMappingMain( Base):
    __tablename__ = "SP_CategoriesMappingMain"
    
    ID = Column(Integer, primary_key=True, autoincrement=True)
    Brand = Column(String(100), nullable=False) #TODO
    CatCode = Column(String(50), nullable=False)
    CatName = Column(String(100), nullable=False)
    CatDesc = Column('CatDesc', String(255), key = 'CATDesc')
    SubCat = Column(String(100))
    UpdatedBy = Column(String(50))
    UpdatedAt = Column(DateTime, default=func.now())



# ---- FOR sell-out ----
class SP_MCSI_SellOut(Base):
    __tablename__ = "SP_MCSI_SellOut"
    UploadID    = Column(Integer, ForeignKey("SP_SellOutUploads.UploadID"), primary_key=True)
    SKU_ID      = Column(Integer, ForeignKey("SP_SKU.SKU_ID"), primary_key=True)
    RowNumber   = Column(Integer, primary_key=True)
    DocumentDate= Column(Date, nullable=False)
    CustSKUCode = Column(String(100))
    SellOutQty  = Column(Float, nullable=False)
    ReportedSOH = Column(Float)
    IsActive    = Column(Boolean, nullable=False, default=True)
    
    SKU = relationship("SP_SKU", lazy="joined", primaryjoin="SP_MCSI_SellOut.SKU_ID==SP_SKU.SKU_ID")

class SP_SellOutUploadAudit(Base):
    __tablename__ = "SP_SellOutUploadAudit"
    AuditID            = Column(Integer, primary_key=True, autoincrement=True)
    Action             = Column(String(30), nullable=False)      # 'UPLOAD'/'REPLACE'
    CustomerID         = Column(Integer, nullable=False)
    LevelType          = Column(String(10), nullable=False)
    UploadType         = Column(String(15), nullable=False)
    Brand              = Column(String(100))
    PeriodStart        = Column(Date)
    PeriodEnd          = Column(Date)
    NewUploadID        = Column(Integer)
    SupersededUploadIDs= Column(String(4000))
    DeactivatedRows    = Column(Integer, default=0)
    InsertedRows       = Column(Integer, default=0)
    SourceFileName     = Column(String(255))
    SourceFileHash     = Column(String(64))
    PerformedBy        = Column(String(100))
    PerformedAt        = Column(DateTime, default=datetime.utcnow)

class SP_SellOutUploads(Base):
    __tablename__ = "SP_SellOutUploads"
    UploadID     = Column(Integer, primary_key=True, autoincrement=True)
    CustomerID   = Column(Integer, ForeignKey("SP_Customer.CustomerID"), nullable=False)
    LevelType    = Column(String(10), nullable=False)          # 'HO'/'Branch'
    UploadType   = Column(String(15), nullable=False)          # 'Transactional'/'Accumulative'
    Brand        = Column(String(100))
    DocumentDate = Column(Date, nullable=False)                # legacy; kept for compatibility
    PeriodStart  = Column(Date)
    PeriodEnd    = Column(Date)
    Status       = Column(String(20), default="Draft")
    CreatedBy    = Column(String(100))
    CreatedAt    = Column(DateTime, default=func.now())
    ApprovedBy   = Column(Integer, ForeignKey("SP_Users.UserID")) 
    ApprovedAt   = Column(DateTime)
    SupersededByUploadID = Column(Integer)
    SourceFileName = Column(String(255))
    SourceFileHash = Column(String(64)) 
    Notes          = Column(String(500))
    
    HasPotentialNegatives = Column(Boolean, nullable=False, server_default=text("0"))
    NegPreviewComputedAt  = Column(DateTime(timezone=False))
    
    Approver = relationship(
        "SP_Users",
        primaryjoin="SP_SellOutUploads.ApprovedBy==SP_Users.UserID",
        lazy="joined",
        viewonly=True
    )
    
    __table_args__ = (
        # filtered unique index (SQL Server)
        Index(
            "UX_SO_Uploads_FileHash",
            "SourceFileHash",
            unique=True,
            mssql_where=(SourceFileHash.isnot(None))
        ),
    )
    
# For any attachments 
class SP_SellOutUploadFile(Base):
    __tablename__ = "SP_SellOutUploadFile"
    FileID       = Column(Integer, primary_key=True, autoincrement=True)
    UploadID     = Column(Integer, ForeignKey("SP_SellOutUploads.UploadID"), index=True, nullable=False)
    ServerID     = Column(String(500), nullable=False)  # e.g. "acme-retail/abc123__invoice.pdf"
    OriginalName = Column(String(255))
    MimeType     = Column(String(120))
    SizeBytes    = Column(Integer)
    UploadedBy   = Column(String(100))
    UploadedAt   = Column(DateTime, default=datetime.utcnow, nullable=False)

# ------- END
# Negative sellout to presist for audit
class SP_SellOutNegPreview(Base):
    __tablename__ = "SP_SellOutNegPreview"
    __table_args__ = (
        PrimaryKeyConstraint("UploadID", "RowNumber", name="PK_SP_SellOutNegPreview"),
        {"schema": "dbo"},
    )

    UploadID             = Column(Integer, ForeignKey("SP_SellOutUploads.UploadID", ondelete="CASCADE"), nullable=False)
    RowNumber            = Column(Integer, nullable=False)
    SKU_ID               = Column(Integer, nullable=False)
    DocumentDate         = Column(Date,    nullable=False)
    SellOutQty           = Column(Numeric(18, 3), nullable=False)
    AvailableBefore      = Column(Numeric(18, 3))
    CumulativeFromUpload = Column(Numeric(18, 3))
    ResultingBalance     = Column(Numeric(18, 3))
    IsNegative           = Column(Boolean, nullable=False, default=False)
    ComputedAt           = Column(DateTime(timezone=False), server_default=text("SYSUTCDATETIME()"), nullable=False)

    # Optional: relationship back to header
    # upload = relationship("SP_SellOutUploads", backref="NegPreviewLines")

# FOR Approval process of sell-out
class SP_SellOutApproval(Base):
    __tablename__ = "SP_SellOutApproval"
    ApprovalID   = Column(Integer, primary_key=True, autoincrement=True)
    UploadID     = Column(Integer, ForeignKey("SP_SellOutUploads.UploadID"), index=True, nullable=False)
    Action       = Column(String(20), nullable=False)  # 'SUBMIT' | 'APPROVE' | 'REJECT'
    Comment      = Column(String(1000))
    Actor        = Column(Integer, nullable=False)
    ActedAt      = Column(DateTime, default=func.now(), nullable=False)

# For approval /SOH chnages TODO PUSH TO PROD
class SP_InventoryLedger(Base):
    __tablename__ = "SP_InventoryLedger"

    LedgerID       = Column(Integer, primary_key=True, autoincrement=True)
    CustomerID     = Column(Integer, nullable=False)
    SKU_ID         = Column(Integer, nullable=False)
    DocDate        = Column(Date,    nullable=False)
    MovementType   = Column(String(15), nullable=False)   # 'INIT'|'SELLIN'|'SELLOUT'|'ADJUST'|...
    MovementSubType= Column(String(20))                   # optional: e.g., reason code
    Qty            = Column(Integer,  nullable=False)       # SELLOUT posted as NEGATIVE
    UploadID       = Column(Integer, ForeignKey("SP_SellOutUploads.UploadID"))  # optional ref
    RefTable       = Column(String(40))                   # 'SP_SOH_Uploads' / 'SP_SellOutUploads' / 'TB_WH_B2B_SO'
    RefID          = Column(String(64))                   # SOHUploadID / UploadID / BillingDocument / etc.
    IdempotencyKey = Column(String(100))                  # to avoid duplicates on retries
    CreatedAt      = Column(DateTime, default=func.now(), nullable=False)

    __table_args__ = (
        # “one event once” guard (UploadID nullable allowed via compound uniqueness)
        Index("UX_InvLedger_Idempotent",
              "CustomerID", "SKU_ID", "DocDate", "MovementType", "IdempotencyKey", "UploadID",
              unique=True),
        # common rollups
        Index("IX_InvLedger_ByCustSkuDate",
              "CustomerID", "SKU_ID", "DocDate",
              mssql_include=["Qty", "MovementType", "MovementSubType"]),
    )


# =========================
# Customer Format Mapping
# =========================

class SP_CustomerUploadProfile(Base):
    __tablename__ = "SP_CustomerUploadProfile"

    ProfileID    = Column(Integer, primary_key=True, autoincrement=True)
    CustomerID   = Column(Integer, ForeignKey("SP_Customer.CustomerID"), nullable=False)
    ProfileName  = Column(String(200), nullable=False)

    IsActive     = Column(Boolean, nullable=False, server_default=text("1"))
    IsDefault    = Column(Boolean, nullable=False, server_default=text("0"))

    SheetName       = Column(String(200))
    HeaderRowIndex  = Column(Integer)  # 1-based
    DataStartRow    = Column(Integer)  # 1-based
    Notes           = Column(String(500))

    CreatedAt    = Column(DateTime, server_default=text("SYSUTCDATETIME()"), nullable=False)
    CreatedBy    = Column(String(100), nullable=False)
    UpdatedAt    = Column(DateTime)
    UpdatedBy    = Column(String(100))

    # relationships
    detail  = relationship("SP_CustomerUploadProfileDetail",
                           back_populates="profile",
                           cascade="all, delete-orphan",
                           uselist=False)
    stagings = relationship("SP_SellOut_Staging", back_populates="profile")

    __table_args__ = (
        UniqueConstraint("CustomerID", "ProfileName", name="UQ_SP_CUP_Customer_ProfileName"),
        # one default per customer (when active)
        Index(
            "UX_SP_CUP_DefaultPerCustomer",
            "CustomerID",
            unique=True,
            mssql_where=text("IsDefault = 1 AND IsActive = 1")
        ),
        Index("IX_SP_CUP_Customer_IsActive", "CustomerID", "IsActive"),
    )


class SP_CustomerUploadProfileDetail(Base):
    __tablename__ = "SP_CustomerUploadProfileDetail"

    ProfileID   = Column(Integer,
                         ForeignKey("SP_CustomerUploadProfile.ProfileID", ondelete="CASCADE"),
                         primary_key=True)
    MappingJSON = Column(Text, nullable=False)  # NVARCHAR(MAX) on SQL Server

    profile = relationship("SP_CustomerUploadProfile", back_populates="detail")


class SP_SellOut_Staging(Base):
    __tablename__ = "SP_SellOut_Staging"

    StagingID = Column(BigInteger, primary_key=True, autoincrement=True)

    UploadID  = Column(Integer, ForeignKey("SP_SellOutUploads.UploadID"), nullable=False)
    ProfileID = Column(Integer, ForeignKey("SP_CustomerUploadProfile.ProfileID"))
    CustomerID = Column(Integer, ForeignKey("SP_Customer.CustomerID"), nullable=False)

    SourceFileName = Column(String(260))
    SourceSheet    = Column(String(200))
    SourceRow      = Column(Integer)

    Date        = Column("Date", Date)     # keep column name exactly "Date"
    Article     = Column(String(50))
    CustomerSKU = Column(String(100))
    Qty         = Column(Numeric(18, 3))
    Store       = Column(String(100))
    Region      = Column(String(100))
    InvoiceNo   = Column(String(100))
    Brand       = Column(String(100))
    Site        = Column(String(100))

    ValidationErr = Column(String(500))
    ErrorsJSON    = Column(Text)           # NVARCHAR(MAX)
    RowHash       = Column(LargeBinary)    # e.g., MD5/SHA-256 bytes

    CreatedAt = Column(DateTime, server_default=text("SYSUTCDATETIME()"), nullable=False)

    # relationships
    profile = relationship("SP_CustomerUploadProfile", back_populates="stagings")
    # Optionally: customer/upload relationships if you define classes for them here
    # customer = relationship("SP_Customer")
    # upload   = relationship("SP_SellOutUploads")

    __table_args__ = (
        Index("IX_SP_SO_Staging_Upload", "UploadID"),
        Index("IX_SP_SO_Staging_Customer", "CustomerID"),
        Index("IX_SP_SO_Staging_CoreFields", "Date", "Article", "CustomerSKU"),
        # prevent accidental dupes within the same upload (same sheet+row)
        Index(
            "UX_SP_SO_Staging_NoDupWithinUpload",
            "UploadID", "SourceSheet", "SourceRow",
            unique=True,
            mssql_where=text("SourceRow IS NOT NULL")
        ),
        Index("IX_SP_SO_Staging_ValidationErr", "ValidationErr", mssql_include=["UploadID"]),
    )


# --- NEW: SOH header & detail -------------------------------------------------

class SP_SOH_Uploads(Base):
    __tablename__ = "SP_SOH_Uploads"

    SOHUploadID          = Column(Integer, primary_key=True, autoincrement=True)
    CustomerID           = Column(Integer, ForeignKey("SP_Customer.CustomerID"), nullable=False)
    SnapshotType         = Column(String(15), nullable=False, default="Initial")  # 'Initial'/'Snapshot'
    Brand                = Column(String(100))  # optional tag
    Date                 = Column(Date, nullable=False)
    # PeriodEnd            = Column(Date, nullable=False)
    Status               = Column(String(20), default="Draft")
    CreatedBy            = Column(String(100))
    CreatedAt            = Column(DateTime, default=datetime.utcnow)
    SupersededByUploadID = Column(Integer)  # informational pointer
    SourceFileName       = Column(String(255))
    SourceFileHash       = Column(String(64))
    Notes                = Column(String(500))
    
    __table_args__ = (
        # Partial unique index equivalent:
        Index(
            "UX_SOH_Customer_FileHash",
            "CustomerID",
            "SourceFileHash",
            unique=True,
            mssql_where=SourceFileHash.isnot(None)   # <-- important for SQL Server
        ),
    )

class SP_SOH_Detail(Base): #TODO PUSH TO PRD
    __tablename__ = "SP_SOH_Detail"

    SOHUploadID = Column(Integer, ForeignKey("SP_SOH_Uploads.SOHUploadID", ondelete="CASCADE"), primary_key=True)
    SKU_ID      = Column(Integer, ForeignKey("SP_SKU.SKU_ID"), primary_key=True)
    RowNumber   = Column(Integer, primary_key=True)
    SOHDate     = Column(Date, nullable=False)
    SOHQty      = Column(Integer, nullable=False)
    IsActive    = Column(Boolean, nullable=False, default=True)
    
    __table_args__ = (
        # Fast reads by date/SKU and “is it the active one?”
        Index("IX_SOH_Detail_ByDateSKU", "SOHDate", "SKU_ID", mssql_include=["SOHUploadID", "SOHQty", "IsActive"]),
        # Often you’ll select active rows only
        Index("IX_SOH_Detail_Active", "SOHDate", "SKU_ID", "IsActive"),
    )



# Base.metadata.create_all(engine)
# 