# copy_to_sqlite.py

from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from models import (
    Base,
    db_connection,
    SP_CategoriesMappingMain,
    # RTOS_Brands,
    SP_CategoriesMappingMain,
    SP_Users,
    SP_Customer,
    SP_SKU,
    SP_Customer_SKU_Map,
    SP_Status,
    SP_SellOutUploads,
    SP_SellOutUploadFile,
    SP_SellOutUploadAudit,
    SP_SellOut_Staging,
    SP_SellOutNegPreview,
    SP_SellOutApproval,
    SP_SOH_Uploads,
    SP_SOH_Detail,
    SP_InventoryLedger,
    # SP_MCSI_SellIn,
    SP_MCSI_SellOut,
    SP_SellInFilters,
    UserGridPrefs,
    SP_CustomerStatusTag,
    SP_GlobalConfig,
    SP_UserBrand,
    SP_UserCategory,
    SP_UserCustomer,
    # SP_UserAudit,
    SP_CustomerUploadProfile,
    SP_CustomerUploadProfileDetail,
)

from config import Config

# ---------- build src (SQL Server) ----------
Config.DB_BACKEND = "mssql"
src_engine = db_connection()
SrcSession = sessionmaker(bind=src_engine)
src = SrcSession()

# ---------- build dst (SQLite) ----------
Config.DB_BACKEND = "sqlite"
Config.SQLITE_PATH = "salespulse_dev.sqlite"
dst_engine = db_connection()
DstSession = sessionmaker(bind=dst_engine)
dst = DstSession()


def copy_table(src_session, dst_session, model_cls):
    """Copy all rows from model_cls between engines, with small fixes for SQLite."""
    print(f"=== Copying {model_cls.__tablename__} ===")

    rows = src_session.query(model_cls).all()
    copied = 0

    for row in rows:
        data = {
            col.name: getattr(row, col.name)
            for col in model_cls.__table__.columns
        }

        # --- SPECIAL FIXES FOR SQLITE CONSTRAINTS ---

        # 1) SP_CategoriesMappingMain.Brand is NOT NULL in SQLite,
        #    but some rows in SQL Server have Brand = NULL.
        if model_cls is SP_CategoriesMappingMain:
            if data.get("Brand") is None:
                data["Brand"] = "UNKNOWN"   # or "" if you prefer

        # You can add other per-table fixes here later if needed.

        try:
            dst_session.add(model_cls(**data))
            copied += 1
        except IntegrityError as e:
            dst_session.rollback()
            print(f"  -> Skipped row due to IntegrityError: {e}")

    dst_session.commit()
    print(f"{model_cls.__tablename__}: DONE, total {copied} rows.")


def main():
    # Copy in an order that respects FK dependencies as much as possible
    # copy_table(src, dst, RTOS_Brands)
    # copy_table(src, dst, SP_CategoriesMappingMain)
    # copy_table(src, dst, SP_Status)
    # copy_table(src, dst, SP_Users)
    # copy_table(src, dst, SP_Customer)
    # copy_table(src, dst, SP_SKU)
    # copy_table(src, dst, SP_Customer_SKU_Map)
    # copy_table(src, dst, SP_UserBrand)
    # copy_table(src, dst, SP_UserCategory)
    # copy_table(src, dst, SP_UserCustomer)
    # copy_table(src, dst, SP_GlobalConfig)
    # copy_table(src, dst, SP_CustomerStatusTag)
    # # copy_table(src, dst, SP_UserAudit)
    # copy_table(src, dst, SP_SellInFilters)
    # copy_table(src, dst, UserGridPrefs)

    # copy_table(src, dst, SP_MCSI_SellIn)
    copy_table(src, dst, SP_MCSI_SellOut)

    copy_table(src, dst, SP_SellOutUploads)
    copy_table(src, dst, SP_SellOutUploadFile)
    copy_table(src, dst, SP_SellOutUploadAudit)
    copy_table(src, dst, SP_SellOut_Staging)
    copy_table(src, dst, SP_SellOutNegPreview)
    copy_table(src, dst, SP_SellOutApproval)

    copy_table(src, dst, SP_SOH_Uploads)
    copy_table(src, dst, SP_SOH_Detail)
    copy_table(src, dst, SP_InventoryLedger)

    print("=== ALL DONE ===")


if __name__ == "__main__":
    main()
