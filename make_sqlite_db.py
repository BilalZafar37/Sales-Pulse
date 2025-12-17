# make_sqlite_db.py
from sqlalchemy import create_engine, String
from sqlalchemy.dialects import mssql

from models import Base   # this is your existing Base with all models

# 1) Make a SQLite engine
sqlite_engine = create_engine("sqlite:///salespulse_dev.sqlite", echo=False)

# 2) Patch MSSQL-only types so SQLite can handle them
for table in Base.metadata.tables.values():
    for col in table.c:
        # CorrelationID in SP_UserAudit is mssql.UNIQUEIDENTIFIER
        if isinstance(col.type, mssql.UNIQUEIDENTIFIER):
            # represent it as simple TEXT / VARCHAR(36) in SQLite
            col.type = String(36)
            # SQLite won't understand NEWSEQUENTIALID(), drop default for dev DB
            col.server_default = None

# 3) Now create all tables in SQLite
Base.metadata.create_all(sqlite_engine)

print("SQLite dev DB created: salespulse_dev.sqlite")
