from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import pandas as pd
import duckdb as db

# 1. Connecting to MSSQL
connection_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=XPERIA\SQLEXPRESS;"
    "DATABASE=TravelAgencyDB;"
    "UID=sa;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)
conn_url = "mssql+pyodbc:///?odbc_connect=" + quote_plus(connection_str)
engine = create_engine(conn_url)

# 2. Extract data from MSSQL and store in df.
with engine.connect() as conn:
    df = pd.read_sql_query(text("SELECT * FROM TourBookings"), conn)

# 3. Convert the data to parquet.
df.to_parquet("tour_bookings.parquet", engine="pyarrow", index=False)

# 4. Load the data into DUCK.
con = db.connect("travelduck.duckdb")
con.execute("CREATE TABLE IF NOT EXISTS tour_bookings AS SELECT * FROM read_parquet('tour_bookings.parquet')")

# 5. Run queries to see the execution.
print(con.execute("SELECT COUNT(*) FROM tour_bookings").fetchall())
print(con.execute("SELECT * FROM tour_bookings LIMIT 5").fetchdf())
