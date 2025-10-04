# populate_db.py
import duckdb
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DUCKDB_PATH")
DB_DIR = os.path.dirname(DB_PATH)

# Create the database directory if it doesn't exist
if DB_DIR:
    os.makedirs(DB_DIR, exist_ok=True)

# Connect to DuckDB
con = duckdb.connect(database=DB_PATH, read_only=False)

# Drop table if it exists to make script re-runnable
con.execute("DROP TABLE IF EXISTS ray_jobs;")

# Create the table
con.execute("""
    CREATE TABLE ray_jobs (
        job_name TEXT PRIMARY KEY,
        status TEXT,
        logs TEXT,
        start_time TIMESTAMP,
        end_time TIMESTAMP
    );
""")

print(f"Database '{DB_PATH}' created and populated with sample data successfully.")

# Close connection
con.close()