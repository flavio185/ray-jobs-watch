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

# Sample data
now = datetime.now()
jobs = [
    (
        'job_data_pipeline_alpha', 
        'RUNNING', 
        'INFO: Starting data ingestion...\nINFO: Processing batch 1/100...\nINFO: Current memory usage: 256MB',
        now - timedelta(minutes=10), 
        None
    ),
    (
        'job_model_training_beta', 
        'PENDING', 
        'INFO: Awaiting resource allocation. GPU required.',
        now - timedelta(minutes=2), 
        None
    ),
    (
        'job_report_generation_20251004', 
        'COMPLETED', 
        'INFO: Starting report generation.\nINFO: Fetching sales data.\nINFO: Rendering PDF.\nSUCCESS: Report generated successfully at /reports/20251004.pdf',
        now - timedelta(hours=2), 
        now - timedelta(hours=1, minutes=50)
    ),
    (
        'job_db_cleanup_weekly', 
        'FAILED', 
        'INFO: Starting weekly cleanup job.\nERROR: Could not connect to primary database cluster.\nTRACEBACK: ...\nFATAL: Job failed after 3 retries.',
        now - timedelta(days=1), 
        now - timedelta(days=1, hours=23, minutes=55)
    ),
    (
        'job_user_sync_gamma', 
        'COMPLETED', 
        'INFO: Syncing user profiles from LDAP.\nINFO: 500 users processed.\nSUCCESS: Sync complete.',
        now - timedelta(hours=5), 
        now - timedelta(hours=4, minutes=45)
    )
]

# Insert data
con.executemany("INSERT INTO ray_jobs VALUES (?, ?, ?, ?, ?)", jobs)

print(f"Database '{DB_PATH}' created and populated with sample data successfully.")

# Close connection
con.close()