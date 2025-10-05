import sqlite3
import os
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

# Load environment variables from .env file (for local development)
load_dotenv()

# --- Configuration ---
# This path should match the one used by your poller.py script.
DB_PATH = os.getenv("SQLITE_PATH", "database/ray_jobs.db")

# --- FastAPI App Initialization ---
app = FastAPI(title="Ray Jobs Dashboard")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# --- Database Connection (Corrected for Threading) ---
def get_db():
    """
    FastAPI dependency to get an SQLite DB connection safely.
    - Connects with check_same_thread=False, which is required for FastAPI.
    - Enables WAL for concurrent access.
    - Handles cases where the DB file doesn't exist yet.
    """
    conn = None
    try:
        # THE FIX: Add check_same_thread=False to allow usage across FastAPI's threads.
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        
        # Readers must also enable WAL mode to read from a WAL-enabled DB.
        conn.execute("PRAGMA journal_mode=WAL;")
        yield conn
    except sqlite3.OperationalError:
        print(f"WARNING: Database not found at {DB_PATH} or is not accessible. It will be created by the poller.")
        # If DB doesn't exist, yield None so endpoints can handle it gracefully.
        yield None
    finally:
        if conn:
            conn.close()

# --- Endpoints (Updated for Robustness) ---

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: sqlite3.Connection = Depends(get_db)):
    """
    Home page: Shows jobs with status 'RUNNING', 'PENDING' or 'UNKNOWN'.
    """
    active_jobs = []
    # Check if the database connection was successful
    if db:
        query = "SELECT job_name, status, logs, start_time, end_time FROM ray_jobs WHERE status IN ('RUNNING', 'PENDING', 'UNKNOWN') ORDER BY start_time DESC;"
        active_jobs = db.execute(query).fetchall()
        
    return templates.TemplateResponse(
        "index.html", {"request": request, "jobs": active_jobs}
    )

@app.get("/completed", response_class=HTMLResponse)
async def completed_jobs(request: Request, db: sqlite3.Connection = Depends(get_db)):
    """
    Completed jobs page: Shows jobs with status NOT in 'RUNNING', 'PENDING' or 'UNKNOWN'.
    """
    jobs = []
    if db:
        query = "SELECT job_name, status, logs, start_time, end_time FROM ray_jobs WHERE status NOT IN ('RUNNING', 'PENDING', 'UNKNOWN') ORDER BY end_time DESC;"
        jobs = db.execute(query).fetchall()
        
    return templates.TemplateResponse(
        "completed.html", {"request": request, "jobs": jobs}
    )

@app.get("/jobs/{job_name}", response_class=HTMLResponse)
async def job_details(job_name: str, request: Request, db: sqlite3.Connection = Depends(get_db)):
    """
    Job details page: Shows detailed information for a single job.
    """
    # If DB is unavailable, raise a 503 Service Unavailable error
    if not db:
        raise HTTPException(status_code=503, detail="Database is not available yet. Please try again shortly.")
        
    query = "SELECT job_name, status, logs, start_time, end_time FROM ray_jobs WHERE job_name = ?;"
    job = db.execute(query, [job_name]).fetchone()

    if not job:
        raise HTTPException(status_code=440, detail=f"Job '{job_name}' not found")
        
    return templates.TemplateResponse(
        "job_details.html", {"request": request, "job": job}
    )

