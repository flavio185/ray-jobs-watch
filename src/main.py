# main.py
import duckdb
import os
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
DB_PATH = os.getenv("DUCKDB_PATH", "database/ray_jobs.db")

# --- FastAPI App Initialization ---
app = FastAPI(title="Ray Jobs Dashboard")

# Mount static files (for CSS)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Setup Jinja2 templates
templates = Jinja2Templates(directory="templates")


# --- Database Connection ---
def get_db():
    """
    FastAPI dependency to get a DB connection.
    Yields a connection for a single request and ensures it's closed.
    """
    try:
        # read_only=True is safer for web apps that only display data
        conn = duckdb.connect(database=DB_PATH, read_only=True)
        yield conn
    finally:
        if 'conn' in locals() and conn:
            conn.close()

# --- Endpoints ---

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: duckdb.DuckDBPyConnection = Depends(get_db)):
    """
    Home page: Shows jobs with status 'RUNNING', 'PENDING' or 'UNKNOWN'.
    """
    query = "SELECT job_name, status, logs, start_time, end_time FROM ray_jobs WHERE status IN ('RUNNING', 'PENDING', 'UNKNOWN') ORDER BY start_time DESC;"
    active_jobs = db.execute(query).fetchall()
    return templates.TemplateResponse(
        "index.html", {"request": request, "jobs": active_jobs}
    )

@app.get("/completed", response_class=HTMLResponse)
async def completed_jobs(request: Request, db: duckdb.DuckDBPyConnection = Depends(get_db)):
    """
    Completed jobs page: Shows jobs with status NOT in 'RUNNING', 'PENDING' or 'UNKNOWN'.
    """
    query = "SELECT job_name, status, logs, start_time, end_time FROM ray_jobs WHERE status NOT IN ('RUNNING', 'PENDING', 'UNKNOWN') ORDER BY end_time DESC;"
    jobs = db.execute(query).fetchall()
    return templates.TemplateResponse(
        "completed.html", {"request": request, "jobs": jobs}
    )

@app.get("/jobs/{job_name}", response_class=HTMLResponse)
async def job_details(job_name: str, request: Request, db: duckdb.DuckDBPyConnection = Depends(get_db)):
    """
    Job details page: Shows detailed information for a single job.
    """
    # Use parameterized query to prevent SQL injection
    query = "SELECT job_name, status, logs, start_time, end_time FROM ray_jobs WHERE job_name = ?;"
    job = db.execute(query, [job_name]).fetchone()

    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found")
        
    return templates.TemplateResponse(
        "job_details.html", {"request": request, "job": job}
    )

# To run the app, use the command: uvicorn main:app --reload