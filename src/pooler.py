import os
import time
import requests
import sqlite3
import re
import json

# --- Configuration from Environment Variables ---
NAMESPACE = os.getenv("POD_NAMESPACE", "default")
API_BASE = os.getenv("KUBERAY_API_SERVER", "http://kuberay-apiserver-service.default.svc.cluster.local:8888")
DB_PATH = os.getenv("SQLITE_PATH", "/app/database/ray_jobs.db")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))

TERMINAL_STATES = {"SUCCEEDED", "FAILED", "STOPPED"}

def clean_raw_logs(raw_logs: str) -> str:
    """
    Cleans raw log output by first attempting to parse it as JSON (as Ray often returns),
    and then stripping all ANSI/Unicode terminal formatting codes for clean display.
    """
    # 1. Try to parse as JSON, as the Ray dashboard API often wraps logs this way.
    try:
        data = json.loads(raw_logs)
        log_content = data.get('logs', '')
        # Unescape characters like \\n and \\t
        log_content = log_content.encode().decode('unicode_escape')
    except (json.JSONDecodeError, TypeError):
        # If it's not JSON or not a string, process it as plain text.
        log_content = raw_logs if isinstance(raw_logs, str) else "Log content is not in a readable format."

    # 2. Use a regular expression to remove ANSI/Unicode control characters.
    # This pattern matches escape sequences for colors, box drawing, etc.
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])|[\u2500-\u257F]+')
    return ansi_escape.sub('', log_content)


class RayJobManager:
    """Manages fetching, processing, and cleaning up RayJobs."""
    def __init__(self, namespace, api_base):
        self.namespace = namespace
        self.api_base = api_base
        self._init_db()

    def _init_db(self):
        """Ensures the database and table exist and enables WAL mode."""
        print(f"Initializing database at {DB_PATH}...")
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            # CRITICAL: Enable WAL mode for concurrent read/write.
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ray_jobs (
                    job_name TEXT PRIMARY KEY,
                    status TEXT,
                    logs TEXT,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP
                )
            """)
        print("Database initialized successfully.")

    def get_all_jobs(self):
        """Fetches all RayJob resources from the Kubernetes API."""
        url = f"{self.api_base}/apis/ray.io/v1/namespaces/{self.namespace}/rayjobs"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json().get("items", [])
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching RayJobs: {e}")
            return []

    def get_job_logs(self, job_name, job_details):
        """Fetches and cleans logs for a given job from the Ray Dashboard."""
        dashboard_url = job_details.get('dashboardURL')
        job_id = job_details.get('jobId')
        
        if not all([dashboard_url, job_id]):
            print(f"⚠️ Missing dashboardURL or jobId for {job_name}. Cannot fetch logs.")
            return "Log data unavailable: Missing dashboard URL or Job ID."

        url = f"http://{dashboard_url}/api/jobs/{job_id}/logs"
        try:
            response = requests.get(url, timeout=15)
            if response.status_code == 200:
                # The cleaning function is now robust
                return clean_raw_logs(response.text)
            else:
                print(f"❌ Failed to fetch logs for {job_name} (Status {response.status_code}) from {url}")
                return f"Failed to fetch logs. Status: {response.status_code}"
        except requests.exceptions.RequestException as e:
            print(f"❌ Exception while fetching logs for {job_name}: {e}")
            return f"Failed to fetch logs due to a network error: {e}"

    def delete_job(self, job_name):
        """Deletes a RayJob resource from the Kubernetes API."""
        url = f"{self.api_base}/apis/ray.io/v1/namespaces/{self.namespace}/rayjobs/{job_name}"
        try:
            response = requests.delete(url, timeout=10)
            if 200 <= response.status_code < 300:
                print(f"🗑️ Deleted job resource: {job_name}")
            else:
                print(f"❌ Failed to delete job {job_name}: {response.status_code} {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Exception during job deletion for {job_name}: {e}")

    def save_job_to_db(self, job_name, status, logs, start_time, end_time):
        """Saves or updates job information in the SQLite database correctly."""
        try:
            # The 'with' statement handles commit and close automatically.
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                # Ensure WAL mode is active for every connection.
                cursor.execute("PRAGMA journal_mode=WAL;")
                cursor.execute("""
                    INSERT OR REPLACE INTO ray_jobs (job_name, status, logs, start_time, end_time) 
                    VALUES (?, ?, ?, ?, ?)
                """, (job_name, status, logs, start_time, end_time))
        except Exception as e:
            print(f"❌ Database error for job {job_name}: {e}")

    def process_jobs(self):
        """The main processing loop."""
        print("\n--- Starting job processing cycle ---")
        jobs = self.get_all_jobs()
        if not jobs:
            print("No RayJobs found in the cluster.")
            return

        for job in jobs:
            job_name = job.get("metadata", {}).get("name")
            job_status_details = job.get("status", {})
            status = job_status_details.get("jobStatus", "UNKNOWN")
            
            if not job_name:
                continue

            print(f"Processing job: {job_name}, Status: {status}")

            start_time = job_status_details.get('startTime')
            end_time = job_status_details.get('endTime')
            
            if status in TERMINAL_STATES:
                print(f"📌 Job '{job_name}' is in terminal state: {status}. Fetching logs and cleaning up.")
                logs = self.get_job_logs(job_name, job_status_details)
                self.save_job_to_db(job_name, status, logs, start_time, end_time)
                self.delete_job(job_name)
            else:
                self.save_job_to_db(job_name, status, "Logs are available after job completion.", start_time, end_time)

if __name__ == "__main__":
    manager = RayJobManager(NAMESPACE, API_BASE)
    print(f"Starting RayJob poller in namespace '{NAMESPACE}'. Polling every {POLL_INTERVAL} seconds.")
    
    while True:
        try:
            manager.process_jobs()
        except Exception as e:
            print(f"🚨 An unexpected error occurred in the main loop: {e}")
        
        time.sleep(POLL_INTERVAL)

