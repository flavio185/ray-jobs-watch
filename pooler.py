import os
import time
import requests
import duckdb
from datetime import datetime

# --- Configuration from Environment Variables ---
# The namespace is injected by the Kubernetes Downward API
NAMESPACE = os.getenv("POD_NAMESPACE", "default")
# The API server for KubeRay
API_BASE = os.getenv("KUBERAY_API_SERVER", "http://kuberay-apiserver-service.default.svc.cluster.local:8888")
# The path to the database on the shared persistent volume
DB_PATH = os.getenv("DUCKDB_PATH", "/app/database/ray_jobs.db")
# Polling interval in seconds
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))

TERMINAL_STATES = {"SUCCEEDED", "FAILED", "STOPPED"}

class RayJobManager:
    """Manages fetching, processing, and cleaning up RayJobs."""
    def __init__(self, namespace, api_base):
        self.namespace = namespace
        self.api_base = api_base
        self._init_db()

    def _init_db(self):
        """Ensures the database and table exist."""
        print(f"Initializing database at {DB_PATH}...")
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = duckdb.connect(DB_PATH)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ray_jobs (
                job_name TEXT PRIMARY KEY,
                status TEXT,
                logs TEXT,
                start_time TIMESTAMP,
                end_time TIMESTAMP
            )
        """)
        conn.close()
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
        """Fetches logs for a given job from the Ray Dashboard."""
        dashboard_url = job_details.get('dashboardURL')
        job_id = job_details.get('jobId')
        
        if not all([dashboard_url, job_id]):
            print(f"⚠️ Missing dashboardURL or jobId for {job_name}. Cannot fetch logs.")
            return "Log data unavailable: Missing dashboard URL or Job ID."

        url = f"http://{dashboard_url}/api/jobs/{job_id}/logs"
        try:
            # Adding a timeout is crucial for network requests
            response = requests.get(url, timeout=15)
            if response.status_code == 200:
                # Clean logs for potential encoding issues
                return response.text.encode("utf-8", "surrogateescape").decode("utf-8", "ignore")
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

    def save_job_to_duckdb(self, job_name, status, logs, start_time, end_time):
        """Saves or updates job information in the DuckDB database."""
        try:
            with duckdb.connect(DB_PATH) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO ray_jobs (job_name, status, logs, start_time, end_time) 
                    VALUES (?, ?, ?, ?, ?)
                """, (job_name, status, logs, start_time, end_time))
            # print(f"✅ Saved job '{job_name}' with status '{status}' to DB.")
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
                self.save_job_to_duckdb(job_name, status, logs, start_time, end_time)
                self.delete_job(job_name)
            else: # PENDING, RUNNING, etc.
                self.save_job_to_duckdb(job_name, status, "Logs are available after job completion.", start_time, end_time)

if __name__ == "__main__":
    manager = RayJobManager(NAMESPACE, API_BASE)
    print(f"Starting RayJob poller in namespace '{NAMESPACE}'. Polling every {POLL_INTERVAL} seconds.")
    
    while True:
        try:
            manager.process_jobs()
        except Exception as e:
            print(f"🚨 An unexpected error occurred in the main loop: {e}")
        
        time.sleep(POLL_INTERVAL)
