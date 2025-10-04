.
├── database/
│   └── ray_jobs.db        # DuckDB database file will be created here
├── static/
│   └── styles.css         # Basic CSS for styling
├── templates/
│   ├── base.html          # Base template with navigation
│   ├── completed.html     # Page for completed jobs
│   ├── index.html         # Home page for active jobs
│   └── job_details.html   # Page for a single job's details
├── .env                   # Environment variables file
├── main.py                # The main FastAPI application
├── populate_db.py         # A script to create and populate the DB with sample data
└── requirements.txt       # Python dependencies


```

#### Build and Push the Image

Now, build and push this image to your container registry.

```bash
# Replace 'your-registry' and 'your-repo' with your actual registry and repository name
export IMAGE_NAME="flavio185/fastapi-duckdb-app:0.1.5"

# Build the image
podman build -t $IMAGE_NAME .

# Push the image
podman push $IMAGE_NAME
```
