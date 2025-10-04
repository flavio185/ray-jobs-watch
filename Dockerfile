# Dockerfile
# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY src/ .

# The user and group 'appuser' will be created with UID/GID 1001
RUN groupadd -r appuser && useradd --no-log-init -r -g appuser appuser

# Change ownership of the app directory to the new user
# The database directory will be mounted later, so we just create a mount point
RUN chown -R appuser:appuser /app && mkdir -p /app/database && chown -R appuser:appuser /app/database

# Switch to the non-root user
USER appuser

# Command to run the application
# Use 0.0.0.0 to make it accessible from outside the container
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
