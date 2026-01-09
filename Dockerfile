# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies for psycopg2 and other packages
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Environment variable to ensure output is sent straight to terminal
ENV PYTHONUNBUFFERED=1

# Run main.py when the container launches
# Production Entrypoint with Gunicorn
CMD ["gunicorn", "-k", "gthread", "-w", "1", "--threads", "8", "-b", "0.0.0.0:5000", "--access-logfile", "-", "--error-logfile", "-", "main:app"]
