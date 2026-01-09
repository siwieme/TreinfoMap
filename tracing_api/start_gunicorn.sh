#!/bin/bash
export FLASK_APP=app.py
# Use 4 workers, bind to port 8000
echo "Starting Tracing API with Gunicorn..."
gunicorn -w 4 -b 0.0.0.0:8000 app:app
