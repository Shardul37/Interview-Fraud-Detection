#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

echo "Running secrets_loader.py to fetch environment variables..."
# Ensure PYTHONUNBUFFERED for secrets_loader.py output too
python -u secrets_loader.py

echo "Starting main application..."
# Execute the command passed as arguments to the entrypoint (CMD in Dockerfile)
exec "$@"