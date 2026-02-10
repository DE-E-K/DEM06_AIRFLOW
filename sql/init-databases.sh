#!/bin/bash
# Initialize databases for Airflow and Analytics

set -e

echo "Creating airflow database..."
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE airflow;" || true

echo "Airflow database created successfully"
