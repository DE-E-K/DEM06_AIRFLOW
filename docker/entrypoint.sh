#!/bin/bash
set -e

echo "Waiting for PostgreSQL to be ready..."
sleep 10  # Give services time to start

echo "Initializing Airflow database..."
airflow db init

echo "Starting Airflow webserver..."
exec airflow webserver
