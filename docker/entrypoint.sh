#!/bin/bash
set -e

# Only the first container (webserver) should init the database
if [ "$1" = "webserver" ]; then
  echo "Waiting for PostgreSQL to be ready..."
  sleep 10

  echo "Initializing Airflow database..."
  airflow db init
fi

echo "Starting Airflow $1..."
exec airflow "$@"
