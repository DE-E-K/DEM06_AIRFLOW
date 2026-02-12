#!/bin/bash
set -e

# Create flight analytics database
echo "Creating flight_analytics database..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE flight_analytics;
    ALTER DATABASE flight_analytics OWNER TO "$POSTGRES_USER";
EOSQL

# Now create tables in flight_analytics
echo "Creating tables in flight_analytics..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "flight_analytics" <<-EOSQL
    CREATE TABLE IF NOT EXISTS flights_enriched (
      id SERIAL PRIMARY KEY,
      airline VARCHAR(100),
      source VARCHAR(100),
      destination VARCHAR(100),
      base_fare NUMERIC(10,2),
      tax_surcharge NUMERIC(10,2),
      total_fare NUMERIC(10,2),
      flight_date DATE,
      season VARCHAR(50),
      is_valid BOOLEAN DEFAULT TRUE,
      loaded_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      CONSTRAINT unique_flight_record UNIQUE (airline, source, destination, flight_date, total_fare)
    );

    CREATE TABLE IF NOT EXISTS kpi_airline_average (
      id SERIAL PRIMARY KEY,
      airline VARCHAR(100),
      avg_base_fare NUMERIC(10,2),
      avg_tax_surcharge NUMERIC(10,2),
      avg_total_fare NUMERIC(10,2),
      booking_count INT,
      computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS kpi_seasonal_variation (
      id SERIAL PRIMARY KEY,
      airline VARCHAR(100),
      avg_fare_peak NUMERIC(10,2),
      avg_fare_non_peak NUMERIC(10,2),
      fare_difference NUMERIC(10,2),
      peak_percentage_increase NUMERIC(6,2),
      peak_booking_count INT,
      non_peak_booking_count INT,
      computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS kpi_popular_routes (
      id SERIAL PRIMARY KEY,
      source VARCHAR(100),
      destination VARCHAR(100),
      booking_count INT,
      route_rank INT,
      avg_fare_on_route NUMERIC(10,2),
      computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS data_quality_metrics (
      id SERIAL PRIMARY KEY,
      check_name VARCHAR(255),
      check_type VARCHAR(50),
      records_processed INT,
      records_valid INT,
      records_invalid INT,
      error_message TEXT,
      execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
EOSQL

echo "PostgreSQL initialization complete!"
