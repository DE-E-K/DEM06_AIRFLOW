-- Airflow will auto-create its database via AIRFLOW__CORE__SQL_ALCHEMY_CONN
-- Flight Analytics database is auto-created via POSTGRES_DATABASE env var

-- Tables are created in the POSTGRES_DATABASE (flight_analytics)

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
  loaded_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
