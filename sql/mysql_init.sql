CREATE DATABASE IF NOT EXISTS flight_staging;
USE flight_staging;

CREATE TABLE IF NOT EXISTS raw_flight_data (
  id INT PRIMARY KEY AUTO_INCREMENT,
  airline VARCHAR(100) NOT NULL,
  source VARCHAR(100) NOT NULL,
  destination VARCHAR(100) NOT NULL,
  base_fare DECIMAL(10,2),
  source_name VARCHAR(255),
  destination_name VARCHAR(255),
  departure_date DATETIME,
  arrival_date DATETIME,
  duration_hours DECIMAL(5,2),
  stopovers VARCHAR(50),
  aircraft_type VARCHAR(100),
  class VARCHAR(50),
  booking_source VARCHAR(100),
  seasonality VARCHAR(50),
  days_before_departure INT,
  tax_surcharge DECIMAL(10,2),
  total_fare DECIMAL(10,2),
  source_file VARCHAR(255),
  ingestion_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
  record_status VARCHAR(50) DEFAULT 'VALID',
  validation_errors TEXT,
  INDEX idx_airline (airline),
  INDEX idx_source (source),
  INDEX idx_destination (destination)
);

CREATE TABLE IF NOT EXISTS validation_log (
  id INT PRIMARY KEY AUTO_INCREMENT,
  check_name VARCHAR(255),
  records_checked INT,
  records_passed INT,
  records_failed INT,
  log_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);
