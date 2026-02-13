# Database Interaction Guide

This guide explains how to connect to and query the project databases used by the pipeline.

## Overview

The pipeline uses two databases:

- **MySQL (staging)**: stores raw ingested records (`raw_flight_data`)
- **PostgreSQL (analytics)**: stores transformed data, KPIs, and quality metrics (`flights_enriched`, `kpi_*`, `data_quality_metrics`)

## Connection Details

Use values from `config/.env` whenever possible.

| Database | Host (from machine) | Port | Database Name (default) |
|---|---|---:|---|
| MySQL | `localhost` | `3308` | `flight_staging` |
| PostgreSQL | `localhost` | `5435` | `flight_analytics` |

## Option 1: Connect from Docker Compose containers (recommended)

### PostgreSQL

```bash
docker compose exec postgres psql -U airflow_user -d flight_analytics
```

### MySQL

```bash
docker compose exec mysql mysql -u airflow_user -p flight_staging
```

When prompted, enter the password configured in `config/.env`.

## Option 2: Connect from local DB tools

You can use tools such as DBeaver, pgAdmin, or MySQL Workbench.

### PostgreSQL profile
- Host: `localhost`
- Port: `5435`
- Database: value of `POSTGRES_DATABASE` (default: `flight_analytics`)
- Username: value of `POSTGRES_USER`
- Password: value of `POSTGRES_PASSWORD`

### MySQL profile
- Host: `localhost`
- Port: `3308`
- Database: value of `MYSQL_DATABASE` (default: `flight_staging`)
- Username: value of `MYSQL_USER`
- Password: value of `MYSQL_PASSWORD`

## Common SQL Commands

### PostgreSQL: inspect analytics tables

```sql
\dt
SELECT COUNT(*) AS flights_count FROM flights_enriched;
SELECT COUNT(*) AS quality_checks FROM data_quality_metrics;
```

### PostgreSQL: review recent KPIs

```sql
SELECT * FROM kpi_airline_average ORDER BY computed_at DESC LIMIT 10;
SELECT * FROM kpi_seasonal_variation ORDER BY computed_at DESC LIMIT 10;
SELECT * FROM kpi_popular_routes ORDER BY computed_at DESC LIMIT 10;
```

### MySQL: inspect staging data

```sql
SHOW TABLES;
SELECT COUNT(*) AS staging_rows FROM raw_flight_data;
SELECT * FROM raw_flight_data ORDER BY ingestion_timestamp DESC LIMIT 20;
```

## Validate a Pipeline Run from SQL

After triggering a DAG run, you can check whether data was loaded successfully:

```sql
SELECT COUNT(*) FROM flights_enriched;
SELECT execution_timestamp, check_name, records_processed, records_invalid
FROM data_quality_metrics
ORDER BY execution_timestamp DESC
LIMIT 20;
```

You can also open generated JSON reports in `logs/reports/` for per-run summaries.

## Notes and Safety

- Prefer `SELECT` queries for exploration.
- Avoid `DELETE`, `TRUNCATE`, or `DROP` commands unless you intentionally want to remove data.
- If schema initialization was skipped previously, the pipeline now creates required PostgreSQL database/table resources at runtime.