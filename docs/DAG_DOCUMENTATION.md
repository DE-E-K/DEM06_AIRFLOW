# Airflow DAG Documentation

## DAG Overview

- **DAG ID**: `flight_price_pipeline`
- **Schedule**: User-configurable (Default: `@daily`)
- **Owner**: `analytics-team`
- **SLA**: N/A
- **Catchup**: `False` (Historical runs ignored)

## Task Dependency Graph

```mermaid
graph LR
    A[check_csv_exists] --> B[ingest_to_staging]
    B --> C[validate_data]
    C --> D[transform_data]
    D --> E[compute_kpis]
    E --> F[load_to_postgres]
    F --> G[generate_report]
    G --> H[pipeline_complete]
    
    style A fill:#e1f5fe,stroke:#01579b
    style C fill:#fff9c4,stroke:#fbc02d
    style F fill:#e8f5e9,stroke:#2e7d32
```

## Task Reference

### 1. `check_csv_exists`
- **Type**: `PythonOperator`
- **Goal**: Fail-fast check to ensure the source file is present.
- **Fail Condition**: File missing at `CSV_INPUT_PATH`.

### 2. `ingest_to_staging`
- **Type**: `PythonOperator`
- **Goal**: High-speed batch insert of raw CSV data into MySQL.
- **Details**:
    - Chunk Size: 2,000 rows
    - Normalization: Column names converted to `snake_case`.

### 3. `validate_data`
- **Type**: `PythonOperator`
- **Goal**: Apply business rules and schema validation.
- **Output**: JSON Validation Report.
- **Side Effect**: Writes to `data_quality_metrics` table.

### 4. `transform_data`
- **Type**: `PythonOperator`
- **Goal**: Pure functional transformation of valid records.
- **Logic**:
    - `Total Fare (BDT)` calculation.
    - `Seasonality` logic (derived from `Departure Date & Time` if missing).

### 5. `compute_kpis`
- **Type**: `PythonOperator`
- **Goal**: Aggregate data into business metrics.
- **Metrics**:
    - Airline Average Fares
    - Seasonal Price Surges
    - Route Popularity Ranking

### 6. `load_to_postgres`
- **Type**: `PythonOperator`
- **Goal**: Persist enriched data and KPIs.
- **Strategy**:
    - `flights_enriched`: Batch insert with `ON CONFLICT DO NOTHING` on duplicate keys.
    - `kpi_*`: Replace current KPI snapshots per run.
    - Automatically creates the PostgreSQL target database and `flights_enriched` table when absent.

### 7. `generate_report`
- **Type**: `PythonOperator`
- **Goal**: Compile final execution stats and persist them as a report artifact.
- **Output**:
    - XCom payload with run summary.
    - JSON report file in `/opt/airflow/logs/reports` (mounted to `logs/reports` in the workspace).

## Configuration & Alerts

### Retry Strategy
- **Count**: 2 retries
- **Delay**: 5 minutes

### Notifications
- **Email**: Not enabled by default in the DAG.
- **Logs**: Standard Airflow logs mounted in `./logs`.
