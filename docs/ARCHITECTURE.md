# Pipeline Architecture

## High-Level Overview

The **Bangladesh Flight Price Pipeline** is designed as a robust, scalable ELT (Extract, Load, Transform) solution. It orchestrates the flow of data from raw CSV files through staging, validation, and transformation layers to a final analytical store.

```mermaid
graph TD
    subgraph Source
    A[Raw CSV Data]
    end

    subgraph Staging Layer
    B[(MySQL Staging)]
    end

    subgraph Processing Layer
    C{Data Validation}
    D[Transformation & Enrichment]
    end

    subgraph Analytics Layer
    E[(PostgreSQL Analytics)]
    F[KPI Tables]
    G[Data Quality Metrics]
    end

    A -->|Ingest| B
    B -->|Fetch| C
    C -->|Valid| D
    C -->|Invalid| G
    D -->|Load| E
    D -->|Compute| F
```

## Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Orchestration** | Apache Airflow | 2.7.3 | Workflow scheduling, dependency management, and monitoring. |
| **Staging DB** | MySQL | 8.0.35 | High-throughput ingestion of raw row-level data. |
| **Analytics DB** | PostgreSQL | 16.2 | Structured storage for enriched data and complex KPI queries. |
| **Processing** | Python | 3.11+ | Core logic for dataframes (Pandas), SQL interaction (SQLAlchemy), and validation. |
| **Containerization** | Docker | 29.1.0 | Isolates services and ensures consistent environments. |

## Data Flow Detail

### 1. Ingestion (`ingest_to_staging`)
- **Mechanism**: Batch processing (1,000 rows/chunk).
- **Normalization**: Column headers are standardized (snake_case).
- **Metadata Injection**: Adds `source_filename`, `ingestion_timestamp`, and `record_status`.
- **Target**: MySQL `raw_flight_data`.

### 2. Validation (`validate_data`)
Implements a "Quality Gate" pattern. Records must pass 6 specific checks:
1.  **Schema Integrity**: Presence of all required columns.
2.  **Type Safety**: Numeric and string type enforcement.
3.  **Completeness**: Critical fields must not be NULL.
4.  **Business Logic**: No negative fares.
5.  **Referential Integrity**: (Optional) City whitelist check.
6.  **Mathematical Consistency**: `Total Fare (BDT)` ≈ `Base Fare (BDT)` + `Tax & Surcharge (BDT)`.

**Audit**: All results are logged to PostgreSQL `data_quality_metrics`.

### 3. Transformation (`transform_data`)
Enriches valid data for business insights:
- **Fare Reconstruction**: Auto-calculates `Total Fare (BDT)` if missing.
- **Data Standardization**: Title-casing cities, trimming whitespace.
- **Seasonality Logic** (month-based classification):
    - **PEAK_EID**: May and July.
    - **PEAK_WINTER**: December and January.
    - **NON_PEAK**: Remaining months.

### 4. KPI Computation (`compute_kpis`)
Aggregates data into business-ready metrics:

| Metric | Logic | Target Table |
|--------|-------|--------------|
| **Airline Strategy** | Avg `Base`/`Total Fare (BDT)` per airline. | `kpi_airline_average` |
| **Seasonal Trends** | Price elasticity (Peak vs Non-Peak). | `kpi_seasonal_variation` |
| **Route Popularity** | Top 10 routes by booking volume. | `kpi_popular_routes` |

### 5. Loading (`load_to_postgres`)
- **Strategy**: Batch insert to `flights_enriched` with duplicate protection via `ON CONFLICT DO NOTHING`; KPI tables are refreshed with replace semantics.
- **Consistency**: Uses database transactions to ensure atomicity.
- **Resilience**: Runtime safeguards create the target PostgreSQL database and `flights_enriched` table if they are missing.

## Database Schema

### MySQL (Staging)
**Table**: `raw_flight_data`
- Optimized for write-speed.
- Stores raw strings/decimals to avoid ingestion failures on type mismatches.
- Includes `validation_errors` JSON column for debugging.

### PostgreSQL (Analytics)
**Schema**: Star-schema inspired design.
- **Fact Table**: `flights_enriched` (The source of truth).
- **Dimension/Aggr Tables**: `kpi_*` tables for fast dashboarding.
- **Audit Table**: `data_quality_metrics`.

## Error Handling & Resilience

- **Retry Policy**: 2 retries with a 5-minute delay.
- **Connection Reliability**: SQLAlchemy connection pooling and connection timeouts are enabled.
- **Idempotency**: KPI calculations can be re-run safely without duplicating data.
