# Challenges Encountered and Resolutions

This document summarizes the implementation and operational challenges observed during pipeline development and execution, along with the actions taken to resolve them.

## Summary Table

| ID | Challenge | Impact | Resolution Status |
|---|---|---|---|
| C1 | PostgreSQL database did not exist (`flight_analytics`) | `load_to_postgres` failed to connect | Resolved |
| C2 | Target table missing (`flights_enriched`) | Insert step failed with `UndefinedTable` | Resolved |
| C3 | Schema mismatch (`source_name` not in target table) | Insert step failed with `UndefinedColumn` | Resolved |
| C4 | Report artifact not persisted as file | No downloadable execution report | Resolved |
| C5 | Documentation drift from implementation | Inconsistent setup and operational guidance | Resolved |
| C6 | Dependency conflicts during environment setup | Build/startup instability and import issues | Resolved |
| C7 | Initial connectivity to both databases | Early task failures when MySQL/PostgreSQL were not ready | Resolved |

## Detailed Challenges and Resolutions

### C1. PostgreSQL Database Missing

**Observed issue**
- Runtime error: `database "flight_analytics" does not exist`.

**Root cause**
- Database initialization scripts were not consistently applied in environments with existing Docker volumes.

**Resolution**
- Added runtime safeguard in database connection logic to create the target PostgreSQL database if missing before engine creation.

**Outcome**
- Connection failures due to missing `flight_analytics` were eliminated.

---

### C2. Missing `flights_enriched` Table

**Observed issue**
- Runtime error: `relation "flights_enriched" does not exist`.

**Root cause**
- Table initialization did not always execute before first pipeline insert.

**Resolution**
- Added runtime table bootstrap to ensure `flights_enriched` exists before bulk inserts.

**Outcome**
- Initial loading no longer fails because of an absent analytics table.

---

### C3. Insert Schema Mismatch

**Observed issue**
- Runtime error: `column "source_name" of relation "flights_enriched" does not exist`.

**Root cause**
- Transformed payload included fields that were not present in the target PostgreSQL table schema.

**Resolution**
- Updated bulk insert logic to introspect target table columns and insert only matching fields.
- Added warning logs for dropped columns to make schema differences visible.

**Outcome**
- `load_to_postgres` succeeds even when transformed records include additional non-persisted fields.

---

### C4. Report Output Was Not Persisted

**Observed issue**
- Final report existed only in XCom and was not available as a file artifact.

**Resolution**
- Updated `generate_report` task to write JSON reports to `logs/reports/`.
- Included run metadata and key counts in each report file.

**Outcome**
- Each DAG run now produces a persistent report file suitable for review and sharing.

---

### C5. Documentation Misalignment

**Observed issue**
- Some docs had outdated ports, command paths, and behavior descriptions.

**Resolution**
- Updated setup, architecture, DAG, and database interaction docs to match current implementation.
- Standardized tone and formatting for professional delivery.

**Outcome**
- Documentation now accurately reflects runtime behavior and operational steps.

---

### C6. Dependency Conflicts During Setup

**Observed issue**
- Package dependency conflicts caused unstable container builds and runtime import problems across the project stack (Docker, Python, Airflow, MySQL, and PostgreSQL connectors).

**Root cause**
- Mixed dependency constraints between the Airflow base image and project-level Python packages.

**Resolution**
- Consolidated package versions in project dependency configuration.
- Rebuilt containers with clean dependency installation to avoid stale or conflicting package layers.

**Outcome**
- Environment setup became reproducible, and runtime import conflicts were removed.

---

### C7. Initial Connection to Both Databases (MySQL and PostgreSQL)

**Observed issue**
- Early pipeline runs failed when one or both databases were not fully ready during startup.

**Root cause**
- Service startup timing and readiness gaps between orchestrated containers and task execution.

**Resolution**
- Enforced service health checks and dependency ordering in Docker Compose.
- Added runtime safeguards for PostgreSQL database and table creation, and retained the retry strategy in DAG defaults.

**Outcome**
- Pipeline tasks now connect reliably to both MySQL and PostgreSQL during initial and subsequent runs.

## Lessons Learned

- Runtime safeguards reduce failures caused by partially initialized environments.
- Schema-aware inserts are safer than assuming transformed payload and target table are always identical.
- Persisting execution artifacts improves traceability and handover quality.
- Continuous documentation alignment is essential for operational reliability.
- Dependency version control is critical for stable builds and predictable runtime behavior.
- Readiness checks and startup sequencing are essential in multi-database pipelines.