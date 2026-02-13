# Docker Setup for Flight Pipeline

## Quick Start

### 1. Prerequisites
- Docker Desktop installed and running
- Docker Compose V2

### 2. Environment Setup
The `.env` file in the `config/` directory contains all necessary environment variables. Copy it to the root if needed:
```bash
cp config/.env .env
```

### 3. Start the Pipeline
```bash
# Build and start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f
```

### 4. Access Services
- **Airflow Web UI**: http://localhost:8080
  - Username: `admin` (from AIRFLOW_DEFAULT_USER)
  - Password: `admin_password` (from AIRFLOW_DEFAULT_PASSWORD)
- **MySQL**: localhost:3306
- **PostgreSQL**: localhost:5432

### 5. Initialize Databases
The databases will be automatically initialized with the SQL scripts in the `sql/` directory.

## Architecture

The Docker setup includes:
- **PostgreSQL**: Airflow metadata DB + Analytics DB
- **MySQL**: Staging database for raw flight data
- **Airflow Webserver**: Web interface on port 8090
- **Airflow Scheduler**: DAG execution scheduler
- **Airflow Triggerer**: Handles deferred tasks

## Common Commands

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (WARNING: deletes data)
docker-compose down -v

# Rebuild images
docker-compose build

# Restart specific service
docker-compose restart airflow-scheduler

# View logs for specific service
docker-compose logs -f airflow-webserver

# Execute airflow CLI commands
docker-compose run --rm airflow-cli airflow dags list

# Access airflow container shell
docker-compose exec airflow-webserver bash
```

## Troubleshooting

### Permission Issues
If you encounter permission errors:
```bash
# On Linux/WSL
mkdir -p ./logs ./plugins
chmod -R 777 ./logs ./plugins

# On Windows, grant full access to these folders
```

### Database Connection Issues
- Ensure MySQL and PostgreSQL services are healthy:
  ```bash
  docker-compose ps
  ```
- Check database logs:
  ```bash
  docker-compose logs mysql
  docker-compose logs postgres
  ```

### Airflow Not Starting
- Check the init service completed successfully:
  ```bash
  docker-compose logs airflow-init
  ```
- Verify environment variables in `.env` file

## Development

### Testing DAGs
```bash
# Test a specific DAG
docker-compose run --rm airflow-cli airflow dags test flight_pipeline_dag 2024-01-01

# List all DAGs
docker-compose run --rm airflow-cli airflow dags list
```

### Accessing Databases

**MySQL:**
```bash
docker-compose exec mysql mysql -u root -plearnairflow flight_staging
```

**PostgreSQL:**
```bash
docker-compose exec postgres psql -U airflow_user -d flight_analytics
```

## Volumes

- `postgres-db-volume`: PostgreSQL data persistence
- `mysql-db-volume`: MySQL data persistence
- Local directories mounted as volumes:
  - `./dags` → `/opt/airflow/dags`
  - `./logs` → `/opt/airflow/logs`
  - `./plugins` → `/opt/airflow/plugins`
  - `./src` → `/opt/airflow/src`
  - `./data` → `/opt/airflow/data`

## Environment Variables

Key environment variables (defined in `config/.env`):
- `AIRFLOW_UID`: User ID for Airflow (default: 50000)
- `FERNET_KEY`: Encryption key for Airflow
- Database credentials for MySQL and PostgreSQL
- Application settings (CSV path, validation threshold, etc.)
- Default Airflow admin user credentials
