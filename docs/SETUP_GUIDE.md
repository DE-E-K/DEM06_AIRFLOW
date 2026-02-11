# 🛠️ Setup Guide

## Prerequisites

Before starting, ensure your system meets the following requirements:

| Requirement | Details |
|-------------|---------|
| **OS** | Windows 10/11 (WSL2 recommended), macOS, or Linux |
| **Docker** | Docker Desktop (Engine v29) |
| **Compose** | Docker Compose v2.0+ |
| **Resources** | Min: 4GB RAM, 2 CPU Cores |
| **Python** | 3.11 (for utility scripts) |

## Quick Start Instructions

### 1. Repository Setup
Navigate to your desired workspace and clone the project:

```bash
git clone https://github.com/DE-E-K/DEM06_AIRFLOW.git
cd DEM06_AIRFLOW
```

### 2. Security Configuration
Generate a secure Fernet key for Airflow:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Open `config/.env` and paste the key:
```ini
FERNET_KEY=YourGeneratedKeyHere...
```

### 3. Data Acquisition
1. Download the **Flight Price Dataset of Bangladesh** from [Kaggle](https://www.kaggle.com/datasets/atikrahman/flight-price-dataset-of-bangladesh).
2. Place the CSV file in the `data/` directory:
   ```bash
   # Example command
   mv ~/Downloads/flight_price.csv ./data/Flight_Price_Dataset_of_Bangladesh.csv
   ```
3. **Verify**: Ensure the file is named exactly `Flight_Price_Dataset_of_Bangladesh.csv`.

### 4. Application Launch
Spin up the entire stack (MySQL, Postgres, Airflow) using Docker Compose:

```bash
docker compose --env-file config/.env -f docker/docker-compose.yml up --build -d
```
> **Note**: The first build may take 5-10 minutes as it downloads images and installs dependencies.

### 5. Verify & Access
Check that all containers are healthy:
```bash
docker compose -f docker/docker-compose.yml ps
```

| Service | access URL | Credentials |
|---------|-----------|-------------|
| **Airflow UI** | http://localhost:8080 | user: `admin` / pass: `admin` |
| **MySQL Staging** | `localhost:3306` | `airflow_user` / `airflow_password` |
| **Postgres Utils** | `localhost:5432` | `airflow_user` / `airflow_password` |

## Pipeline Operations

### Triggering the DAG
1. Log in to Airflow UI.
2. Toggle the `flight_price_pipeline` switch to **ON**.
3. Click the **Trigger DAG** (Play) button.

### Monitoring
- **Graph View**: Watch tasks turn green (Success) or red (Fail).
- **Logs**: Click any task execution box -> **Log** to see detailed output.

## Troubleshooting

### Common Issues

#### `check_csv_exists` Fails
- **Cause**: File missing or incorrectly named.
- **Fix**: Check `data/` folder content.
  ```bash
  ls -l data/
  ```

#### Database Connection Timeout
- **Cause**: Containers not fully ready.
- **Fix**: Wait 60s or restart services.
  ```bash
  docker compose -f docker/docker-compose.yml restart
  ```

#### "Fernet Key" Error
- **Cause**: Invalid or missing key in `.env`.
- **Fix**: Re-generate key and restart containers.

### Hard Reset
If you need to wipe everything (including databases) and start fresh:
```bash
docker compose -f docker/docker-compose.yml down -v
```
**⚠️ WARNING**: This deletes all processed data!
