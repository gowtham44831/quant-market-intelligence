# Docker Setup

This repository contains the Airflow DAGs. Keep deployment credentials outside
Git and mount this directory into an Airflow container at `/opt/airflow/dags`.

## Suggested directory layout

```text
platform/
├── Dockerfile.airflow
├── docker-compose.yaml
├── requirements.txt
├── stocks.env              # private; never commit
├── logs/                   # generated; never commit
└── dags/                   # this repository
```

## 1. Create the Airflow Dockerfile

Create `Dockerfile.airflow` in the `platform` directory:

```dockerfile
FROM apache/airflow:2.10.2

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
```

Keep the Airflow version pinned. Test dependency upgrades before using a newer
image in production.

## 2. Create the dependency file

Create `requirements.txt` beside the Dockerfile:

```text
xgboost
pandas
numpy
scikit-learn
vaderSentiment==3.3.2
massive
psycopg2-binary
pendulum
joblib
```

For reproducible production builds, pin every dependency after validating a
working image.

## 3. Create private environment configuration

Copy `.env.example` to `platform/stocks.env` and replace every placeholder.
Never add `stocks.env` to Git.

Generate a unique Airflow Fernet key instead of reusing an example value:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Store the generated value in the private environment file:

```dotenv
AIRFLOW_FERNET_KEY=replace_with_generated_value
POSTGRES_PASSWORD=replace_with_a_strong_password
MARKET_API_KEY=replace_with_your_provider_key
```

Reference these values from Compose rather than embedding them directly:

```yaml
environment:
  AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY}
  POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
```

## 4. Build and initialize Airflow

Run these commands from the directory containing `docker-compose.yaml`:

```bash
docker compose build
docker compose up airflow-init
docker compose up -d
```

Open Airflow at `http://localhost:5050` when the webserver is healthy.

## 5. Validate the deployment

```bash
docker compose ps
docker compose logs --tail=100 airflow-scheduler
docker compose logs --tail=100 airflow-webserver
```

Confirm that the DAGs load without import errors before enabling schedules.

## Public-repository checklist

- Publish the `dags` directory as the Git root, or create an additional
  `.gitignore` at the `platform` root.
- Exclude `stocks.env`, `logs/`, `vault-data/`, database volumes, model files,
  and the retired `scripts/stock_pipeline.py` prototype.
- Do not publish live API keys, database passwords, Fernet keys, Vault tokens,
  or broker credentials.
- Review staged content with `git diff --cached` before pushing.
- Enable GitHub secret scanning and push protection.
