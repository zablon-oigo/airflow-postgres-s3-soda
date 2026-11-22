![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Astro_Runtime-017CEE?logo=apacheairflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.12+-FF694B?logo=dbt&logoColor=white)
![Soda](https://img.shields.io/badge/Soda-Data_Contracts-6C47FF)
![SeaweedFS](https://img.shields.io/badge/SeaweedFS-S3_Object_Storage-65A30D)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.12+-3776AB?logo=python&logoColor=white)
![uv](https://img.shields.io/badge/uv-Package_Manager-DE5FE9?logo=uv&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-CI%2FCD-2088FF?logo=githubactions&logoColor=white)
![Build Status](https://github.com/zablon-oigo/airflow-postgres-s3-soda/actions/workflows/ci.yml/badge.svg)
![GitHub Release](https://img.shields.io/github/v/release/zablon-oigo/airflow-postgres-s3-soda?label=Production%20Release)

# Running the Pipeline Locally


## 1. Start Airflow

Start the local Astro development environment:

```bash
astro dev start
```

Verify that the containers are running:

```bash
docker ps
```

Open the Airflow web interface:

```text
http://localhost:8080
```

## 2. Initialize the PostgreSQL Database

Connect to the PostgreSQL container:

```bash
docker exec -it demo_878d9c-postgres-1 psql -U postgres
```

Create the analytics database and its schemas:

```sql
CREATE DATABASE analytics;
```

Connect to the newly created database:

```sql
\c analytics
```

Create the required schemas:

```sql
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
```

## 3. Connect Airflow to SeaweedFS

Airflow and SeaweedFS must be able to communicate over a shared Docker network.

Connect the Airflow network to the SeaweedFS container:

```bash
docker network connect <airflow_network> seaweedfs
```

Replace <airflow_network> with the actual Airflow Docker network name.

To identify the network:

```bash
docker network ls
```

Inspect the Airflow containers if necessary:

```bash
docker inspect <airflow-container>
```

### Test connectivity

Run the following command from an Airflow worker or scheduler container:

```bash
docker exec -it <airflow-worker-or-scheduler> \
  curl -I http://seaweedfs:8333
```

A successful HTTP response confirms that the Airflow container can reach the SeaweedFS S3 endpoint.

## 4. Execute the Airflow DAGs

1. Open the Airflow UI at http://localhost:8080.
2. Locate the Titanic pipeline DAG.
3. Enable the DAG.
4. Trigger a DAG run.
5. Monitor task execution and logs.

The pipeline downloads the dataset from GitHub Releases, loads it into PostgreSQL, and stores the dataset in SeaweedFS.

## 5. Verify Data in SeaweedFS

Use the AWS CLI to list objects in the warehouse bucket:

```bash
AWS_ACCESS_KEY_ID=admin \
AWS_SECRET_ACCESS_KEY=password \
aws --endpoint-url http://localhost:8333 \
s3 ls s3://warehouse/ --recursive
```

This verifies that the dataset has been successfully written to object storage.

## 6. Run dbt Transformations

Navigate to the dbt project directory:

```bash
cd analytics
```

### Validate the dbt configuration

```bash
uv run dbt debug
```

### Clean previous build artifacts

```bash
uv run dbt clean
```

### Parse the project

```bash
uv run dbt parse
```

### Inspect configured sources

```bash
uv run dbt ls --resource-type source
```

### Build models and execute tests

```bash
uv run dbt build
```

### Generate documentation

```bash
uv run dbt docs generate
```

### Serve documentation

```bash
uv run dbt docs serve --port 8081
```

Open the documentation at:

```text
http://localhost:8081
```

Port 8081 is used to avoid conflicting with the Airflow UI on port 8080

## 7. Verify dbt Models in PostgreSQL

After running dbt build, connect to PostgreSQL to verify that the staging and marts models have been created successfully.

Connect to PostgreSQL
```sh
docker exec -it demo_878d9c-postgres-1 psql -U postgres -d analytics
```

```sh
\dt raw.*
\dv staging.*
\dt marts.*
```



## 8. Run Soda Data Quality Tests

Soda is used to validate data quality through contracts across the PostgreSQL datasets.

### Test the data source connection

```bash
uv run soda data-source test -ds config.yml
```

### Create a contract

Generate a contract for the Titanic fact table:

```bash
uv run soda contract create \
  -ds config.yml \
  -d titanic/marts/fct_titanic \
  -f contracts/fct_titanic.yml
```

### Test the contract

```bash
uv run soda contract test -c contracts/fct_titanic.yml
```

### Verify the contract against the data source

```bash
uv run soda contract verify \
  -ds config.yml \
  -c contracts/fct_titanic.yml
```

The contract validates the dataset against its defined schema, constraints, and data quality expectations.

