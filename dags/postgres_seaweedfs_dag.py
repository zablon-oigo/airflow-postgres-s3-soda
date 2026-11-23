from airflow.sdk import dag, task
from airflow.sdk import Variable

from airflow.sdk.bases.hook import BaseHook

from datetime import datetime
from io import StringIO
from pathlib import Path
import zipfile

import requests
import pandas as pd
import boto3
from io import BytesIO


from botocore.config import Config
from sqlalchemy import create_engine, text


POSTGRES_URL = (
    "postgresql+psycopg://postgres:postgres@postgres:5432/analytics"
)


def get_postgres_engine():
    return create_engine(POSTGRES_URL)


default_args = {
    "owner": "zablon",
    "start_date": datetime(2026, 11, 11),
}


@dag(
    dag_id="postgres_seaweedfs_dag",
    default_args=default_args,
    schedule="@once",
    description="Load Titanic data into Astro PostgreSQL and SeaweedFS",
    catchup=False,
    tags=["postgres", "seaweedfs"],
)
def postgres_seaweedfs_dag():

    @task
    def download_data() -> str:
        REPO = "zablon-oigo/airflow-postgres-s3-soda"
        TAG = "dataset-v1"
        URL = f"https://github.com/{REPO}/releases/download/{TAG}/titanic.zip"

        destination = Path("/tmp")
        csv_file = destination / "titanic.csv"

        print("Downloading Titanic dataset")
        response = requests.get(URL, timeout=120, allow_redirects=True)
        response.raise_for_status()

        
        if not response.content.startswith(b"PK"):
            raise ValueError(
                f"Downloaded content is not a zip file. "
                f"First bytes: {response.content[:200]!r}"
            )

        print("Extracting dataset")
        with zipfile.ZipFile(BytesIO(response.content)) as archive:
            archive.extractall(destination)

        if not csv_file.exists():
            raise FileNotFoundError("titanic.csv was not found after extraction")

        print("Dataset ingestion completed")
        return csv_file.read_text(encoding="utf-8")

    @task
    def write_to_postgres(csv_data: str):
        df = pd.read_csv(StringIO(csv_data), sep=";")

        engine = get_postgres_engine()

        try:
            with engine.begin() as connection:
                connection.execute(
                    text("CREATE SCHEMA IF NOT EXISTS raw")
                )

            df.to_sql(
                name="titanic",
                schema="raw",
                con=engine,
                if_exists="replace",
                index=False,
                chunksize=1000,
                method="multi",
            )

            print(f"Inserted {len(df)} records into analytics.raw.titanic")

        finally:
            engine.dispose()

    @task
    def create_view():
        engine = get_postgres_engine()
        try:
            with engine.begin() as connection:
                connection.execute(
                    text("""
                        CREATE OR REPLACE VIEW raw.titanic_count_survivors AS
                        SELECT
                            "Sex",
                            SUM("Survived") AS survivors_count
                        FROM raw.titanic
                        GROUP BY "Sex";
                    """)
                )
            print("Created titanic_count_survivors view")
        finally:
            engine.dispose()

    @task
    def upload_to_seaweedfs(csv_data: str):
        connection = BaseHook.get_connection("s3_conn")
        extra = connection.extra_dejson

        bucket = Variable.get("seaweedfs_bucket", default="warehouse")

        access_key = extra.get("aws_access_key_id")
        secret_key = extra.get("aws_secret_access_key")
        endpoint = extra.get("endpoint_url")
        region = extra.get("region_name", "us-east-1")

        if not access_key or not secret_key:
            raise ValueError("AWS credentials missing from s3_conn Extra")

        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(
                signature_version="s3v4",
                **extra.get("config_kwargs", {}),
            ),
        )

        buckets = s3_client.list_buckets()["Buckets"]

        if not any(item["Name"] == bucket for item in buckets):
            s3_client.create_bucket(Bucket=bucket)
            print(f"Created bucket: {bucket}")

        s3_client.put_object(
            Bucket=bucket,
            Key="titanic.csv",
            Body=csv_data.encode("utf-8"),
        )

        print(f"Uploaded titanic.csv to SeaweedFS bucket '{bucket}'")

    download = download_data()
    write = write_to_postgres(download)
    view=create_view()
    upload = upload_to_seaweedfs(download)

    write >> view >> upload


postgres_seaweedfs_dag()