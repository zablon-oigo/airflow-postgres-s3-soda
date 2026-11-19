from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime
from pathlib import Path
import pandas as pd
import requests
import zipfile


default_args = {
    "owner": "zablon",
    "start_date": datetime(2026, 11, 11),
}


@dag(
    default_args=default_args,
    schedule="@once",
    description="Titanic dataset ingestion and analysis",
    catchup=False,
    tags=["Titanic"],
)
def titanic_processing():
    start = EmptyOperator(task_id="start")

    @task
    def download_data() -> str:
        REPO = "zablon-oigo/airflow-postgres-s3-soda"
        TAG = "dataset-v1"
        URL = f"https://github.com/{REPO}/releases/download/{TAG}/titanic.zip"

        destination = Path("/tmp")
        zip_file = destination / "titanic.zip"
        csv_file = destination / "titanic.csv"

        print("Downloading Titanic dataset")
        response = requests.get(URL, timeout=120)
        response.raise_for_status()
        zip_file.write_bytes(response.content)

        print("Extracting dataset")
        with zipfile.ZipFile(zip_file, "r") as archive:
            archive.extractall(destination)
        zip_file.unlink()

        if not csv_file.exists():
            raise FileNotFoundError("titanic.csv was not found after extraction")

        print("Dataset ingestion completed!")
        return str(csv_file)

    @task
    def analyze_survivors(source: str) -> None:
        df = pd.read_csv(source, sep=";")
        survivors = df.loc[df["Survived"] == 1, "Survived"].count()
        print(f"Total survivors: {survivors}")


    @task
    def survivors_sex(source: str) -> None:
        df = pd.read_csv(source, sep=";")
        result = (
            df.loc[df["Survived"] == 1]
            .groupby("Sex")["Survived"]
            .count()
        )
        print(result)

    download = download_data()
    surv_count = analyze_survivors(download)
    surv_sex = survivors_sex(download)

    last = BashOperator(
        task_id="last_task",
        bash_command='echo "This is the last task"',
    )
    end = EmptyOperator(task_id="end")

    start >> download
    [surv_count, surv_sex] >> last >> end


titanic_processing()