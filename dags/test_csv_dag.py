from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# Fonction qui lit le fichier CSV
def read_csv():
    file_path = "/opt/airflow/data/raw/sample.csv"

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Le fichier n'existe pas : {file_path}")

    df = pd.read_csv(file_path)

    print("=" * 50)
    print("Le fichier a été lu avec succès !")
    print(f"Nombre de lignes : {len(df)}") 
    print("\nContenu du fichier :")
    print(df)
    print("=" * 50)


with DAG(
    dag_id="test_csv_dag",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["test", "csv"],
) as dag:

    read_csv_task = PythonOperator(
        task_id="read_csv",
        python_callable=read_csv,
    )

    read_csv_task