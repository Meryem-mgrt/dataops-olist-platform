import os                  # Permet de manipuler les fichiers et dossiers
import logging             # Permet d'écrire des messages dans les logs d'Airflow

from airflow import DAG   # Importe la classe DAG qui permet de créer un workflow Airflow

from airflow.operators.python import PythonOperator # Permet de créer une tâche qui exécute une fonction Python

from airflow.exceptions import AirflowException #Permet d'arrêter proprement le DAG avec un message d'erreur clair

from datetime import datetime, timedelta # timedelta : pour gérer les délais entre deux tentatives

import pandas as pd # Bibliothèque utilisée pour lire et manipuler les fichiers CSV

from sqlalchemy import create_engine # Permet de créer une connexion entre Python et PostgreSQL
from sqlalchemy import text

DW_CONN = "postgresql://dataops_user:dataops_pass@host.docker.internal:5433/dataops_dw" #Connexion au Data Warehouse

FILES_TO_TABLES = { #Dictionnaire des fichiers
    "olist_customers_dataset.csv": "raw_customers",  #nom de fichier,nom de la table
    "olist_orders_dataset.csv": "raw_orders",
    "olist_order_items_dataset.csv": "raw_order_items",
    "olist_order_payments_dataset.csv": "raw_order_payments",
    "olist_products_dataset.csv": "raw_products",
    "olist_sellers_dataset.csv": "raw_sellers",
}

DATA_DIR = "/opt/airflow/data/raw" #dossier des données

logger = logging.getLogger(__name__)


def load_csv_to_postgres(filename, table_name):
    filepath = f"{DATA_DIR}/{filename}"

    if not os.path.exists(filepath):
        raise AirflowException(f"Fichier introuvable : {filepath}.")

    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        raise AirflowException(f"Erreur de lecture du fichier {filename} : {e}")

    if df.empty:
        raise AirflowException(f"Le fichier {filename} est vide, chargement annulé.")

    try:
        engine = create_engine(DW_CONN)
        with engine.begin() as conn:
            # On vide la table (TRUNCATE) plutôt que de la supprimer (DROP),
            # pour ne pas casser les vues dbt qui en dépendent
            conn.execute(text(f'TRUNCATE TABLE "{table_name}"'))
        df.to_sql(table_name, engine, if_exists="append", index=False)
    except Exception as e:
        raise AirflowException(f"Erreur de chargement dans {table_name} : {e}")

    logger.info(f"{len(df)} lignes chargées avec succès dans {table_name}")

def task_failure_alert(context):
    task_instance = context.get('task_instance')
    logger.error(
        f"[ALERTE] Échec de la tâche '{task_instance.task_id}' "
        f"dans le DAG '{task_instance.dag_id}' à {context.get('execution_date')}"
    )
default_args = {
    "owner": "meryem",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="load_olist_to_dw",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",   # planning : une exécution automatique par jour
    catchup=False,
    tags=["dataops", "olist", "ingestion"],
) as dag:

    for filename, table_name in FILES_TO_TABLES.items():
        PythonOperator(
            task_id=f"load_{table_name}",
            python_callable=load_csv_to_postgres,
            op_kwargs={"filename": filename, "table_name": table_name},
        )