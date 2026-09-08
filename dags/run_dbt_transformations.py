# ---------------------------------------------------------------
# DAG : exécution automatique des transformations dbt
# ---------------------------------------------------------------
# Ce DAG déclenche dbt run puis dbt test, juste après que les
# données brutes ont été chargées par les DAGs d'ingestion
# (load_olist_to_dw et fetch_exchange_rate).

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging 

# 1. Définition du logger (OBLIGATOIRE pour éviter le NameError)
logger = logging.getLogger("airflow.task")

def task_failure_alert(context):
    """
    Callback exécuté automatiquement par Airflow quand une tâche échoue
    définitivement (après épuisement des retries).
    Ce callback logue l'erreur ; l'envoi de l'email est géré séparément
    et automatiquement par Airflow via email_on_failure ci-dessous.
    """
    task_instance = context.get('task_instance')
    logger.error(
        f"[ALERTE] Échec de la tâche '{task_instance.task_id}' "
        f"dans le DAG '{task_instance.dag_id}' "
        f"(run_id : {context.get('run_id')})"
    )
default_args = {
    "owner": "meryem",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": task_failure_alert,  
    "email_on_failure": True,                     # active l'envoi d'email en cas d'échec
    "email_on_retry": False,                      # pas besoin d'être notifiée à chaque tentative de retry
    "email": ["meryemmouguert@gmail.com"],           # N'oublie pas de mettre ton vrai e-mail ici
}

with DAG(
    dag_id="run_dbt_transformations",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    # Planifié un peu après les DAGs d'ingestion (qui tournent à minuit),
    # pour laisser le temps aux données d'être chargées avant de transformer
    schedule="0 1 * * *",   # tous les jours à 1h du matin
    catchup=False,
    tags=["dataops", "dbt", "transformation"],
    description="Exécute dbt run puis dbt test après l'ingestion des données",
) as dag:

    # BashOperator : exécute une commande shell directement depuis Airflow.
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="docker exec dbt dbt run", 
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="docker exec dbt dbt test",
    )

    # Dépendance entre tâches
    dbt_run >> dbt_test