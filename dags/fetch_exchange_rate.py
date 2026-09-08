# ---------------------------------------------------------------
# DAG indépendant : récupération du taux de change (API externe)
# ---------------------------------------------------------------
# Ce DAG est volontairement séparé de load_olist_to_dw.py :
# il gère une source différente (API, pas CSV), avec sa propre
# logique et sa propre fréquence, sans dépendre de l'autre DAG.

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import logging
import requests  # librairie pour faire des appels HTTP vers l'API externe
import pandas as pd
from sqlalchemy import create_engine

# Connexion vers l'entrepôt de données PostgreSQL (la même que l'autre DAG,
# puisqu'on écrit dans la même base, juste une table différente)
DW_CONN = "postgresql://dataops_user:dataops_pass@host.docker.internal:5433/dataops_dw"

logger = logging.getLogger(__name__)


def fetch_exchange_rate():
    """
    Récupère le taux de change du jour (BRL vers USD et EUR)
    depuis l'API Frankfurter, et l'enregistre dans PostgreSQL.

    Contrairement aux tâches CSV (qui rechargent tout à chaque fois),
    ici on AJOUTE une nouvelle ligne à chaque exécution, pour construire
    un historique des taux dans le temps (une vraie série temporelle).
    """

    # L'URL de l'API : on demande le taux de base BRL (Real brésilien),
    # converti vers USD et EUR
    url = "https://api.frankfurter.dev/v1/latest?base=BRL&symbols=USD,EUR"

    # On appelle l'API avec un timeout de 10 secondes :
    # si l'API ne répond pas dans ce délai, on abandonne plutôt que
    # de bloquer indéfiniment la tâche (bonne pratique réseau)
    try:
        response = requests.get(url, timeout=10)  # 10 s
        # raise_for_status() lève une erreur si le code HTTP n'est pas 200 (succès)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        # Toute erreur réseau (timeout, API indisponible, etc.) est
        # transformée en erreur Airflow claire, visible dans les logs
        raise AirflowException(f"Erreur lors de l'appel à l'API de taux de change : {e}")

    # On transforme la réponse JSON en dictionnaire Python
    data = response.json()

    # On vérifie que les champs attendus sont bien présents dans la réponse
    # (protection contre un changement de format de l'API)
    if "rates" not in data or "USD" not in data["rates"] or "EUR" not in data["rates"]:
        raise AirflowException(f"Réponse API inattendue, format invalide : {data}")

    # On construit une seule ligne de données, prête à être insérée en base
    row = {
        "base_currency": data["base"],           # "BRL"
        "target_usd": data["rates"]["USD"],       # ex. 0.19
        "target_eur": data["rates"]["EUR"],       # ex. 0.17
        "rate_date": data["date"],                # date du taux, fournie par l'API
        "fetched_at": datetime.utcnow(),          # horodatage exact de la récupération
    }

    # On transforme cette unique ligne en petit DataFrame pandas,
    # pour réutiliser la même logique d'écriture que le reste du projet
    df = pd.DataFrame([row])

    try:
        engine = create_engine(DW_CONN)
        # if_exists="append" : on AJOUTE cette ligne à la table existante,
        # sans la vider (contrairement aux CSV) — on veut garder
        # l'historique des taux jour après jour
        df.to_sql("raw_exchange_rate", engine, if_exists="append", index=False)
    except Exception as e:
        raise AirflowException(f"Erreur d'écriture du taux de change en base : {e}")

    logger.info(f"Taux de change du {row['rate_date']} enregistré avec succès.")


def task_failure_alert(context):
    """
    Callback exécuté automatiquement par Airflow quand une tâche échoue
    définitivement (après épuisement des retries).

    Ce callback ne fait que LOGUER l'erreur, de façon structurée et lisible.
    L'envoi de l'email en lui-même est géré séparément et automatiquement
    par Airflow, grâce aux paramètres email_on_failure / email ci-dessous
    (les deux mécanismes sont indépendants et complémentaires).
    """
    task_instance = context.get('task_instance')
    logger.error(
        f"[ALERTE] Échec de la tâche '{task_instance.task_id}' "
        f"dans le DAG '{task_instance.dag_id}' "
        f"(run_id : {context.get('run_id')})"
    )


# Arguments par défaut du DAG : mêmes principes que load_olist_to_dw
# (retries en cas d'échec temporaire, ex. API momentanément indisponible)
default_args = {
    "owner": "meryem",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": task_failure_alert,
    "email_on_failure": True,                     # active l'envoi d'email en cas d'échec
    "email_on_retry": False,                      # pas besoin d'être notifiée à chaque tentative de retry
    "email": ["meryemmouguert@gmail.com"],  
}


with DAG(
    dag_id="fetch_exchange_rate",              # identifiant unique de ce DAG dans Airflow
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",                          # récupère un taux frais chaque jour
    catchup=False,                              # n'exécute pas les jours passés manqués
    tags=["dataops", "api", "enrichment"],      # tags différents de l'autre DAG
    description="Récupère quotidiennement le taux de change BRL -> USD/EUR via l'API Frankfurter",
) as dag:

    fetch_rate_task = PythonOperator(
        task_id="fetch_exchange_rate",
        python_callable=fetch_exchange_rate,
    )