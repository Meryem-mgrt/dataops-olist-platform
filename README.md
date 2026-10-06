![dbt CI](https://github.com/Meryem-mgrt/dataops-olist-platform/actions/workflows/dbt_ci.yml/badge.svg)

# DataOps Platform — Olist E-Commerce

Plateforme DataOps pour l'intégration, la qualité et la gouvernance des données,
construite avec Apache Airflow, dbt, PostgreSQL et Docker, dans le cadre d'un stage
PFA chez Zenithsoft (Rabat).

## Contexte

Ce projet met en place une chaîne DataOps complète simulant un cas d'usage e-commerce :
ingestion automatisée de données réparties sur plusieurs tables liées (clients,
commandes, paiements, produits, vendeurs) et sur une API externe, transformation et
contrôle qualité selon une approche **ELT**, gouvernance (documentation et traçabilité),
intégration continue, supervision active, et restitution via un dashboard décisionnel.

## Architecture
CSV Olist (6 fichiers) API taux de change (Frankfurter)
│ │
└──────────────┬───────────────────┘
▼
Apache Airflow (Extract + Load)
│
▼
PostgreSQL — entrepôt dédié (postgres_dw)
│
▼
dbt (Transform — ELT)
staging → intermediate → marts
│
▼
Power BI

Chaque brique tourne dans un conteneur Docker indépendant. Airflow et l'entrepôt de
données (+ dbt) sont déployés via deux fichiers Docker Compose distincts, qui
communiquent via la machine hôte (connexion réseau sur le port publié, et pilotage de
dbt par Airflow via le socket Docker monté — *Docker-outside-of-Docker*).

**Approche ELT, pas ETL** : Airflow se limite à l'extraction et au chargement brut des
données (sans transformation) ; l'ensemble des transformations (nettoyage, typage,
jointures, agrégations) est exécuté directement dans PostgreSQL par dbt.

## Stack technique

| Composant | Outil | Rôle |
|---|---|---|
| Orchestration | Apache Airflow | Planification et exécution des pipelines d'ingestion (DAGs) |
| Transformation & qualité | dbt | Modélisation SQL en couches, tests de qualité, documentation |
| Stockage | PostgreSQL | Entrepôt de données dédié (`postgres_dw`) |
| Conteneurisation | Docker / Docker Compose | Environnement reproductible |
| CI/CD | GitHub Actions | Exécution automatique de `dbt run` / `dbt test` à chaque push |
| Supervision | SMTP (Gmail) | Notification email automatique en cas d'échec de tâche |
| Restitution | Power BI | Dashboard connecté à la table finale |

## Données

**Source principale** : [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
(dataset public, utilisé en l'absence de données réelles d'entreprise).

À télécharger et placer dans `data/raw/` :
- `olist_customers_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`

**Source secondaire** : [API Frankfurter](https://frankfurter.dev) (taux de change
BRL → USD/EUR), interrogée quotidiennement, sans configuration requise.

## Structure du projet
├── dags/
│ ├── load_olist_to_dw.py # Ingestion batch des 6 CSV
│ ├── fetch_exchange_rate.py # Ingestion incrémentale API (taux de change)
│ └── run_dbt_transformations.py # Déclenche dbt run + dbt test depuis Airflow
├── data/raw/ # CSV Olist (non versionnés, voir .gitignore)
├── dbt/
│ ├── models/
│ │ ├── staging/ # Nettoyage, 1 modèle par table brute
│ │ ├── intermediate/ # Agrégations et jointures
│ │ └── marts/ # Table finale : mart_sales_summary
│ ├── seeds/ # Échantillons de données pour les tests CI
│ ├── Dockerfile
│ ├── dbt_project.yml
│ └── profiles.yml # cibles dev (local) et ci (GitHub Actions)
├── powerbi/
│ └── Dashboard_DataOps_Olist.pbix
├── .github/workflows/
│ └── dbt_ci.yml # Pipeline CI/CD
├── docker-compose.yml # postgres_dw + dbt
├── docker-compose-airflow.yaml # Apache Airflow
├── Dockerfile.airflow # Image Airflow personnalisée (client Docker CLI)
├── .env # Secrets (non versionné)
└── README.md

## Démarrage

```bash
# 1. Lancer l'entrepôt de données et dbt
docker compose -f docker-compose.yml up -d --build

# 2. Lancer Airflow
docker compose -f docker-compose-airflow.yaml up -d --build

# 3. Interface Airflow : http://localhost:8080
#    DAGs : load_olist_to_dw, fetch_exchange_rate, run_dbt_transformations

# 4. Exécuter manuellement les transformations et tests dbt (optionnel,
#    sinon automatique via run_dbt_transformations)
docker exec -it dbt dbt run
docker exec -it dbt dbt test

# 5. Documentation dbt (lineage)
docker exec -it dbt dbt docs generate
docker exec -it dbt dbt docs serve --port 8080   # accessible sur localhost:8085
```

## Qualité des données

- **11 modèles dbt** (6 staging, 3 intermediate, 1 mart, 1 taux de change)
- **30+ tests automatisés** : `unique`, `not_null`, `accepted_values`, `relationships`
- Documentation et lineage générés automatiquement via `dbt docs`

## CI/CD

À chaque `git push`, GitHub Actions exécute automatiquement `dbt seed`, `dbt run` et
`dbt test` sur un environnement PostgreSQL temporaire, pour garantir qu'aucune
modification ne casse le pipeline. Voir `.github/workflows/dbt_ci.yml`.

## Supervision

Un callback Airflow (`on_failure_callback`) journalise toute tâche en échec, et un
envoi d'email automatique (`email_on_failure`) notifie immédiatement en cas d'échec
définitif, après épuisement des tentatives automatiques.

## Dashboard

Un dashboard Power BI (`powerbi/Dashboard_DataOps_Olist.pbix`) se connecte à la table
`mart_sales_summary` et présente : le chiffre d'affaires total (en BRL et en USD via
conversion dynamique), la répartition par État, les moyens de paiement, et le délai de
livraison moyen.

## Statut du projet

Projet finalisé — stage PFA, Zenithsoft (2026).

## Auteure

Meryem Mouguert — 2ᵉ année Ingénieur, Transformation Digitale Industrielle (TDI),
ENSA Béni Mellal.