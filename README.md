![dbt CI](https://github.com/Meryem-mgrt/dataops-olist-platform/actions/workflows/dbt_ci.yml/badge.svg)

# DataOps Platform — Olist E-Commerce

Plateforme DataOps pour l'intégration, la qualité et la gouvernance des données,
construite avec Apache Airflow, dbt et PostgreSQL, dans le cadre d'un stage PFA
chez Zenithsoft.

## Contexte

Ce projet met en place une chaîne DataOps complète simulant un cas d'usage e-commerce : ingestion automatisée de données réparties sur plusieurs tables liées (clients, commandes, paiements, produits, vendeurs), transformation et contrôle qualité, gouvernance (documentation et traçabilité), et restitution via un dashboard.

## Architecture

CSV Olist (6 fichiers)
│
▼
Apache Airflow ──► PostgreSQL (entrepôt dédié)
│
▼
dbt (staging → intermediate → marts)
│
▼
Power BI

Chaque brique tourne dans un conteneur Docker indépendant.

## Stack technique

| Composant | Outil | Rôle |

| Orchestration | Apache Airflow | Planification et exécution des pipelines d'ingestion |
| Transformation & qualité | dbt | Modélisation en couches, tests de qualité, documentation |
| Stockage | PostgreSQL | Entrepôt de données dédié ('postgres_dw') |
| Conteneurisation | Docker / Docker Compose | Environnement reproductible |
| CI/CD | GitHub Actions | Exécution automatique de 'dbt run' / 'dbt test' à chaque push |
| Restitution | Power BI | Dashboard connecté à la table finale |

## Données

Dataset : [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
(dataset public, utilisé en l'absence de données réelles d'entreprise).

À télécharger et placer dans 'data/raw/' :
- 'olist_customers_dataset.csv
- 'olist_orders_dataset.csv'
- 'olist_order_items_dataset.csv'
- 'olist_order_payments_dataset.csv'
- 'olist_products_dataset.csv'
- 'olist_sellers_dataset.csv'

## Structure du projet

├── dags/ # DAG Airflow d'ingestion
├── data/raw/ # CSV Olist (non versionnés, voir .gitignore)
├── dbt/
│ ├── models/
│ │ ├── staging/ # Nettoyage, 1 modèle par table brute
│ │ ├── intermediate/ # Agrégations et jointures
│ │ └── marts/ # Table finale : mart_sales_summary
│ └── seeds/ # Échantillons de données pour les tests CI
├── powerbi/ # Dashboard Power BI (.pbix)
├── .github/workflows/ # Pipeline CI/CD (GitHub Actions)
├── docker-compose.yml # PostgreSQL (entrepôt) + dbt
└── docker-compose-airflow.yaml # Apache Airflow


## Démarrage

```bash
# 1. Lancer l'entrepôt de données et dbt
docker compose -f docker-compose.yml up -d --build

# 2. Lancer Airflow
docker compose -f docker-compose-airflow.yaml up -d

# 3. Déclencher l'ingestion (interface Airflow sur localhost:8080)
#    DAG : load_olist_to_dw

# 4. Exécuter les transformations et tests dbt
docker exec -it dbt dbt run
docker exec -it dbt dbt test

# 5. Générer la documentation dbt (lineage)
docker exec -it dbt dbt docs generate
docker exec -it dbt dbt docs serve --port 8080   # accessible sur localhost:8085
```

## Qualité des données

- **10 modèles dbt** (6 staging, 3 intermediate, 1 mart)
- **30 tests automatisés** : `unique`, `not_null`, `accepted_values`, `relationships`
- Documentation et lineage générés automatiquement via `dbt docs`

## CI/CD

À chaque `git push`, GitHub Actions exécute automatiquement `dbt seed`, `dbt run` et
`dbt test` sur un environnement PostgreSQL temporaire, pour garantir qu'aucune
modification ne casse le pipeline. Voir `.github/workflows/dbt_ci.yml`.

## Dashboard

Un dashboard Power BI (`powerbi/Dashboard_DataOps_Olist.pbix`) se connecte à la table
`mart_sales_summary` et présente : le chiffre d'affaires total, la répartition par État,
les moyens de paiement, et le délai de livraison moyen.

## Auteure

Meryem Mouguert — 2ème année Ingénieur, Transformation Digitale Industrielle (TDI),
ENSA Béni Mellal.