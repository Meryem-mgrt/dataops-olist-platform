# DataOps Platform — Olist E-Commerce

Plateforme DataOps pour l'intégration, la qualité et la gouvernance des données,
construite avec Apache Airflow, dbt et PostgreSQL.

## Stack technique
- Orchestration : Apache Airflow
- Transformation & qualité : dbt
- Stockage : PostgreSQL
- Conteneurisation : Docker / Docker Compose

## Données
Dataset : [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
À télécharger et placer dans `data/raw/`.

## Statut du projet
En cours de développement — stage PFA (Zenithsoft, 2026)

## Dashboard
Un dashboard Power BI (`powerbi/Dashboard_DataOps_Olist.pbix`) se connecte à la table
`mart_sales_summary` et présente : le chiffre d'affaires total, la répartition par État,
les moyens de paiement, et le délai de livraison moyen.