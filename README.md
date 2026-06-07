# Spotify_azure_project

## Pipeline Architecture

![Pipeline Architecture](./assets/Untitled%20Diagram4.png)

The pipeline follows a medallion architecture pattern with the following stages:
- **Ingestion Pipeline**: Incremental loading, back-filling, and alerts
- **Unity Catalog**: Centralized data catalog with Parquet, Bronze-Delta, Silver-Delta, and Gold-Delta tables
- **Bronze Layer (L1)**: Data lake ingestion using autoloader with incremental processing and audit columns
- **Silver Layer (L2)**: Data quality management (DQMs), de-duplication, and SCD 1.2 implementation
- **Gold Layer (L3)**: Aggregated data with KPI tables and business views for reporting
- **Reporting & Delta Sharing**: Dashboards and delta sharing capabilities

## Language Composition

This repository is composed of the following programming languages:

| Language | Percentage |
|----------|-----------|
| Jupyter Notebook | 73.4% |
| Python | 26.6% |
