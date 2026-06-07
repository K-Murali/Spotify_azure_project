# Spotify_azure_project

## Pipeline Architecture

![Pipeline Architecture](./assets/Untitled%20Diagram4.png)

The pipeline follows a medallion architecture pattern with the following stages:
- **Ingestion Pipeline**: Incremental loading, back-filling, and alerts
- **Unity Catalog**: Centralized data catalog which manages tables and metadata of external tables  
- **Bronze Layer (L1)**: Data lake ingestion using autoloader with incremental processing and added audit columns
- **Silver Layer (L2)**: Data quality management (DQMs), de-duplication, and SCD 1,2 implementation
- **Gold Layer (L3)**: Aggregated data with KPI tables and business views for reporting
- **Reporting & Delta Sharing**: Dashboards and delta sharing capabilities
