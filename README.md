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

## ADF Incremental Data Ingestion

![ADF Incremental Data Ingestion](./assets/Screenshot%202026-06-07%20214308.png)

The Azure Data Factory (ADF) pipeline handles incremental data ingestion from Spotify APIs with optimized load patterns and watermark management for efficient data synchronization.

## Job Pipeline

![Job Pipeline](./assets/Screenshot%202026-06-07%20215352.png)

The Job Pipeline orchestrates the end-to-end data processing workflow, coordinating data movement through the medallion layers and ensuring data quality and consistency across all transformation stages.
