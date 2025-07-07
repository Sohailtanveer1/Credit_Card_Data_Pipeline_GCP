# 💳 Credit Data Pipeline on GCP

This repository outlines a modern data engineering pipeline on **Google Cloud Platform (GCP)** to ingest, transform, analyze, and visualize credit-related data using **Apache Spark**, **Cloud Composer**, **Cloud Storage**, **BigQuery**, and **Power BI**.

---

## 📌 Architecture Overview

![Credit Data Pipeline Architecture]
![image](https://github.com/user-attachments/assets/2a4aa98e-712c-41b9-be42-3867839faed6)

---

## 📂 Components and Workflow

### 1. 📥 Landing Zone (Raw CSVs)

- All incoming credit data is delivered in CSV format to a **GCS bucket** (landing zone).
- These raw CSV files are unprocessed and may contain missing or inconsistent data.

---

### 2. 🛠️ Bronze Layer (Raw Ingestion with Metadata)

- **Spark Job #1** (triggered by **Cloud Composer**):
  - Reads CSV files from the landing zone.
  - Adds three metadata columns:
    - `ingestion_timestamp`: When the file was processed.
    - `source_file`: The filename of the CSV.
    - `source`: System or provider info.
  - Saves the enriched data as **Parquet files** in the `cred_raw` GCS bucket (Bronze Layer).
  
✅ Format: Columnar (Parquet)  
🎯 Objective: Create traceable and query-optimized raw data for downstream processing.

---

### 3. 🧹 Silver Layer (Cleaned Data)

- **Spark Job #2** (triggered by **Cloud Composer**):
  - Reads data from the Bronze Layer.
  - Performs **data cleaning**, including:
    - Column renaming based on config.
    - Type casting (e.g., string to `DateType`).
    - Dropping null or corrupt rows.
  - Saves the cleaned data as Parquet files in the `cred_silver` GCS bucket (Silver Layer).

🎯 Objective: Make the data analysis-ready, standardized, and consistent.

---

### 4. 📊 Gold Layer (Aggregated Analytics Results)

- **Spark Notebook** (triggered by **Cloud Composer**):
  - Reads cleaned data from the Silver Layer.
  - Executes **10+ analytical computations**, such as:
    - Average credit score by region.
    - Default rates by income category.
    - Monthly trend analysis.
    - Loan approval prediction breakdowns.
  - Saves the result in the `cred_gold` bucket (Gold Layer) as Parquet files.

🎯 Objective: Provide ready-to-consume analytical outputs for visualization and business users.

---

### 5. 🧾 BigQuery Integration

- A final Spark job reads data from the Gold Layer.
- Uses JSON configuration files from `cred_config` bucket to:
  - Define BigQuery table schema.
  - Create or update tables programmatically.
- Data is written into **BigQuery** for fast SQL-based querying.

🎯 Objective: Enable scalable, federated analytics via BigQuery.

---

### 6. 📅 Job Orchestration via Cloud Composer

- All Spark jobs and notebooks are triggered and orchestrated via **Cloud Composer (Apache Airflow)**.
- DAGs handle:
  - Time-based scheduling.
  - Dependency resolution.
  - Failure alerts & retries.

🎯 Objective: Fully automated, repeatable pipeline.

---

### 7. 📈 Visualization in Power BI

- Final BigQuery tables are connected to **Power BI** dashboards.
- Business users can run reports and queries in near real-time.

---

## 🧾 Configuration

All Spark job logic and paths are dynamically controlled via **config JSON files** stored in the `cred_config` GCS bucket. These include:

- Input/output paths
- Column transformation rules
- Table definitions for BigQuery
- Data validation parameters

---

## 📌 Benefits

| Benefit                    | Description                                                                 |
|---------------------------|-----------------------------------------------------------------------------|
| 🔁 End-to-End Automation   | Fully automated from raw CSV ingestion to Power BI dashboards.             |
| 🔍 Traceability           | Metadata columns and standardized storage layers ensure data lineage.       |
| 📈 Scalable Analytics     | BigQuery and Spark scale with volume and complexity of credit data.         |
| ⚙️ Config Driven          | Easily maintain or modify pipeline without changing core Spark logic.       |
| ⏱️ Scheduled & Resilient  | Cloud Composer ensures scheduling and fault tolerance across the pipeline. |
