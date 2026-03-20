# Monash Microbiome Datalake

This folder contains components used for uploading and processing microbiome data to the datalake.

## Folder Overview

- `athena_db_sql_tables/` — SQL scripts to create tables in AWS Athena.
- `aws_glue_spark_scripts/` — PySpark scripts for AWS Glue jobs to process and write data to Iceberg tables.
- `data/` — Sample raw data used for testing.
- `streamlit_front_end/` — Streamlit web-app for uploading files and interacting with the data pipeline.

## How to Use

### 1. Deploy Glue Jobs
Upload the scripts in aws_glue_spark_scripts/ to AWS Glue, set input/output paths, and run the job.

### 2. Use Athena Tables
Run the SQL scripts in athena_db_sql_tables/ using the Athena console to query data stored in S3.

### 3. Run Streamlit App

```bash
cd streamlit_front_end
streamlit run app.py
```


