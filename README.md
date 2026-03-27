# ETL Pipeline with Apache Airflow

This project demonstrates a simple ETL pipeline built using Apache Airflow and Python to process raw data files and prepare them for analytics.

The pipeline automates the Extract → Transform → Load workflow using Airflow operators.

---

## Project Overview

The goal of this project is to simulate a real data engineering workflow where raw log / CSV data is processed automatically and prepared for reporting and analysis.

The pipeline performs the following steps:

1. Download raw data file
2. Transform the data using Python
3. Save the cleaned data to a new file
4. Move the processed data for further use

---

## Airflow DAG Workflow

The DAG contains three tasks:

- download_data → BashOperator
- transform → PythonOperator
- copy → BashOperator

Workflow order:

download_data >> transform >> copy

---

## Technologies Used

- Python
- Apache Airflow
- CSV files
- ETL pipeline design
- BashOperator
- PythonOperator

---

## Files

people_dag.py → Main Airflow DAG  
README.md → Documentation  
Input CSV file → Raw data source  

---

## What I learned

- Building ETL workflows with Airflow
- Writing DAGs in Python
- Automating data pipelines
- Transforming CSV data programmatically
- Structuring data for analytics

---

## Future Improvements

- Load data into PostgreSQL
- Add logging and error handling
- Use environment variables
- Add Power BI dashboard
- Create full data warehouse pipeline

---

## Author

Sarraj Alsammak  
MSc Data Science & Engineering
