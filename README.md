# airflow-data-quality-pipeline


## Overview

This project demonstrates an automated data quality pipeline built with Apache Airflow.

The pipeline downloads CSV files from an FTP server, validates flight data using business rules, and automatically uploads processed reports to Slack.

---

## Features

- Dynamic file download from FTP
- Apache Airflow DAG scheduling
- Automated data quality validation
- Route validation using airport reference data
- Country route validation (Domestic or International) with Pandas merge()
- Missing value detection
- Zero-value detection
- Duplicate detection
- Business rule validation
- Slack API integration
- Automatic logging
- Retry mechanism for FTP failures

---

## Technologies

- Python
- Apache Airflow
- Pandas
- NumPy
- FTP
- Slack API
- Docker

---

## Pipeline

FTP
↓
Download CSV
↓
Validate data
↓
Generate checked reports
↓
Upload to Slack

---

## Data Quality Rules

- Missing values
- Empty strings
- Zero values
- Route validation
- Country validation
- Search class validation
- Price validation
- Duplicate detection

---

## Project Structure

dags/
flight_data_quality_pipeline.py

data/
Airports_code.csv

screenshots/
Airflow DAG Graph
Slack notification

README.md

---

## Author

Inna Antoniuk

