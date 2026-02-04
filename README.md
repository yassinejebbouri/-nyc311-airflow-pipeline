# NYC 311 Daily Data Engineering Pipeline

## Overview
This project implements an end-to-end data engineering pipeline that ingests
NYC 311 service request data from a public API, processes it daily using
Apache Airflow, stores aggregated results in PostgreSQL, and visualizes the
results through a Streamlit dashboard.

The goal of this project is to demonstrate core data engineering concepts such as:
- workflow orchestration
- API ingestion
- idempotent batch loading
- basic data quality validation
- analytical transformations

---

## Tech Stack
- **Apache Airflow** – workflow orchestration
- **PostgreSQL** – data storage
- **Streamlit** – interactive dashboard
- **Docker & Docker Compose** – local infrastructure
- **NYC Open Data (Socrata API)** – data source

---

## Architecture

NYC 311 API  
↓  
Airflow DAG (daily)  
↓  
PostgreSQL tables  
↓  
Streamlit dashboard  

---

## Airflow Pipeline

**DAG name:** `nyc311_daily_pipeline_with_spikes`

The DAG runs daily and always processes data for the **previous completed day**
to avoid partial or incomplete data.

### Tasks

#### create_tables
- Creates required PostgreSQL tables if they do not exist

#### extract_transform
- Fetches NYC 311 records for the previous day via the API
- Aggregates complaint counts by `borough` and `complaint_type`

#### load_daily_counts
- Loads aggregated results into PostgreSQL
- Uses date-based deletion to ensure idempotent loads

#### data_quality
- Verifies that data was loaded for the day
- Ensures required fields are not null

#### compute_spikes
- Compares daily complaint counts with the previous day
- Stores absolute and percentage changes

---

## Data Model

### nyc311_daily_borough_complaint
Stores daily aggregated complaint counts.

Columns:
- `load_day` (DATE)
- `borough` (TEXT)
- `complaint_type` (TEXT)
- `count` (BIGINT)

Primary Key:
- `(load_day, borough, complaint_type)`

---

### nyc311_spikes
Stores day-over-day changes in complaint counts.

Columns:
- `load_day` (DATE)
- `borough` (TEXT)
- `complaint_type` (TEXT)
- `count_today` (BIGINT)
- `count_yesterday` (BIGINT)
- `abs_change` (BIGINT)
- `pct_change` (DOUBLE PRECISION)

---

## Streamlit Dashboard
The Streamlit application connects directly to PostgreSQL and allows users to:
- Select a date to explore daily complaint data
- Filter results by borough
- View top complaint categories
- Inspect day-over-day complaint changes

The dashboard provides a simple interface for exploring the outputs of the
data pipeline.

---

## Running the Project Locally

### Prerequisites
- Docker
- Docker Compose

### Setup
```bash
git clone <repository-url>
cd <repository>
cp .env.example .env
docker compose up -d --build
```
# What This Project Demonstrates: 
-Orchestrating batch data workflows with Airflow
-Ingesting data from a public API
-Performing daily aggregations
-Designing idempotent database loads
-Running basic data quality checks
-Making processed data accessible via a simple UI
