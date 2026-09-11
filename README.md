# Credit Risk & Insurance Lakehouse Modernization

[![CI](https://github.com/OdysseasGlenis/Credit-risk-insurance-lakehouse-fabric/actions/workflows/ci.yml/badge.svg)](https://github.com/OdysseasGlenis/Credit-risk-insurance-lakehouse-fabric/actions/workflows/ci.yml)

End-to-end local PySpark lakehouse project for synthetic banking and insurance data, designed to simulate a modern data engineering pipeline using Bronze, Silver and Gold layers.

The project demonstrates ingestion, cleansing, deduplication, feature engineering, customer-level analytical marts, rule-based risk scoring, data quality monitoring, automated tests and Power BI-ready exports. It is implemented locally with PySpark and Parquet, but the architecture is portable to platforms such as Microsoft Fabric or Databricks.

---

## Business Context

Financial institutions often need to modernize fragmented operational data into curated analytical datasets that support credit risk monitoring, customer analytics, BI reporting and downstream machine learning use cases.

This project simulates that scenario using synthetic data from multiple banking and insurance domains:

- Customers
- Accounts
- Credit cards
- Transactions
- Loans
- Insurance claims
- Customer notes
- Reference mappings

The objective is to transform raw operational-style data into reliable, business-ready customer-level datasets.

---

## Architecture

```text
data/raw + data/reference
        |
        v
Bronze Layer
Raw ingestion with technical metadata
        |
        v
Silver Layer
Cleansing, casting, deduplication, JSON parsing and business flags
        |
        v
Gold Layer
Customer 360, transaction KPIs, loan behavior, card features, claims metrics and risk features
        |
        v
Monitoring Layer
Data quality checks, reconciliation and pipeline run logging
        |
        v
Power BI Export
CSV datasets for local reporting/dashboard use
```

---

## Tech Stack

- Python
- PySpark
- Parquet
- pytest
- GitHub Actions
- Git / GitHub
- VS Code
- Local lakehouse architecture
- Data quality monitoring
- Bronze / Silver / Gold data modeling
- Power BI-ready CSV exports

---

## Project Structure

```text
Credit-risk-insurance-lakehouse-fabric/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── data/
│   ├── raw/
│   └── reference/
│
├── data_generator/
│   └── generate_synthetic_data.py
│
├── docs/
│
├── fabric_notebooks/
│
├── pipelines/
│
├── sql/
│
├── src/
│   ├── common/
│   │   └── spark_session.py
│   │
│   └── local_pipeline/
│       ├── bronze_ingestion.py
│       ├── silver_transformations.py
│       ├── gold_features.py
│       ├── data_quality_checks.py
│       ├── export_powerbi_datasets.py
│       └── run_all.py
│
├── tests/
│   ├── test_data_quality_checks.py
│   ├── test_gold_features.py
│   └── test_silver_transformations.py
│
├── pytest.ini
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Pipeline Layers

### Bronze Layer

The Bronze layer ingests raw CSV and JSON files into Parquet format while preserving the source structure.

It adds technical metadata columns such as:

- `ingestion_timestamp`
- `source_file`
- `batch_id`
- `record_hash`

Main script:

```text
src/local_pipeline/bronze_ingestion.py
```

Example outputs:

```text
lakehouse/bronze/bronze_customers
lakehouse/bronze/bronze_transactions
lakehouse/bronze/bronze_loans
lakehouse/bronze/bronze_claims
```

---

### Silver Layer

The Silver layer standardizes and cleans the data.

Implemented transformations include:

- Type casting
- Date parsing
- Text standardization
- Transaction deduplication
- JSON field extraction
- Orphan customer flagging
- Suspicious transaction amount flagging
- Business rule preparation

Main script:

```text
src/local_pipeline/silver_transformations.py
```

Example outputs:

```text
lakehouse/silver/silver_customers
lakehouse/silver/silver_transactions
lakehouse/silver/silver_loans
lakehouse/silver/silver_claims
```

---

### Gold Layer

The Gold layer creates business-ready analytical marts.

Generated datasets include:

- `gold_monthly_transaction_kpis`
- `gold_credit_card_features`
- `gold_loan_payment_behavior`
- `gold_insurance_claims_mart`
- `gold_customer_note_features`
- `gold_customer_360`
- `gold_customer_risk_features`

Main script:

```text
src/local_pipeline/gold_features.py
```

The most important outputs are customer-level marts, where each row represents one customer enriched with behavioral, financial and risk-related features.

---

## Customer 360

The `gold_customer_360` dataset combines customer information with features from accounts, cards, loans, insurance claims, customer notes and transactions.

Example feature groups:

- Demographics
- Account balances
- Active cards
- Credit limits
- Loan exposure
- Delinquency indicators
- Claims behavior
- Customer note flags
- Transaction spend over rolling windows

---

## Risk Features

The `gold_customer_risk_features` dataset extends the Customer 360 mart with a rule-based risk scoring approach.

Example fields:

- `risk_score`
- `risk_band`
- `high_risk_customer_flag`

The risk score uses indicators such as:

- Missing declared income
- Financial stress notes
- Fraud alerts
- Loan delinquency
- Default flags
- Suspicious transactions
- High credit utilization proxy
- High debt-to-income proxy
- High-severity insurance claims

---

## Data Quality & Monitoring

The monitoring layer validates data quality across Bronze, Silver and Gold.

Implemented checks include:

- Minimum row counts
- Null or blank key checks
- Duplicate key checks
- Referential integrity checks
- Orphan record detection
- Suspicious transaction monitoring
- Bronze-to-Silver reconciliation
- Gold customer count reconciliation
- Risk score validity
- Risk band validity

Main script:

```text
src/local_pipeline/data_quality_checks.py
```

Monitoring outputs:

```text
lakehouse/monitoring/dq_results
lakehouse/monitoring/pipeline_run_log
```

Example final status:

```text
PASS: 22
WARN: 3
FAIL: 0

Pipeline status: SUCCESS_WITH_WARNINGS
```

Warnings are expected in this demo project because the synthetic dataset intentionally contains controlled data quality issues, such as duplicate transactions, orphan transactions and suspicious amounts.

---

## Testing and CI

The project includes lightweight pytest-based unit tests for critical PySpark transformation logic.

The tests validate:

- Silver transaction deduplication
- Orphan customer flagging
- Suspicious amount flagging
- Gold risk score validity
- Risk band validity
- DQ results creation

Run tests locally:

```powershell
python -m pytest tests -q
```

The repository also includes a GitHub Actions CI workflow that runs the tests automatically on pushes and pull requests to `main`.

Workflow file:

```text
.github/workflows/ci.yml
```

---

## Power BI Export

The project includes a Power BI export script that converts selected Gold and Monitoring Parquet outputs into CSV files.

Main script:

```text
src/local_pipeline/export_powerbi_datasets.py
```

Run the export:

```powershell
python -m src.local_pipeline.export_powerbi_datasets
```

Generated local CSV outputs:

```text
exports/powerbi/customer_360.csv
exports/powerbi/customer_risk_features.csv
exports/powerbi/monthly_transaction_kpis.csv
exports/powerbi/loan_payment_behavior.csv
exports/powerbi/insurance_claims_mart.csv
exports/powerbi/dq_results.csv
exports/powerbi/pipeline_run_log.csv
```

The `exports/` directory is excluded from Git because it contains generated local outputs.

---

## How to Run

Create and activate a Python virtual environment, then install dependencies:

```powershell
pip install -r requirements.txt
```

Run the full pipeline:

```powershell
python -m src.local_pipeline.run_all
```

This executes:

```text
Bronze ingestion
Silver transformations
Gold feature generation
Data quality and monitoring
```

Generated outputs are written locally under:

```text
lakehouse/
```

The `lakehouse/` directory is intentionally excluded from Git because it contains generated data outputs.

---

## Example Run Output

```text
Starting step: Bronze ingestion
Step completed: Bronze ingestion

Starting step: Silver transformations
Step completed: Silver transformations

Starting step: Gold feature generation
Step completed: Gold feature generation

Starting step: Data quality and monitoring

DQ summary:
PASS 22
WARN 3

Pipeline completed successfully.
```

---

## Why This Project Matters

This project demonstrates practical data engineering capabilities beyond isolated scripts:

- Building layered lakehouse pipelines
- Designing reusable PySpark transformations
- Creating analytical data models
- Implementing data quality controls
- Performing reconciliation across layers
- Producing business-ready customer-level datasets
- Adding automated tests for core transformation logic
- Running CI checks through GitHub Actions
- Preparing curated datasets for Power BI reporting
- Structuring a project for GitHub portfolio visibility

The architecture can be extended to cloud platforms such as Microsoft Fabric or Databricks by replacing local Parquet paths with managed lakehouse storage and orchestration services.

---

## Potential Future Improvements

- Add Delta Lake support
- Add incremental loading logic
- Add partitioning by `year_month`
- Add Power BI dashboard examples
- Add Microsoft Fabric deployment version
- Add Databricks notebook version
- Add Great Expectations-style validation
- Add parameterized environment configuration
- Add richer logging and pipeline metrics