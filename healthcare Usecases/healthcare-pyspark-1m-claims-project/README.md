# Healthcare Claims Analytics | PySpark

## Project Overview

A hard-level **PySpark healthcare data engineering project** that generates and processes **1,000,000 synthetic healthcare claim records**.

> The data is synthetic and contains no real patient information.

The project demonstrates production-style transformations used in healthcare claims analytics:

**Raw Claims → Data Quality → Silver Claims → Business Rules → Window Functions → Provider/Patient Metrics → Gold Analytics**

## Business Problem

Healthcare organizations need to analyze claims to identify:

- denied and paid claims
- high-cost claims
- outstanding reimbursement
- patient utilization
- provider performance
- duplicate claims
- claim processing delays
- denial reasons
- readmission-like utilization patterns

## Architecture

```text
                 Synthetic 1M Claims
                         |
                         v
                 Bronze / Raw Data
                         |
                         v
              Data Quality & Cleaning
             /          |           \
        null checks   duplicates   type checks
             \          |           /
                         v
                  Silver Claims
                         |
          +--------------+--------------+
          |              |              |
          v              v              v
     Claim Metrics   Patient Metrics Provider Metrics
          |              |              |
          +--------------+--------------+
                         |
                         v
                   Gold Analytics
```

## Hard-Level Transformations

This project intentionally covers interview-level PySpark concepts:

- 1,000,000 synthetic records
- explicit schema
- null handling
- duplicate detection
- data quality flags
- date parsing
- conditional business logic
- claim status normalization
- denial classification
- financial calculations
- `groupBy` aggregations
- joins
- window functions
- patient-level ranking
- provider-level metrics
- rolling 30-day claim metrics
- high-cost claim identification
- partition-aware output
- Spark configuration for large data
- `repartition` / `coalesce`
- output partitioning
- caching where appropriate

## Key Healthcare Metrics

### Claim Financial Metrics

```text
billed_amount
allowed_amount
paid_amount
patient_responsibility
outstanding_amount
```

### Claim Processing Metrics

```text
processing_days
claim_status
denial_category
```

### Utilization Metrics

```text
patient_total_claims
patient_total_paid
patient_average_claim
provider_claim_count
provider_denial_rate
```

### Risk / Flagging Rules

The pipeline creates flags for:

- high-cost claims
- duplicate claims
- denied claims
- delayed claims
- high-utilization patients

## Project Structure

```text
healthcare-pyspark-1m-claims-project/
│
├── data/
│   ├── sample/
│   │   └── claims_sample.csv
│   └── output/
│
├── src/
│   ├── generate_1m_claims.py
│   └── healthcare_claims_pipeline.py
│
├── docs/
│   └── interview_notes.md
│
├── README.md
├── requirements.txt
└── .gitignore
```

## Step 1 — Generate 1 Million Records

```bash
pip install -r requirements.txt
```

Then:

```bash
spark-submit src/generate_1m_claims.py
```

The generator creates approximately:

```text
data/output/raw_claims/
```

with **1,000,000 synthetic claim records**.

## Step 2 — Run Transformation Pipeline

```bash
spark-submit src/healthcare_claims_pipeline.py
```

The pipeline creates:

```text
data/output/silver_claims/
data/output/gold_patient_metrics/
data/output/gold_provider_metrics/
data/output/gold_denial_analysis/
data/output/gold_high_cost_claims/
```

## Why the 1 Million Records Are Not Stored in GitHub

GitHub repositories should not contain the generated 1M-record output. Instead, this project contains a reproducible Spark generator.

Anyone can clone the repository and recreate the complete dataset by running:

```bash
spark-submit src/generate_1m_claims.py
```

This keeps the GitHub repository lightweight while still demonstrating a real large-scale processing workflow.

## Interview Explanation

> "I built a PySpark healthcare claims pipeline capable of generating and processing one million synthetic claim records. I used an explicit schema, performed data-quality validation and duplicate detection, standardized claim attributes, derived financial and processing metrics, classified denials, and used window functions to calculate patient-level and provider-level metrics. I separated the transformation flow into Silver and Gold datasets and partitioned the outputs for scalable downstream analytics."

## Technologies

- Python
- PySpark
- Apache Spark
- SQL-style DataFrame transformations
- Window Functions
- ETL / ELT
- Healthcare Claims Analytics
- Data Quality
- Data Engineering

## Important Note

This project uses **synthetic healthcare data only**. It is intended for learning, portfolio development, and interview demonstration.
