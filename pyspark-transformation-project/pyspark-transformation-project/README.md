# PySpark Data Transformation Project

## Project Overview

This project demonstrates an end-to-end **PySpark data transformation pipeline** using a retail sales dataset.

The pipeline reads raw CSV data, performs data cleansing and transformation, creates derived business columns, removes duplicates, filters invalid records, and writes a transformed dataset for analytics.

## Architecture

```text
Raw CSV
   |
   v
Read with PySpark
   |
   v
Data Cleaning
   |
   +--> Handle nulls
   +--> Remove duplicates
   +--> Cast data types
   |
   v
Transformations
   |
   +--> Calculate total_amount
   +--> Create order_year / order_month
   +--> Standardize city/category
   +--> Filter cancelled orders
   |
   v
Aggregations
   |
   +--> Category sales
   +--> City sales
   |
   v
Output CSV
```

## Transformations Covered

- Reading CSV using Spark DataFrame API
- Explicit schema definition
- `withColumn()`
- `cast()`
- `when()`
- `trim()`
- `upper()`
- `to_date()`
- `year()` and `month()`
- `dropDuplicates()`
- `filter()`
- `fillna()`
- `groupBy()` and `agg()`
- `sum()`, `count()`, `avg()`
- Window-free business transformations
- Writing transformed data to CSV

## Project Structure

```text
pyspark-transformation-project/
│
├── data/
│   ├── input/
│   │   └── customer_sales.csv
│   └── output/
│
├── src/
│   └── transformation_pipeline.py
│
├── requirements.txt
└── README.md
```

## How to Run

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the PySpark pipeline

```bash
spark-submit src/transformation_pipeline.py
```

### 3. Output

The pipeline creates:

- `data/output/transformed_sales/`
- `data/output/category_sales/`
- `data/output/city_sales/`

## Business Logic

`total_amount = unit_price × quantity`

Only orders with status other than `Cancelled` are included in the final sales analysis.

The pipeline also creates:

- `order_year`
- `order_month`
- `total_amount`
- `order_status`

## Skills Demonstrated

**PySpark | Python | SQL-style transformations | ETL | Data Cleaning | Data Transformation | Aggregation | Data Quality**

## Resume / Interview Description

> Built an end-to-end PySpark transformation pipeline to process retail sales data, perform data cleansing, type conversion, derived-column creation, duplicate handling, filtering, and aggregations. Generated analytical datasets by category and city for downstream reporting.

