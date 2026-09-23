# Interview Notes — Healthcare PySpark Project

## 1. Why PySpark?

PySpark is suitable because the project processes 1 million records and the same transformation pattern can scale horizontally across Spark executors.

## 2. How did you handle duplicates?

I used a window partitioned by `claim_id` and assigned `row_number()`. Only the first record was retained.

## 3. How did you handle data quality?

I created a `data_quality_status` column and validated claim ID, patient ID, provider ID, and financial values.

## 4. How did you calculate denial rate?

```text
denied_claims / total_claims * 100
```

The calculation was performed at provider level.

## 5. Where did you use window functions?

Window functions were used for patient-level and provider-level metrics without collapsing the underlying claim records.

Examples:

- patient claim count
- patient total billed
- provider claim count
- provider denial count

## 6. How did you optimize Spark?

I used:

- Adaptive Query Execution
- controlled shuffle partitions
- repartitioning on useful dimensions
- Parquet instead of CSV for processed data
- partitioned output
- avoiding unnecessary wide transformations

## 7. Why Parquet?

Parquet is columnar, supports predicate pushdown and compression, and is generally more suitable than CSV for analytical Spark workloads.

## 8. What makes this healthcare-specific?

The business logic models:

- claims
- providers
- patients
- diagnoses
- denials
- reimbursement
- patient responsibility
- outstanding amounts
- claim processing delays
- utilization

## 9. Is this real patient data?

No. All data is synthetically generated for portfolio and interview purposes.

## 10. Strong interview summary

"I designed a scalable PySpark healthcare claims pipeline that generates one million synthetic records and processes them through data-quality, cleansing, enrichment and analytics stages. I used window functions for patient and provider metrics, implemented denial and financial business rules, calculated risk flags, and created Silver and Gold datasets in Parquet. I also used Spark partitioning and adaptive query execution to make the pipeline scalable."
