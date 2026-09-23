from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    rand, randn, when, expr, concat, lit, floor, round,
    to_date, date_sub, current_date, col
)
from pyspark.sql.types import *
from datetime import date

spark = (
    SparkSession.builder
    .appName("HealthcareClaims-Generate-1M")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

N = 1_000_000

# Generate deterministic-style synthetic IDs and healthcare attributes.
df = spark.range(0, N).withColumnRenamed("id", "record_id")

df = (
    df
    .withColumn("patient_id", concat(lit("PAT"), expr("lpad(cast(floor(rand(7) * 100000) as string), 6, '0')")))
    .withColumn("provider_id", concat(lit("PRV"), expr("lpad(cast(floor(rand(11) * 5000) as string), 5, '0')")))
    .withColumn("claim_id", concat(lit("CLM"), expr("lpad(cast(record_id + 1 as string), 10, '0')")))
    .withColumn(
        "claim_date",
        date_sub(current_date(), floor(rand(17) * 730).cast("int"))
    )
    .withColumn(
        "admission_date",
        date_sub(current_date(), floor(rand(19) * 730).cast("int"))
    )
    .withColumn(
        "claim_type",
        when(rand(23) < 0.35, "Inpatient")
        .when(rand(23) < 0.70, "Outpatient")
        .when(rand(23) < 0.88, "Emergency")
        .otherwise("Pharmacy")
    )
    .withColumn(
        "diagnosis_group",
        expr("""
            element_at(
                array('Cardiology','Orthopedics','Oncology','Neurology',
                      'Diabetes','Respiratory','Gastroenterology','General'),
                cast(floor(rand(29) * 8) + 1 as int)
            )
        """)
    )
    .withColumn(
        "claim_status",
        when(rand(31) < 0.62, "Paid")
        .when(rand(31) < 0.78, "Pending")
        .when(rand(31) < 0.93, "Denied")
        .otherwise("Partially Paid")
    )
    .withColumn(
        "denial_reason",
        when(col("claim_status") != "Denied", lit(None).cast("string"))
        .when(rand(37) < 0.30, "Authorization")
        .when(rand(37) < 0.55, "Eligibility")
        .when(rand(37) < 0.75, "Coding")
        .when(rand(37) < 0.90, "Duplicate")
        .otherwise("Medical Necessity")
    )
    .withColumn(
        "billed_amount",
        round(expr("50 + abs(randn(41)) * 1500"), 2)
    )
    .withColumn(
        "processing_days",
        floor(rand(43) * 31).cast("int")
    )
    .withColumn(
        "patient_age",
        floor(18 + rand(47) * 82).cast("int")
    )
)

df = (
    df
    .withColumn(
        "allowed_amount",
        round(col("billed_amount") * (0.55 + rand(53) * 0.40), 2)
    )
    .withColumn(
        "paid_amount",
        when(col("claim_status") == "Paid", col("allowed_amount"))
        .when(col("claim_status") == "Partially Paid",
              round(col("allowed_amount") * (0.25 + rand(59) * 0.50), 2))
        .otherwise(lit(0.0))
    )
    .withColumn(
        "patient_responsibility",
        round(col("allowed_amount") * (0.05 + rand(61) * 0.20), 2)
    )
    .withColumn(
        "outstanding_amount",
        round(col("billed_amount") - col("paid_amount"), 2)
    )
    .select(
        "record_id", "claim_id", "patient_id", "provider_id",
        "claim_date", "admission_date", "claim_type",
        "diagnosis_group", "claim_status", "denial_reason",
        "patient_age", "billed_amount", "allowed_amount",
        "paid_amount", "patient_responsibility",
        "outstanding_amount", "processing_days"
    )
)

output = "data/output/raw_claims"

(
    df.repartition(20)
      .write
      .mode("overwrite")
      .parquet(output)
)

print(f"Generated {N:,} synthetic healthcare claim records.")
print(f"Output: {output}")

spark.stop()
