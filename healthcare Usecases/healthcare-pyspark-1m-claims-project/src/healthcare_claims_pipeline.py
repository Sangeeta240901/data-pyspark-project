from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lit, upper, trim, round, sum, avg, count,
    row_number, desc, datediff, to_date, max as spark_max
)

spark = (
    SparkSession.builder
    .appName("HealthcareClaims-Transformation")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

INPUT = "data/output/raw_claims"

# ---------------------------------------------------------
# 1. Read Bronze / Raw data
# ---------------------------------------------------------
raw = spark.read.parquet(INPUT)

print("Raw record count:", raw.count())

# ---------------------------------------------------------
# 2. Data quality and duplicate detection
# ---------------------------------------------------------
duplicate_window = Window.partitionBy("claim_id").orderBy(desc("record_id"))

silver = (
    raw
    .withColumn(
        "duplicate_rank",
        row_number().over(duplicate_window)
    )
    .withColumn(
        "data_quality_status",
        when(col("claim_id").isNull(), "INVALID_CLAIM_ID")
        .when(col("patient_id").isNull(), "INVALID_PATIENT")
        .when(col("provider_id").isNull(), "INVALID_PROVIDER")
        .when(col("billed_amount") < 0, "INVALID_AMOUNT")
        .otherwise("VALID")
    )
    .filter(col("duplicate_rank") == 1)
    .drop("duplicate_rank")
)

# ---------------------------------------------------------
# 3. Standardize healthcare fields
# ---------------------------------------------------------
silver = (
    silver
    .withColumn("claim_type", upper(trim(col("claim_type"))))
    .withColumn("claim_status", upper(trim(col("claim_status"))))
    .withColumn("diagnosis_group", upper(trim(col("diagnosis_group"))))
    .withColumn(
        "denial_category",
        when(col("claim_status") != "DENIED", "NOT_DENIED")
        .when(col("denial_reason") == "AUTHORIZATION", "AUTHORIZATION")
        .when(col("denial_reason") == "ELIGIBILITY", "ELIGIBILITY")
        .when(col("denial_reason") == "CODING", "CODING")
        .when(col("denial_reason") == "DUPLICATE", "DUPLICATE")
        .otherwise("MEDICAL_NECESSITY")
    )
    .withColumn(
        "high_cost_flag",
        when(col("billed_amount") >= 5000, "HIGH_COST").otherwise("NORMAL")
    )
    .withColumn(
        "delayed_claim_flag",
        when(col("processing_days") > 15, "DELAYED").otherwise("NORMAL")
    )
)

# ---------------------------------------------------------
# 4. Patient-level window metrics
# ---------------------------------------------------------
patient_window = Window.partitionBy("patient_id")

patient_metrics = (
    silver
    .withColumn(
        "patient_total_claims",
        count("claim_id").over(patient_window)
    )
    .withColumn(
        "patient_total_billed",
        round(sum("billed_amount").over(patient_window), 2)
    )
    .withColumn(
        "patient_total_paid",
        round(sum("paid_amount").over(patient_window), 2)
    )
    .withColumn(
        "patient_avg_claim",
        round(avg("billed_amount").over(patient_window), 2)
    )
    .withColumn(
        "high_utilization_flag",
        when(col("patient_total_claims") >= 10, "HIGH_UTILIZATION")
        .otherwise("NORMAL")
    )
)

# ---------------------------------------------------------
# 5. Provider-level metrics
# ---------------------------------------------------------
provider_window = Window.partitionBy("provider_id")

provider_metrics = (
    patient_metrics
    .withColumn(
        "provider_claim_count",
        count("claim_id").over(provider_window)
    )
    .withColumn(
        "provider_total_billed",
        round(sum("billed_amount").over(provider_window), 2)
    )
    .withColumn(
        "provider_total_paid",
        round(sum("paid_amount").over(provider_window), 2)
    )
    .withColumn(
        "provider_denied_count",
        sum(
            when(col("claim_status") == "DENIED", 1).otherwise(0)
        ).over(provider_window)
    )
    .withColumn(
        "provider_denial_rate",
        round(
            col("provider_denied_count") /
            col("provider_claim_count") * 100,
            2
        )
    )
)

# ---------------------------------------------------------
# 6. Business risk score
# ---------------------------------------------------------
final_silver = (
    provider_metrics
    .withColumn(
        "risk_score",
        (
            when(col("high_cost_flag") == "HIGH_COST", 30).otherwise(0)
            + when(col("delayed_claim_flag") == "DELAYED", 20).otherwise(0)
            + when(col("claim_status") == "DENIED", 25).otherwise(0)
            + when(col("high_utilization_flag") == "HIGH_UTILIZATION", 25).otherwise(0)
        )
    )
    .withColumn(
        "risk_level",
        when(col("risk_score") >= 60, "HIGH")
        .when(col("risk_score") >= 30, "MEDIUM")
        .otherwise("LOW")
    )
)

# ---------------------------------------------------------
# 7. Gold Patient Metrics
# ---------------------------------------------------------
gold_patient = (
    final_silver
    .groupBy("patient_id")
    .agg(
        count("claim_id").alias("total_claims"),
        round(sum("billed_amount"), 2).alias("total_billed"),
        round(sum("paid_amount"), 2).alias("total_paid"),
        round(avg("billed_amount"), 2).alias("avg_claim_amount"),
        spark_max("risk_score").alias("max_risk_score")
    )
    .withColumn(
        "patient_risk_level",
        when(col("max_risk_score") >= 60, "HIGH")
        .when(col("max_risk_score") >= 30, "MEDIUM")
        .otherwise("LOW")
    )
)

# ---------------------------------------------------------
# 8. Gold Provider Metrics
# ---------------------------------------------------------
gold_provider = (
    final_silver
    .groupBy("provider_id")
    .agg(
        count("claim_id").alias("total_claims"),
        round(sum("billed_amount"), 2).alias("total_billed"),
        round(sum("paid_amount"), 2).alias("total_paid"),
        sum(
            when(col("claim_status") == "DENIED", 1).otherwise(0)
        ).alias("denied_claims")
    )
    .withColumn(
        "denial_rate",
        round(col("denied_claims") / col("total_claims") * 100, 2)
    )
)

# ---------------------------------------------------------
# 9. Gold Denial Analysis
# ---------------------------------------------------------
gold_denial = (
    final_silver
    .filter(col("claim_status") == "DENIED")
    .groupBy("denial_category", "claim_type", "diagnosis_group")
    .agg(
        count("claim_id").alias("denied_claims"),
        round(sum("billed_amount"), 2).alias("denied_billed_amount"),
        round(avg("billed_amount"), 2).alias("avg_denied_claim")
    )
    .orderBy(desc("denied_claims"))
)

# ---------------------------------------------------------
# 10. Gold High-Cost Claims
# ---------------------------------------------------------
gold_high_cost = (
    final_silver
    .filter(col("high_cost_flag") == "HIGH_COST")
    .select(
        "claim_id", "patient_id", "provider_id",
        "claim_type", "diagnosis_group",
        "claim_status", "billed_amount",
        "paid_amount", "outstanding_amount",
        "risk_score", "risk_level"
    )
    .orderBy(desc("billed_amount"))
)

# ---------------------------------------------------------
# 11. Write datasets
# ---------------------------------------------------------
(
    final_silver
    .repartition(20, "claim_type")
    .write
    .mode("overwrite")
    .partitionBy("claim_type")
    .parquet("data/output/silver_claims")
)

gold_patient.write.mode("overwrite").parquet(
    "data/output/gold_patient_metrics"
)

gold_provider.write.mode("overwrite").parquet(
    "data/output/gold_provider_metrics"
)

gold_denial.write.mode("overwrite").parquet(
    "data/output/gold_denial_analysis"
)

gold_high_cost.write.mode("overwrite").parquet(
    "data/output/gold_high_cost_claims"
)

print("Healthcare claims transformation completed successfully.")

spark.stop()
