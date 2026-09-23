from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, to_date, year, month,
    when, sum, count, avg
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)

# ---------------------------------------------------------
# 1. Create Spark Session
# ---------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("PySparkTransformationProject")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------
# 2. Define Schema
# ---------------------------------------------------------
schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True)
])

input_path = "data/input/customer_sales.csv"

# ---------------------------------------------------------
# 3. Read Raw Data
# ---------------------------------------------------------
df = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv(input_path)
)

print("===== RAW DATA =====")
df.show(truncate=False)

# ---------------------------------------------------------
# 4. Data Quality Checks
# ---------------------------------------------------------
print("===== RECORD COUNT =====")
print(df.count())

print("===== NULL COUNTS =====")
for column in df.columns:
    null_count = df.filter(col(column).isNull()).count()
    print(f"{column}: {null_count}")

# ---------------------------------------------------------
# 5. Data Cleaning
# ---------------------------------------------------------
clean_df = (
    df
    .dropDuplicates(["order_id"])
    .fillna({
        "customer_name": "Unknown",
        "city": "Unknown",
        "category": "Unknown",
        "quantity": 0,
        "unit_price": 0.0
    })
)

# ---------------------------------------------------------
# 6. Standardize Columns
# ---------------------------------------------------------
clean_df = (
    clean_df
    .withColumn("customer_name", trim(col("customer_name")))
    .withColumn("city", upper(trim(col("city"))))
    .withColumn("category", upper(trim(col("category"))))
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
)

# ---------------------------------------------------------
# 7. Derived Columns
# ---------------------------------------------------------
transformed_df = (
    clean_df
    .withColumn(
        "total_amount",
        col("unit_price") * col("quantity")
    )
    .withColumn("order_year", year(col("order_date")))
    .withColumn("order_month", month(col("order_date")))
    .withColumn(
        "order_status",
        when(col("status") == "Completed", "SUCCESS")
        .when(col("status") == "Pending", "PENDING")
        .otherwise("CANCELLED")
    )
)

# ---------------------------------------------------------
# 8. Filter Cancelled Orders
# ---------------------------------------------------------
sales_df = transformed_df.filter(
    col("order_status") != "CANCELLED"
)

print("===== TRANSFORMED SALES =====")
sales_df.show(truncate=False)

# ---------------------------------------------------------
# 9. Category-Level Aggregation
# ---------------------------------------------------------
category_sales = (
    sales_df
    .groupBy("category")
    .agg(
        sum("total_amount").alias("total_sales"),
        count("order_id").alias("order_count"),
        avg("total_amount").alias("average_order_value")
    )
    .orderBy(col("total_sales").desc())
)

print("===== CATEGORY SALES =====")
category_sales.show()

# ---------------------------------------------------------
# 10. City-Level Aggregation
# ---------------------------------------------------------
city_sales = (
    sales_df
    .groupBy("city")
    .agg(
        sum("total_amount").alias("total_sales"),
        count("order_id").alias("order_count")
    )
    .orderBy(col("total_sales").desc())
)

print("===== CITY SALES =====")
city_sales.show()

# ---------------------------------------------------------
# 11. Write Outputs
# ---------------------------------------------------------
(
    sales_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv("data/output/transformed_sales")
)

(
    category_sales
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv("data/output/category_sales")
)

(
    city_sales
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv("data/output/city_sales")
)

print("Pipeline completed successfully!")

spark.stop()
