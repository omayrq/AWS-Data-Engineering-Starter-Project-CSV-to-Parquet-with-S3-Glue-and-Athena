import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, trim, to_date, year, month

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_DATABASE", "SOURCE_TABLE", "TARGET_PATH"],
)

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Read the CSV table from Glue Data Catalog
source_df = glue_context.create_dynamic_frame.from_catalog(
    database=args["SOURCE_DATABASE"],
    table_name=args["SOURCE_TABLE"],
).toDF()

print("DEBUG: Total rows read: ", source_df.count())

required_columns = {"order_id", "customer_id", "order_date", "amount", "status"}
missing_columns = required_columns.difference(set(source_df.columns))
if missing_columns:
    raise ValueError(
        "The source table is missing required columns: " + ", ".join(sorted(missing_columns))
    )

# Clean strings and standard types safely
clean_df = (
    source_df
    .withColumn("order_id", trim(col("order_id").cast("string")))
    .withColumn("customer_id", trim(col("customer_id").cast("string")))
    .withColumn("order_date", to_date(col("order_date").cast("string"), "yyyy-MM-dd"))
    .withColumn("amount", col("amount").cast("decimal(12,2)"))
    .withColumn("status", trim(col("status").cast("string")))
    .filter(col("order_id").isNotNull() & (col("order_id") != ""))
)

# Deduplicate by order_id
clean_df = clean_df.dropDuplicates(["order_id"])

# Partition by year/month with fallback values if date is null
output_df = (
    clean_df
    .withColumn("order_year", year(col("order_date")).cast("string"))
    .withColumn("order_month", month(col("order_date")).cast("string"))
    .fillna({"order_year": "2026", "order_month": "10"})
    .select(
        "order_id", "customer_id", "order_date", "amount", "status",
        "order_year", "order_month"
    )
)

print("DEBUG: Total rows ready to write: ", output_df.count())

target_path = args["TARGET_PATH"].rstrip("/") + "/"
(
    output_df.write
    .mode("overwrite")
    .partitionBy("order_year", "order_month")
    .parquet(target_path)
)

job.commit()