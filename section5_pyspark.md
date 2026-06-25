# Section 5: PySpark Must-Knows (The "Write the Code" Round)

---

## 1. Read CSV/Parquet/Delta into a DataFrame

* **Concept:** PySpark uses a unified reader interface (`spark.read`) to load external data formats into distributed DataFrames.
* **Whiteboard Code:**
```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define explicit schema for CSV reads
schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("price", DoubleType(), True)
])

# Read CSV with explicit schema (highly optimized, avoids scanning full files)
df_csv = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("s3://bucket/raw/orders.csv")

# Read Parquet
df_parquet = spark.read.parquet("s3://bucket/clean/orders.parquet")

# Read Delta
df_delta = spark.read.format("delta").load("s3://bucket/lakehouse/orders")
```
* **What the Interviewer is Testing:** 
  * Understanding that CSV files do not store schema metadata, so defining an explicit schema is critical (using `inferSchema: true` forces Spark to scan the entire dataset first, which causes a performance bottleneck).
  * Knowledge of the difference between text-based formats (CSV) and self-describing binary formats (Parquet, Delta) that load schemas instantly.

---

## 2. Filter, Select, WithColumn, Drop

* **Concept:** PySpark transformations used to subset columns, filter records, and create or transform attributes.
* **Whiteboard Code:**
```python
from pyspark.sql import functions as F

# Clean up, add columns, and filter records
cleaned_df = df_delta \
    .select("order_id", "customer_id", "sale_amount", "order_date") \
    .filter(F.col("sale_amount") > 100.0) \
    .withColumn("tax_amount", F.col("sale_amount") * 0.18) \
    .withColumn("order_year", F.year(F.col("order_date"))) \
    .drop("order_date")
```
* **What the Interviewer is Testing:**
  * Clean method-chaining style (improves code readability).
  * Use of functions from `pyspark.sql.functions` instead of writing custom raw string expressions or SQL queries where DataFrame APIs can perform the optimization natively.

---

## 3. GroupBy + Agg (Count, Sum, Avg, Max, Min, Collect_List)

* **Concept:** Aggregating data across logical partitions of a key.
* **Whiteboard Code:**
```python
from pyspark.sql import functions as F

# Aggregate sales and collect purchase list per customer
agg_df = df_delta \
    .groupBy("customer_id") \
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("sale_amount").alias("total_spend"),
        F.avg("sale_amount").alias("avg_spend"),
        F.collect_list("product_category").alias("purchased_categories")
    )
```
* **What the Interviewer is Testing:**
  * Syntax for executing multiple aggregations within a single `.agg()` function call.
  * Difference between `collect_list()` (keeps duplicate entries) and `collect_set()` (deduplicates entries) when collecting nested collections.

---

## 4. Window Functions in PySpark

* **Concept:** Applying analytical partition calculations over dynamic windows without collapsing the underlying dataset rows.
* **Whiteboard Code:**
```python
from pyspark.sql import expressions as Exp
from pyspark.sql import functions as F

# Define a window partitioned by department and ordered by salary desc
window_spec = Exp.Window \
    .partitionBy("department_id") \
    .orderBy(F.col("salary").desc())

# Calculate ranks and lead values
ranked_df = df_delta \
    .withColumn("rank", F.dense_rank().over(window_spec)) \
    .withColumn("previous_salary", F.lag("salary", 1).over(window_spec)) \
    .filter(F.col("rank") <= 3)
```
* **What the Interviewer is Testing:**
  * Import structure for PySpark window functions (`pyspark.sql.expressions.Window`).
  * Syntax mapping window definitions to Column expressions using the `.over(window_spec)` API.

---

## 5. All Join Types in PySpark

* **Concept:** Joining distributed datasets using the DataFrame API.
* **Whiteboard Code:**
```python
# Join df_orders (large) and df_customers (large) on customer_id
joined_df = df_orders.join(
    df_customers,
    on="customer_id",  # Safe syntax to prevent duplicate join columns
    how="inner"        # Choices: "inner", "left", "right", "outer", "left_semi", "left_anti"
)

# Left anti-join (identifies keys in left table that do NOT exist in right table)
churned_customers = df_customers.join(
    df_active_orders,
    on="customer_id",
    how="left_anti"
)
```
* **What the Interviewer is Testing:**
  * Specifying the join condition as a single string column name (`on="customer_id"`) rather than a boolean equivalence (`df_orders.customer_id == df_customers.customer_id`), which prevents duplicate join key columns in the output DataFrame.
  * Understanding of advanced joins like `left_semi` (returns left rows that match right) and `left_anti` (returns left rows with no matches in right) to optimize performance over standard joins followed by filters.

---

## 6. Handling Nulls (fillna, dropna, replace)

* **Concept:** Standard PySpark APIs for clean null management and data cleansing operations.
* **Whiteboard Code:**
```python
# Fill null values with defaults based on data type
filled_df = df_delta.na.fill({
    "customer_name": "Unknown",
    "age": 0,
    "rating": 5.0
})

# Drop rows if crucial columns are null
dropped_df = df_delta.na.drop(subset=["order_id", "customer_id"])

# Replace specific placeholder values
replaced_df = df_delta.na.replace("N/A", "Unknown", subset=["country"])
```
* **What the Interviewer is Testing:**
  * Use of the `.na` sub-API in PySpark (`df.na.fill`, `df.na.drop`) for managing missing values.
  * Targeting specific columns during null removal or default assignments to prevent data corruption.

---

## 7. UDFs (User Defined Functions) — Performance Warning

* **Concept:** Writing custom python logic when Spark's built-in functions are insufficient.
* **Whiteboard Code:**
```python
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Define pure Python logic
def standardize_email(email: str) -> str:
    if email is None:
        return "unknown@domain.com"
    return email.strip().lower()

# Register UDF
standardize_email_udf = F.udf(standardize_email, StringType())

# Apply UDF (WARNING: High performance cost!)
df_processed = df_delta.withColumn("email", standardize_email_udf(F.col("email")))
```
* **What the Interviewer is Testing:**
  * **Critical Performance understanding:** Traditional UDFs force Spark to serialize data out of JVM memory, pass it to a Python subprocess, execute the function, and serialize the result back to the JVM. This breaks execution pipeline optimizations.
  * **Alternative:** Always mention using Spark SQL built-in functions (e.g., `F.lower(F.trim(col("email")))`) first. If UDFs are mandatory, use **Pandas UDFs** (Vectorized UDFs using PyArrow) to execute computations in batch.

---

## 8. Reading a Delta Table with Time Travel

* **Concept:** Accessing historical snapshots of Delta tables using version or timestamp parameters.
* **Whiteboard Code:**
```python
# Access historical state using version number
df_v1 = spark.read \
    .format("delta") \
    .option("versionAsOf", 1) \
    .load("s3://bucket/lakehouse/orders")

# Access historical state using timestamp string
df_time = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2026-06-01 00:00:00") \
    .load("s3://bucket/lakehouse/orders")
```
* **What the Interviewer is Testing:**
  * Syntax for configuring Delta-specific time-travel parameters in reader options.
  * Practical understanding that time travel is only possible if the data files have not been purged by a `VACUUM` command.

---

## 9. Writing to Delta with MERGE (Upsert Pattern)

* **Concept:** Running conditional upserts from an incoming DataFrame into a target Delta Table.
* **Whiteboard Code:**
```python
from delta.tables import DeltaTable

# Reference target Delta table using its storage path
target_table = DeltaTable.forPath(spark, "s3://bucket/lakehouse/users")

# Execute Upsert (SCD Type 1) using Merge Builder API
target_table.alias("t") \
    .merge(
        source=df_updates.alias("s"),
        condition="t.user_id = s.user_id"
    ) \
    .whenMatchedUpdate(set={
        "email": "s.email",
        "updated_at": "current_timestamp()"
    }) \
    .whenNotMatchedInsert(values={
        "user_id": "s.user_id",
        "name": "s.name",
        "email": "s.email",
        "created_at": "current_timestamp()",
        "updated_at": "current_timestamp()"
    }) \
    .execute()
```
* **What the Interviewer is Testing:**
  * Familiarity with the `delta.tables.DeltaTable` class.
  * Clean utilization of Delta's Merge Builder API, establishing correct aliases, conditions, and update/insert actions.

---

## 10. Streaming: readStream from Kafka, writeStream to Delta with Checkpointing

* **Concept:** Ingesting real-time event streams from Apache Kafka and writing them to Delta tables with ACID guarantees.
* **Whiteboard Code:**
```python
# Read streaming data from Kafka broker
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_clicks") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka binary payload to String
parsed_stream = kafka_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Write streaming data to Delta sink with Checkpointing
query = parsed_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3://bucket/lakehouse/checkpoints/user_clicks") \
    .start("s3://bucket/lakehouse/tables/user_clicks")
```
* **What the Interviewer is Testing:**
  * Understanding that Spark Kafka integration reads keys and values as raw binary data that must be explicitly cast to string or deserialized using a schema.
  * Knowledge of the critical role of `checkpointLocation` in enabling fault tolerance and offset tracking for restarts.

---

## 11. Repartition vs Coalesce

* **Concept:** Adjusting the number of partitions in a distributed DataFrame.
* **Whiteboard Code:**
```python
# Repartition: Increases or decreases partition count. Triggers a full shuffle.
df_repartitioned = df_delta.repartition(10, "country")

# Coalesce: Decreases partition count only. Does NOT trigger a full network shuffle.
df_coalesced = df_delta.coalesce(2)
```
* **What the Interviewer is Testing:**
  * **Critical Performance understanding:** `coalesce` avoids shuffles by merging adjacent partitions on executors, but can result in skewed partition sizes if partition counts are reduced drastically. `repartition` redistributes all data evenly using a hash partitioner, but incurs a high network and disk shuffle cost.
  * **Rule of thumb:** Use `coalesce` only when decreasing partition counts (e.g., right before writing a small dataset to disk). Use `repartition` when increasing partitions to increase parallelism, or when needing to re-align partitions based on a join key.

---

## 12. Cache vs Persist

* **Concept:** Saving intermediate DataFrame computations in memory or on disk to prevent recomputing them when multiple actions are called downstream.
* **Whiteboard Code:**
```python
from pyspark import StorageLevel

# Cache: Saves DataFrame in memory with default storage level (MEMORY_AND_DISK)
df_cached = df_delta.filter(F.col("year") == 2026).cache()

# Persist: Saves DataFrame with a customizable storage level
df_persisted = df_delta.persist(StorageLevel.MEMORY_ONLY_SER)

# Trigger an action to actually populate the cache
df_cached.count()
```
* **What the Interviewer is Testing:**
  * **Key Difference:** `cache()` is a shorthand for `persist(StorageLevel.MEMORY_AND_DISK)` (or `MEMORY_ONLY` for RDDs). `persist()` allows specifying storage levels, such as serialized in memory (`MEMORY_ONLY_SER`) or written to local disk (`DISK_ONLY`).
  * **Important Caveat:** Cache/Persist are lazy operations. They do not populate memory until the first action is called on the DataFrame.

---

## 13. Broadcast Join in Code

* **Concept:** Explicitly instructing Spark to send a small DataFrame to all executors, converting a wide Sort-Merge Join into a narrow Map-Side Join.
* **Whiteboard Code:**
```python
from pyspark.sql import functions as F

# Read massive fact table
df_sales = spark.read.parquet("s3://bucket/lakehouse/sales")

# Read small lookup table
df_stores = spark.read.parquet("s3://bucket/lakehouse/stores")

# Join using broadcast optimization on the small lookup table
df_joined = df_sales.join(
    F.broadcast(df_stores),
    on="store_id",
    how="inner"
)
```
* **What the Interviewer is Testing:**
  * Syntax for wrapping the small DataFrame with `pyspark.sql.functions.broadcast`.
  * Technical reasoning: It completely eliminates the shuffle stage for the large table, dramatically reducing network IO and processing time.

---

## 14. Reading and Writing Partitioned Data

* **Concept:** Storing and reading data organized into partition directories in object storage.
* **Whiteboard Code:**
```python
# Write DataFrame partitioned by country and year to disk
df_delta.write \
    .format("delta") \
    .partitionBy("country", "year") \
    .mode("overwrite") \
    .save("s3://bucket/lakehouse/customers")

# Read partitioned data filtering on partition keys (Partition Pruning)
df_pruned = spark.read \
    .format("delta") \
    .load("s3://bucket/lakehouse/customers") \
    .filter((F.col("country") == "IN") & (F.col("year") == 2026))
```
* **What the Interviewer is Testing:**
  * Ability to write data partitioned by multiple keys.
  * Realization that when reading partitioned data, applying filter clauses on the partition key columns triggers **Partition Pruning** (Spark only reads the matching folders, saving time and disk IO).

---

## 15. Schema Definition (StructType, StructField)

* **Concept:** Defining a robust data structure schema for Spark DataFrames.
* **Whiteboard Code:**
```python
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType
)

# Define complex nested schema
user_schema = StructType([
    StructField("user_id", StringType(), False),  # Not Nullable
    StructField("user_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("account_balance", DoubleType(), True),
    StructField("created_at", TimestampType(), True),
    # Nested column containing a list of strings
    StructField("security_roles", ArrayType(StringType()), True)
])
```
* **What the Interviewer is Testing:**
  * Correct usage of nested types like `ArrayType` and type casting definitions.
  * Designing schemas that enforce column constraints (nullability flags) before writing to clean database stages.

---

## 16. Pandas UDF (Vectorized UDFs) vs Python UDFs

* **Concept:** 
  * Standard Python UDFs execute row-by-row, passing single JVM data objects to a Python worker process which disables Spark query optimizer features.
  * Pandas UDFs use **Apache Arrow** to serialize and transfer data in vectorized batches, executing calculations in Python using Pandas Series or DataFrames, which reduces serialization latency.
* **Whiteboard Code:**
```python
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Define Vectorized Pandas UDF (Series-to-Series)
@F.pandas_udf(DoubleType())
def calculate_discount_udf(price: pd.Series, discount: pd.Series) -> pd.Series:
    """Computes price after discount using vectorized pandas operations."""
    return price * (1.0 - discount)

# Apply Vectorized UDF to DataFrame
df_discounted = df_delta.withColumn(
    "discounted_price", 
    calculate_discount_udf(F.col("price"), F.col("discount_pct"))
)
```
* **What the Interviewer is Testing:**
  * Technical awareness of Spark's execution boundaries and JVM-to-Python serialization cost.
  * Correct usage of the `@pandas_udf` decorator specifying input/output data structures (Pandas Series) and data type mapping.

---

## 17. Implementing Salting to Resolve a Skewed Join

* **Concept:** When joining a massive dataset that has a skewed join key (e.g., millions of records matching `customer_id = 1` or `null`) with a lookup table, a single executor will be overloaded and spill to disk. "Salting" adds a random integer suffix (the salt) to the join key in the skewed table, and replicates the rows in the lookup table by that same range of suffixes, distributing the keys evenly across executors.
* **Whiteboard Code:**
```python
import random
from pyspark.sql import functions as F

# Assume df_large is skewed on "merchant_id" and df_lookup is the merchant lookup table
# We choose a salt factor of 4 (distributes skewed key across 4 partitions)
salt_factor = 4

# Step 1: Add a random salt column to the large, skewed dataset
df_large_salted = df_large.withColumn(
    "salt", 
    F.concat(F.col("merchant_id"), F.lit("_"), F.floor(F.rand() * salt_factor))
)

# Step 2: Replicate the lookup dataset by generating rows for every possible salt value
# Explode creates a row matching each index in [0, 1, 2, 3] for every merchant record
salt_array = F.array([F.lit(i) for i in range(salt_factor)])

df_lookup_replicated = df_lookup \
    .withColumn("salt_val", F.explode(salt_array)) \
    .withColumn("salt", F.concat(F.col("merchant_id"), F.lit("_"), F.col("salt_val")))

# Step 3: Perform the join on the new salted key
df_joined = df_large_salted.join(
    df_lookup_replicated,
    on="salt",
    how="inner"
).drop("salt", "salt_val")  # Clean up temporary columns
```
* **What the Interviewer is Testing:**
  * Solid understanding of distributed computing bottlenecks (data skew).
  * Implementation mechanics: Adding random numbers to source keys, using `.explode()` to replicate lookup records, and joining on the synthetic keys.

---

## 18. Production-Grade PySpark Application Template

* **Concept:** Standard template structure for building a robust Spark ETL script, including optimization parameters, structured logging, schema declarations, and cleanup blocks.
* **Whiteboard Code:**
```python
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def init_spark() -> SparkSession:
    """Initializes Spark Session with production configurations."""
    return SparkSession.builder \
        .appName("Production-ETL-Pipeline") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

def run_transformations(df):
    """Encapsulates pure transformation logic (easy to unit-test)."""
    return df \
        .filter(F.col("status") == "SUCCESS") \
        .withColumn("ingested_at", F.current_timestamp())

def main(input_path: str, output_path: str):
    # Initialize logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger(__name__)
    
    logger.info("Initializing Spark Application...")
    spark = init_spark()
    
    try:
        logger.info(f"Reading source data from {input_path}")
        df_raw = spark.read.parquet(input_path)
        
        logger.info("Running transformations...")
        df_clean = run_transformations(df_raw)
        
        logger.info(f"Writing output to {output_path}")
        df_clean.write \
            .mode("overwrite") \
            .format("delta") \
            .save(output_path)
        
        logger.info("ETL Run Completed Successfully.")
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {str(e)}")
        sys.exit(1)
        
    finally:
        logger.info("Stopping Spark Session...")
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python etl.py <input_path> <output_path>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
```
* **What the Interviewer is Testing:**
  * Clean modular design (decoupling session initialization, transformation logic, and orchestrating execution in a main method).
  * Tuning settings: Explicitly enabling Spark Adaptive Query Execution (`spark.sql.adaptive.enabled`), automatic partition coalescing, and skew join handling.
  * Strict resource management by calling `spark.stop()` in a `finally` block to prevent resource leaks on host machines.

