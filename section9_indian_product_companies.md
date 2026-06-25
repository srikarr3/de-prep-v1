# Section 9: Top 15 Most Asked DE Interview Questions at Indian Product Companies

---

## 1. How do you design a deduplication pipeline for transaction data at scale?
* **Commonly Asked By:** Razorpay, PhonePe, CRED
* **Why they ask it:** Processing payments requires absolute accuracy. They are evaluating your understanding of ACID transactions, idempotency keys, and how to write deduplication logic in Spark/Delta Lake without bottlenecking performance.
* **The Answer that gets you hired:** 
  > "To deduplicate transaction data at scale, I use a two-tiered deduplication strategy. At the ingestion layer, we assign a unique idempotency key (hash of transaction ID, timestamp, and amount) to every event. For our streaming sink, we use Spark Structured Streaming with stateful deduplication via `dropDuplicates(["idempotency_key", "event_time"])` combined with a 1-hour watermark to limit state size. 
  > 
  > At the storage layer, downstream updates are written to our Silver Delta table using a `MERGE` statement matched on the transaction ID:
  > ```sql
  > MERGE INTO silver_transactions t 
  > USING updates s ON t.transaction_id = s.transaction_id
  > WHEN MATCHED AND s.status_time > t.status_time THEN UPDATE SET ...
  > WHEN NOT MATCHED THEN INSERT ...
  > ```
  > This ensures that even if events are retried or arrive out-of-order, our target table remains consistent and duplicate records are rejected."

---

## 2. Explain Spark Shuffle. How do you optimize jobs with heavy network shuffles?
* **Commonly Asked By:** Flipkart, Swiggy, Uber
* **Why they ask it:** These companies process petabytes of clickstream and logistics data daily. Shuffling is the most expensive operation in Spark, and they want to make sure you know how to configure and design jobs to minimize network IO.
* **The Answer that gets you hired:**
  > "A Spark Shuffle occurs during wide transformations (like `groupBy`, `join`, or `repartition`) when data must be redistributed across executors. It involves a write stage (writing sorted partition files to local disk on source executors) and a read stage (transferring those partitions over the network to target executors). 
  > 
  > To optimize jobs with heavy shuffles, I apply the following:
  > 1. **Filter and Project Early:** Filter out irrelevant rows and select only necessary columns before the shuffle stage to reduce the network payload.
  > 2. **Map-side Join (Broadcast):** If joining a large table with a small table (< 100MB), I broadcast the small table using `broadcast(df_small)` to bypass the shuffle entirely.
  > 3. **Optimize Partitions:** Adjust `spark.sql.shuffle.partitions` dynamically to target partition sizes of 128MB–200MB, preventing tasks from idling or spilling data to disk."

---

## 3. How do you handle late-arriving events in a streaming dashboard pipeline?
* **Commonly Asked By:** Swiggy, Uber
* **Why they ask it:** Logistics and delivery apps run on real-time data. Network drops on delivery partner phones mean events arrive hours late. They are testing your knowledge of watermarking, state stores, and windowing.
* **The Answer that gets you hired:**
  > "To handle late-arriving events, I use event-time processing combined with watermarking in Spark Structured Streaming. The watermark defines a threshold for how long the engine should wait for delayed events based on the maximum event time observed so far.
  > 
  > For example, with a 2-hour watermark:
  > ```python
  > df.withWatermark("event_time", "2 hours") \
  >   .groupBy(window(col("event_time"), "10 minutes")) \
  >   .count()
  > ```
  > Events that arrive late but within the 2-hour window will update our windowed aggregates. Any events delayed by more than 2 hours are dropped by the engine to prevent the state store in memory from growing indefinitely. Dropped events are routed to a late-arrival storage path for audit and manual reconciliation."

---

## 4. How would you design a near real-time ingestion pipeline from MySQL to a Data Lakehouse?
* **Commonly Asked By:** Razorpay, CRED, Swiggy
* **Why they ask it:** They want to avoid running heavy batch queries on production transactional databases. They are evaluating your knowledge of Change Data Capture (CDC) architectures.
* **The Answer that gets you hired:**
  > "I would implement an event-driven Change Data Capture (CDC) pipeline. We configure **Debezium** to read the MySQL binary log (`binlog`) asynchronously, which captures all inserts, updates, and deletes without affecting database performance.
  > 
  > Debezium streams these changes as structured JSON/Avro events into **Apache Kafka** topics. We use **Spark Structured Streaming** to consume from Kafka and write the raw events directly to an append-only Delta Lake **Bronze table**. A downstream streaming job runs a `foreachBatch` writer that executes a `MERGE` statement against our conformed **Silver Delta table**, applying updates, inserts, and delete operations based on the CDC metadata."

---

## 5. What is the difference between Partitioning and Bucketing in Spark? When do you choose which?
* **Commonly Asked By:** Flipkart, Adobe
* **Why they ask it:** They want to see if you understand physical storage layouts and how to choose the right strategy based on column cardinality to avoid creating too many small files.
* **The Answer that gets you hired:**
  > "Partitioning physically organizes data into separate directories based on a column's values. It is ideal for low-cardinality columns that are frequently used in query filters, such as `date` or `region`.
  > 
  > Bucketing distributes data across a fixed number of physical files within a directory based on the hash of a column. It is ideal for high-cardinality columns (like `user_id` or `product_id`) that are frequently used in joins or aggregations.
  > 
  > I choose partitioning when I can prune search directories during queries. I choose bucketing when joining two large datasets on a high-cardinality key, pre-bucketing both tables on the join key to allow Spark to perform Bucket-Map Joins without network shuffles."

---

## 6. How do you implement SCD Type 2 updates in Delta Lake?
* **Commonly Asked By:** Atlassian, Adobe
* **Why they ask it:** Tracking historical changes (like subscription status or customer billing address changes) is a standard requirement for analytics. They are testing your mastery of Delta Lake's transactional merge capabilities.
* **The Answer that gets you hired:**
  > "To implement Slowly Changing Dimension (SCD) Type 2 updates in Delta Lake, I use a two-part merge operation using PySpark.
  > 
  > First, I identify the updates from our staging table. I perform a join to find target records where the staging value differs from the active target record (`is_current = true`).
  > 
  > Second, I execute a Delta `MERGE` statement using a unioned dataset containing:
  > 1. The new incoming updates (which are inserted as new rows with `is_current = true`, `effective_date = current_date`, `end_date = null`).
  > 2. A copy of the changed rows (used to match existing target records and update them setting `is_current = false` and `end_date = current_date`). This ensures historical audit records are preserved in a single transaction."

---

## 7. How do you optimize a Spark job that is spilling data to disk?
* **Commonly Asked By:** Flipkart, Swiggy, Uber
* **Why they ask it:** Spill to disk occurs when executor RAM is exhausted during a shuffle or sort, causing Spark to write partitions to local disk. This slows down execution. They are testing your knowledge of Spark memory management.
* **The Answer that gets you hired:**
  > "Spill to disk occurs when a task's execution memory exceeds the allocated partition memory size. To resolve this:
  > 1. **Increase Shuffle Partitions:** I increase `spark.sql.shuffle.partitions` to divide the dataset into smaller, more manageable partitions (target size 128MB).
  > 2. **Resolve Data Skew:** If the spill is caused by data skew on a join key, I apply salting to distribute the skewed keys across workers.
  > 3. **Adjust Memory Configuration:** I increase the executor memory overhead (`spark.executor.memoryOverhead`) or increase the executor core allocation to balance task distribution.
  > 4. **Use Broadcast Joins:** If a join is triggering the shuffle, I broadcast the smaller table to bypass the shuffle stage entirely."

---

## 8. What is the role of the Transaction Log in Delta Lake? How does it enable ACID compliance?
* **Commonly Asked By:** Adobe, Freshworks
* **Why they ask it:** Delta Lake is the standard table format in modern data stacks. They want to make sure you understand the underlying storage format rather than just using it as a black box.
* **The Answer that gets you hired:**
  > "The Delta Transaction Log (stored in the `_delta_log/` directory) is an append-only commit log that serves as the single source of truth for the table. Every write, update, or delete is recorded as a sequential JSON commit file (e.g., `000000.json`).
  > 
  > When a query reads a Delta table, the engine reads the transaction log first to identify the active list of Parquet files for the latest commit version, ignoring any uncommitted or old files. This enables:
  > 1. **ACID Compliance:** Writes are atomic; they commit only when the JSON log is written.
  > 2. **Time Travel:** Readers can query the state of the log at a specific commit version.
  > 3. **Concurrency:** Multiple readers and writers can access the table simultaneously using Optimistic Concurrency Control."

---

## 9. How do you handle schema drift in your ingestion pipelines?
* **Commonly Asked By:** Zoho, Freshworks
* **Why they ask it:** Upstream applications frequently modify database columns or API payloads without notifying the data team. They want to see how you design pipelines that can handle schema changes without crashing downstream processes.
* **The Answer that gets you hired:**
  > "To handle schema drift, I use a layered schema governance strategy:
  > 1. **Bronze Layer (Evolution):** We write raw files to Bronze Delta tables using `.option("mergeSchema", "true")`. This allows Spark to append new columns to the schema dynamically without failing.
  > 2. **Silver Layer (Enforcement):** In the Silver layer, we apply schema enforcement. We validate the incoming columns against our conformed schema. Any unexpected columns are logged, and missing columns are filled with default null values.
  > 3. **Data Contracts:** We implement a Schema Registry (like Confluent) for streaming data. Upstream teams must register schema changes, and backward-incompatible changes are blocked at the CI/CD level."

---

## 10. How do you design an Airflow DAG to support backfilling? What are the key parameters?
* **Commonly Asked By:** CRED, Atlassian
* **Why they ask it:** Backfilling historical data after updating business logic is a common task. They are evaluating your practical experience with Airflow's scheduling parameters and execution contexts.
* **The Answer that gets you hired:**
  > "To design an Airflow DAG that supports backfilling:
  > 1. **Parameterize Dates:** I use Airflow execution macros (like `{{ ds }}`) in my task scripts, ensuring the code processes data for the logical execution date rather than using hardcoded values like `current_date()`.
  > 2. **Enable Catchup:** Set `catchup=True` in the DAG definition to instruct the scheduler to run historical periods sequentially back to the `start_date`.
  > 3. **Control Concurrency:** I set `max_active_runs` to a value that balances cluster resource constraints (e.g., limit to 3 parallel runs) to avoid overwhelming the database.
  > 4. **Ensure Idempotency:** The tasks must use overwrite or merge statements to ensure that running the same date multiple times does not duplicate data."

---

## 11. Explain the difference between Broadcast Join and Sort-Merge Join in Spark. When does Broadcast fail?
* **Commonly Asked By:** Flipkart, PhonePe
* **Why they ask it:** Joins are common bottlenecks in distributed data pipelines. They want to make sure you know when to use broadcast optimizations and how to troubleshoot broadcast failures.
* **The Answer that gets you hired:**
  > "A Broadcast Join sends the smaller dataset to all executor nodes, performing a local map-side join and bypassing the network shuffle stage. A Sort-Merge Join shuffles both datasets on the join key, sorts the partitions on each executor, and merges them sequentially.
  > 
  > Broadcast Joins fail under the following conditions:
  > 1. **Memory Limitations (OOM):** If the broadcast dataset exceeds the executor or driver memory limits, it triggers an Out-Of-Memory error.
  > 2. **Incorrect Size Metadata:** If Spark cannot estimate the size of a DataFrame (e.g., reading from an unindexed custom datasource), it will fail to broadcast it automatically.
  > 3. **Non-Equi Joins:** Broadcast joins require an equality join condition (`=`); they cannot be used for complex inequality joins."

---

## 12. How do you build a Data Quality Framework using the Write-Audit-Publish (WAP) pattern?
* **Commonly Asked By:** Razorpay, CRED
* **Why they ask it:** These companies cannot afford to let bad data reach financial dashboards. They want to see how you validate data quality before making it visible to downstream users.
* **The Answer that gets you hired:**
  > "I implement the Write-Audit-Publish (WAP) pattern using Delta Lake tables:
  > 1. **Write:** The pipeline writes the output of the transformation step to a temporary staging table or an isolated Delta partition.
  > 2. **Audit:** We run automated quality checks on the staging table using a data validation framework (like Great Expectations or Deequ). We validate rules like primary key uniqueness, null limits, and value distributions.
  > 3. **Publish:** If the checks pass, we perform an atomic overwrite or merge into the production Gold table. If any check fails, the pipeline raises an alert to our team and stops execution, preventing the bad data from reaching the production catalog."

---

## 13. How do you configure Kafka partition counts? How does it impact consumer scalability?
* **Commonly Asked By:** Swiggy, Uber, PhonePe
* **Why they ask it:** Kafka is the backbone of real-time messaging systems. They want to see if you know how to scale Kafka topics to support highly parallel consumer groups.
* **The Answer that gets you hired:**
  > "Kafka partition counts are determined by the throughput requirements and the target level of parallelism. The partition is the unit of parallelism in Kafka.
  > 
  > The key rules are:
  > 1. **Consumer Group Limit:** A partition can only be read by one consumer thread within a consumer group. If a topic has 4 partitions, a consumer group can scale up to 4 consumers. Adding a 5th consumer will leave it idle.
  > 2. **Throughput Estimation:** If a single partition supports 10MB/s of write throughput, and our target is 100MB/s, we need at least 10 partitions.
  > 3. **Ordering Guarantee:** Kafka guarantees message ordering only within a partition. If ordering is required, we partition by a key (e.g., `user_id`) to ensure all related events land in the same partition."

---

## 14. What is the difference between Cache and Persist in Spark? When do you clear them?
* **Commonly Asked By:** Flipkart, Adobe
* **Why they ask it:** Unmanaged caching causes executor memory leaks. They are testing your knowledge of Spark memory management and resource cleanup.
* **The Answer that gets you hired:**
  > "`cache()` is a shorthand for `persist(StorageLevel.MEMORY_AND_DISK)`. It stores the DataFrame in serialized or deserialized formats in memory, falling back to disk when executor memory is full. 
  > 
  > `persist()` allows you to customize the storage level, such as storing data in memory only (`MEMORY_ONLY`), serializing it to save memory space (`MEMORY_ONLY_SER`), or replicating it across two nodes (`MEMORY_AND_DISK_2`) for fault tolerance.
  > 
  > **When to clear:** Spark does not clear cached DataFrames automatically during a job execution. If the DataFrame is no longer needed downstream, I explicitly call `.unpersist()` to free up executor memory and prevent garbage collection bottlenecks."

---

## 15. How do you handle transient network failures when fetching data from external APIs?
* **Commonly Asked By:** Zoho, Freshworks
* **Why they ask it:** Ingestion pipelines often query external REST APIs that experience rate limits or temporary drops. They are evaluating your ability to build resilient pipelines using retry patterns and backoff limits.
* **The Answer that gets you hired:**
  > "To handle transient network failures:
  > 1. **Exponential Backoff and Jitter:** I implement a retry mechanism with exponential backoff and random jitter. This avoids overwhelming the external API when retrying requests.
  > 2. **Rate Limit Handling:** I parse the HTTP response headers (such as `Retry-After`) and pause pipeline execution accordingly.
  > 3. **Circuit Breakers:** If the API fails consistently (e.g., 5 consecutive errors), the circuit breaker opens, halting the pipeline and raising alerts instead of wasting system resources.
  > 4. **Context Managers:** I wrap connection calls in context managers to ensure all network sockets are closed properly on failure."

---

## 16. How do you design a rate-limited API ingestion pipeline that complies with rate limits without losing data?
* **Commonly Asked By:** Razorpay, CRED, Zoho
* **Why they ask it:** Integrating third-party APIs (like credit bureaus or payment gateways) often involves strict quotas. They want to check if you know how to build flow control into your extraction jobs.
* **The Answer that gets you hired:**
  > "To ingest from a rate-limited API safely, I use a queue-based token bucket or leaky bucket algorithm:
  > 1. **Orchestrator Rate-Limiting:** If using Celery or Airflow, I configure task concurrency limits to restrict how many instances of the extraction worker run in parallel.
  > 2. **Token Bucket Rate Limiter (Python):** In our ingestion code, we use libraries like `limiter` or custom Redis-based token bucket checks. A worker checks Redis to see if it can acquire a 'token' to call the API. If no tokens are available, the worker pauses (`time.sleep`) or yields back to the task queue.
  > 3. **DLQ and Retry Back-off:** If the API returns HTTP 429 (Too Many Requests), we parse the `Retry-After` header, calculate the sleep delay, and put the request back onto the task queue. Persistent failures are routed to a Dead Letter Queue for reprocessing, ensuring no request is lost."

---

## 17. How do you partition a Spark table that is growing exponentially? What happens if you partition by user_id?
* **Commonly Asked By:** Flipkart, Swiggy, PhonePe
* **Why they ask it:** They want to see if you understand the metadata overhead in distributed file systems. Partitioning by high-cardinality columns is a classic mistake.
* **The Answer that gets you hired:**
  > "For a table growing exponentially (billions of rows per day), I apply partitioning on a low-cardinality column that matches our query filters—most commonly `date` (e.g., `event_date`) or a combination of `year` and `month`.
  > 
  > **What happens if you partition by a high-cardinality column like `user_id`:**
  > It triggers the **Small File Problem** and **Metadata Bloat**:
  > 1. **Directory Explosion:** Spark physically writes separate directory folders for every partition key. If you have 50 million unique users, Spark will attempt to create 50 million folders.
  > 2. **Metadata Exhaustion:** The database metastore (Hive Metastore) or Unity Catalog must track the metadata path for every partition file. Millions of partitions will exhaust metastore memory and slow down query compiling.
  > 3. **Small Files:** Because data is divided across 50 million folders, each user's directory will contain tiny files (under a few kilobytes). Read queries will spend more time running file-open I/O operations than scanning data.
  > 
  > **Solution:** Partition by `date` (or `year/month`), and use **Bucketing** or **Liquid Clustering** on the high-cardinality `user_id` column to group user records into a fixed number of physical files."

