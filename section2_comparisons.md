# Section 2: Comparison Questions (The "Difference Between" Round)

---

## 1. OLTP vs OLAP

| Dimension | OLTP (Online Transaction Processing) | OLAP (Online Analytical Processing) |
| :--- | :--- | :--- |
| **Primary Focus** | Day-to-day transactional operations | Complex analytics and historical reporting |
| **Read vs Write** | Write-heavy, minor read (point lookups) | Read-heavy, batch writes (bulk inserts) |
| **Data Structure** | Highly normalized (3NF) to avoid duplication | Denormalized (Star/Snowflake schema) |
| **Storage Layout** | Row-based (contigious rows on disk) | Columnar (contigious columns on disk) |
| **Transaction Speed**| Sub-millisecond (single row operations) | Seconds to minutes (scans millions of rows) |

* **Decision Rule:**
  * Pick **OLTP** when building application backends, payment processing systems, or user registers where transactions must be completed instantly and remain consistent.
  * Pick **OLAP** when building data pipelines for executive reporting, dashboards, or data science modeling where users need to aggregate millions of historical records.
* **Real-World Pipeline Scenario:**
  * In a payment application like PhonePe, the user-facing ledger that tracks wallet balances lives in an **OLTP** database (PostgreSQL). At midnight, an ETL pipeline extracts these logs and loads them into an **OLAP** database (Snowflake) for fraud analysts to query patterns.

---

## 2. ETL vs ELT

| Dimension | ETL (Extract, Transform, Load) | ELT (Extract, Load, Transform) |
| :--- | :--- | :--- |
| **Transformation Venue**| On an intermediate staging/compute server | Directly inside the target storage platform |
| **Staging Area** | Required for raw data processing | Not required; loads directly to lake/warehouse |
| **Load Speed** | Slower (bottlenecked by transformation engine) | Extremely fast (raw dump, no overhead) |
| **Compute Overhead** | High on ETL tools, low on database | High on database, zero on pipeline code |
| **Data Privacy** | High (sensitive data masked before loading) | Complex (sensitive raw data lands in warehouse) |

* **Decision Rule:**
  * Pick **ETL** when moving data containing sensitive PII (like Credit Card numbers) that must be masked *before* landing on disk, or when target storage compute is extremely expensive.
  * Pick **ELT** when using scalable cloud data warehouses (Snowflake, BigQuery) where the cost of compute is cheap and loading raw data quickly is the primary goal.
* **Real-World Pipeline Scenario:**
  * An e-commerce pipeline uses **ELT** by dumping raw JSON clickstream files from S3 directly into Snowflake using Snowpipe. Downstream, dbt compiles SQL to transform the raw JSON into business tables inside Snowflake.

---

## 3. Data Warehouse vs Data Lake vs Data Lakehouse

| Dimension | Data Warehouse | Data Lake | Data Lakehouse |
| :--- | :--- | :--- | :--- |
| **Data Types** | Structured relational tables | Structured, semi-structured, unstructured | Structured, semi-structured |
| **Cost Profile** | Expensive (bundled storage/compute) | Very cheap (raw object storage) | Medium-Low (cheap storage, active compute) |
| **ACID Compliance** | Out-of-the-box (strict) | None (handled manually in code) | Out-of-the-box via open table formats |
| **Schema enforcement**| Schema-on-Write | Schema-on-Read | Hybrid (Schema Enforcement & Evolution) |
| **Use Cases** | BI and corporate SQL reports | Data archiving, ML training | Unified BI, streaming, and ML pipelines |

* **Decision Rule:**
  * Pick a **Data Warehouse** for structured BI reporting where queries must run in milliseconds and users are business analysts.
  * Pick a **Data Lake** for raw historical storage where you need to archive petabytes of log files for future data science work.
  * Pick a **Data Lakehouse** when building a unified data platform that requires both advanced ML modelling and ACID-compliant SQL reporting on the same storage.
* **Real-World Pipeline Scenario:**
  * Uber builds a **Data Lakehouse** using Apache Hudi on top of ADLS. They ingest raw ride logs (Data Lake style) but run Spark SQL reports and ML prediction models on conformed Hudi tables (Data Warehouse style) without duplicating data.

---

## 4. Batch vs Stream vs Micro-batch

| Dimension | Batch Processing | Stream Processing | Micro-batch Processing |
| :--- | :--- | :--- | :--- |
| **Latency** | Hours to Days | Milliseconds | Seconds (100ms to minutes) |
| **Data Boundaries** | Bounded (fixed size dataset) | Unbounded (continuous stream) | Bounded partitions of an unbounded stream |
| **Throughput** | Maximum (efficient resource use) | Low (system processes events individually)| Medium-High (batches small events) |
| **State Management**| Simple (resets on run) | Complex (requires state store) | Moderate (managed by checkpointing) |
| **Failure Recovery** | Re-run entire batch job | Replay and update state from broker | Resume from last committed checkpoint |

* **Decision Rule:**
  * Pick **Batch** for massive historical analytics, daily dashboard updates, or payroll processing.
  * Pick **Stream** for sub-second alerting, real-time fraud prevention, or live trade matching.
  * Pick **Micro-batch** when using Spark Structured Streaming to ingest web logs where 5-10 second latency is acceptable and high throughput is required.
* **Real-World Pipeline Scenario:**
  * Swiggy uses **Stream Processing** (Flink) to calculate delivery driver availability and ETA every second. They use **Batch Processing** (Spark) overnight to calculate driver payouts based on completed trips.

---

## 5. Star Schema vs Snowflake Schema

| Dimension | Star Schema | Snowflake Schema |
| :--- | :--- | :--- |
| **Normalization** | Denormalized (redundancy allowed) | Normalized (no redundancy) |
| **Join Complexity** | Simple (few joins, fact-to-dimension) | High (multiple joins across dimensions) |
| **Query Performance**| Extremely fast | Slow (due to complex multi-table joins) |
| **Storage Space** | Higher (stores duplicate string attributes) | Optimized and lower |
| **Maintenance** | Complex (updating values requires multi-row changes) | Simple (change a value in a single normalized row) |

* **Decision Rule:**
  * Pick **Star Schema** for modern columnar databases (Snowflake, BigQuery) where query speed is the absolute priority, as columnar storage handles denormalized text efficiently.
  * Pick **Snowflake Schema** in legacy environments where storage costs are high and dimensions have complex multi-level relationships that need strict transactional integrity.
* **Real-World Pipeline Scenario:**
  * A retail store builds a **Star Schema** with a central `fact_sales` table and a denormalized `dim_customers` table containing customer city, state, and country in single rows to ensure fast customer-segment reporting.

---

## 6. Parquet vs Avro vs ORC

| Dimension | Parquet | Avro | ORC |
| :--- | :--- | :--- | :--- |
| **Storage Layout** | Columnar | Row-based | Columnar |
| **Schema Location** | Embedded in the footer | Embedded in the header | Embedded in the stripe and footer |
| **Read Performance**| Excellent for analytical queries | Poor (must read full row) | Excellent for Hive/Presto analytical queries |
| **Write Performance**| Slow (must buffer columns in memory) | Extremely fast (appends rows directly) | Slow (buffers stripes before writing) |
| **Primary Ecosystem** | Apache Spark, AWS Athena, Delta Lake | Apache Kafka, Schema Registry | Apache Hive, Presto |

* **Decision Rule:**
  * Pick **Parquet** for read-heavy analytical pipelines using Spark or Snowflake.
  * Pick **Avro** for write-heavy streaming architectures using Kafka where schema conformance is checked for every message.
  * Pick **ORC** if your primary compute engine is Apache Hive on an enterprise Hadoop cluster.
* **Real-World Pipeline Scenario:**
  * An IoT tracking system writes sensor payloads to Kafka in **Avro** format (fast writes, safe schema). A Spark Streaming job reads these events and writes them to a Data Lake in **Parquet** format for downstream ML modeling (fast columnar scans).

---

## 7. RDD vs DataFrame vs Dataset in Spark

| Dimension | RDD (Resilient Distributed Dataset) | DataFrame | Dataset |
| :--- | :--- | :--- | :--- |
| **Type Safety** | Type-safe (checked at compile time) | Untyped (runtime errors only) | Type-safe (checked at compile time) |
| **Optimizer** | No (developer must optimize code) | Yes (Catalyst & Tungsten) | Yes (Catalyst & Tungsten) |
| **Programming Style**| Functional (map, flatMap, filter) | Declarative (select, groupBy, join) | Hybrid (functional on typed objects) |
| **Memory Efficiency**| Low (high JVM serialization overhead) | Excellent (stored in binary Off-Heap) | Moderate (object serialization overhead) |
| **Language Support** | Scala, Java, Python, R | Scala, Java, Python, R | Scala and Java only |

* **Decision Rule:**
  * Pick **DataFrame** for 95% of PySpark tasks, ensuring the Catalyst Optimizer handles physical execution optimization.
  * Pick **Dataset** when coding in Scala and building mission-critical pipelines that require compile-time verification of schemas.
  * Pick **RDD** only when writing custom low-level serialization code or transforming unstructured binaries.
* **Real-World Pipeline Scenario:**
  * In a Databricks notebook, a developer writes a PySpark **DataFrame** statement: `df.groupBy("store_id").agg(sum("sales"))`. Under the hood, Spark converts this into physical **RDD** tasks executed across executors, optimized via Catalyst.

---

## 8. Narrow vs Wide Transformation

| Dimension | Narrow Transformation | Wide Transformation |
| :--- | :--- | :--- |
| **Data Movement** | None (computed locally within partitions) | High (shuffles data across cluster nodes) |
| **Shuffle Boundary** | No | Yes (creates a new stage in the DAG) |
| **Memory Impact** | Low (uses JVM execution memory) | High (requires spill memory, disk caching) |
| **Performance** | Extremely fast | Slow (limited by network and disk I/O) |
| **Example APIs** | `filter()`, `map()`, `select()`, `drop()` | `groupBy()`, `join()`, `distinct()`, `repartition()` |

* **Decision Rule:**
  * Pick/Design **Narrow Transformations** whenever possible. Keep filtering and projection operations at the beginning of your pipeline to minimize the volume of data that goes into downstream wide transformations.
  * Use **Wide Transformations** only when business logic requires grouping or combining disparate keys across partitions.
* **Real-World Pipeline Scenario:**
  * A Spark job reads 1TB of web log data. It first runs `.filter(col("country") == "IN")` (Narrow) to reduce the data to 10GB, and then runs `.groupBy("user_id").count()` (Wide), avoiding shuffling the unfiltered 1TB of data.

---

## 9. Broadcast Join vs Sort-Merge Join (SMJ)

| Dimension | Broadcast Join | Sort-Merge Join (SMJ) |
| :--- | :--- | :--- |
| **Table Size Limit** | One table must be small (default < 10MB) | Both tables can be infinitely large |
| **Network Shuffle** | None (small table sent to all executors) | High (shuffles both tables on join keys) |
| **Disk Dependency** | None (in-memory lookup) | High (writes sort-shuffled files to local disk)|
| **Data Skew Impact** | Low (no shuffles occur) | High (skewed keys bottleneck worker nodes) |
| **Memory Risk** | High if the small table exceeds driver memory | Low (spills to disk if partitions exceed RAM) |

* **Decision Rule:**
  * Pick **Broadcast Join** when joining a massive fact table with a small dimension table that fits comfortably in executor memory (e.g., store metadata, currency rates).
  * Pick **Sort-Merge Join** when joining two large datasets that cannot fit in executor memory, relying on Spark to partition and sort the data.
* **Real-World Pipeline Scenario:**
  * Joining a `fact_transactions` DataFrame (500GB) with a `dim_payment_methods` DataFrame (5KB) in PySpark. By broadcasting the payment methods table, the job runs in 2 minutes instead of 45 minutes by bypassing a full network shuffle.

---

## 10. Partitioning vs Bucketing vs Sharding

| Dimension | Partitioning | Bucketing | Sharding |
| :--- | :--- | :--- | :--- |
| **Abstraction Level**| File system directory layout | File level hashing inside directories | Database cluster node partitioning |
| **Key Type** | Low cardinality (dates, regions) | High cardinality (user IDs, transaction IDs)| Primary/business distribution key |
| **File Count** | Dynamic (one folder per unique value) | Fixed (configured number of files) | Dynamic (depends on cluster nodes) |
| **Use Case** | Skipping folders during scans | Optimizing joins, preventing shuffles | Scaling writes on transactional databases |
| **Storage Type** | Data Lake / Object Stores | Data Lake / Distributed Tables | Relational / NoSQL Databases |

* **Decision Rule:**
  * Pick **Partitioning** to split data by date or country so queries can prune entire directories.
  * Pick **Bucketing** to organize high-cardinality columns (like `user_id`) into a fixed number of hash files to speed up joins.
  * Pick **Sharding** when scaling transactional databases (like PostgreSQL) to distribute read/write queries across multiple physical servers.
* **Real-World Pipeline Scenario:**
  * A company stores user orders. They **partition** the table by `order_date` (directory level). Inside each date directory, they **bucket** the data by `user_id` into 16 files to optimize downstream user-level joins.

---

## 11. ACID vs BASE

| Dimension | ACID (Atomicity, Consistency, Isolation, Durability) | BASE (Basically Available, Soft State, Eventual Consistency) |
| :--- | :--- | :--- |
| **Consistency style** | Immediate Consistency (all reads reflect writes) | Eventual Consistency (reads converge over time) |
| **Scalability** | Vertical (difficult to scale write clusters) | Horizontal (easy to scale across servers) |
| **Fault Tolerance** | Strong validation (rejects invalid writes) | High availability (allows writes during network splits) |
| **System Complexity**| Low for developers, high for database engine | High for developers (must handle stale data) |
| **Typical Databases** | PostgreSQL, MySQL, Snowflake | Cassandra, DynamoDB, MongoDB |

* **Decision Rule:**
  * Pick **ACID** for transactional ledgers, bank balances, or reservation tables where data must be immediately consistent and correct.
  * Pick **BASE** for social media counters, shopping carts, or high-volume log collectors where system uptime is more important than absolute consistency.
* **Real-World Pipeline Scenario:**
  * A bank uses an **ACID** database for processing transfers (ensuring money is never lost). However, their audit log pipeline dumps event telemetry into Cassandra (**BASE**), allowing high-velocity logs to write without delay.

---

## 12. Lambda vs Kappa Architecture

| Dimension | Lambda Architecture | Kappa Architecture |
| :--- | :--- | :--- |
| **Processing Layers** | Two: Batch Layer + Speed Layer | One: Speed (Streaming) Layer only |
| **Code Maintenance** | High (must maintain batch and streaming code) | Low (single streaming codebase) |
| **Backfill Method** | Re-run batch job on historical files | Replay historical events from message log |
| **Data Consistency** | High (batch layer corrects streaming errors) | Challenging (depends on stream state recovery)|
| **Complexity** | High (requires coordinating two pipelines) | Medium (requires a robust message replay log) |

* **Decision Rule:**
  * Pick **Lambda** in legacy setups where batch engines are highly optimized and streaming is only used for temporary approximations.
  * Pick **Kappa** for modern pipelines where Apache Kafka or Redpanda can retain historical data, and Flink/Spark Streaming can process all history.
* **Real-World Pipeline Scenario:**
  * An IoT pipeline migrated from **Lambda** (Spark Batch on S3 + Storm on Kafka) to **Kappa** (Apache Flink reading from Kafka with a 30-day retention policy), reducing their codebase size by 50% and ensuring consistent business logic.

---

## 13. Row-based vs Columnar Storage

| Dimension | Row-based Storage | Columnar Storage |
| :--- | :--- | :--- |
| **Layout on Disk** | Row 1, Row 2, Row 3 contiguously | Col 1, Col 2, Col 3 contiguously |
| **Write Speed** | Extremely fast (simple appends to disk pages)| Slower (must reconstruct file blocks) |
| **Read Speed** | Fast for reading individual full records | Fast for running aggregations on columns |
| **Compression Ratio**| Low (adjacent fields have different types) | High (adjacent fields have identical types) |
| **Data Skipping** | Not possible (must read whole row to check value) | Yes (uses metadata to skip unused columns) |

* **Decision Rule:**
  * Pick **Row-based** for transactional application databases (OLTP) that perform frequent insertions, single-record updates, and quick lookups.
  * Pick **Columnar** for analytical databases (OLAP) and data lakes where queries aggregate specific columns across billions of records.
* **Real-World Pipeline Scenario:**
  * MySQL stores user profiles in **row-based** format on disk for instant login validations. A daily replica script copies these profiles to Parquet (**columnar**) on S3 so marketing can run demographic aggregations.

---

## 14. Schema Enforcement vs Schema Evolution (Delta)

| Dimension | Schema Enforcement | Schema Evolution |
| :--- | :--- | :--- |
| **Write Action** | Rejects write if incoming schema does not match | Accepts write and updates the target schema |
| **Default Setting** | Enabled by default in Delta Lake | Disabled by default in Delta Lake |
| **PII / Quality Risk**| Low (prevents bad columns from entering) | High (typos in column names create duplicates) |
| **Pipeline Stability**| May break pipelines if schema changes upstream | Ensures pipeline runs, but downstream may break |
| **Syntax option** | Default behavior | `.option("mergeSchema", "true")` |

* **Decision Rule:**
  * Pick **Schema Enforcement** for production Gold and Silver tables to ensure downstream dashboards and BI tools do not break due to schema changes.
  * Pick **Schema Evolution** in Bronze/landing layers to gracefully handle changes in unstructured API payloads.
* **Real-World Pipeline Scenario:**
  * An e-commerce service changes the database field `customer_zip` to `zipcode`. The Bronze Delta table ingests it using **Schema Evolution**, but the Silver table throws an error due to **Schema Enforcement**, alerting the team.

---

## 15. Managed Table vs External Table

| Dimension | Managed Table (Databricks) | External Table (Databricks) |
| :--- | :--- | :--- |
| **Data Storage Location**| Default workspace warehouse path | Custom cloud storage URI (S3, ADLS) |
| **Metadata Location** | Hive Metastore / Unity Catalog | Hive Metastore / Unity Catalog |
| **Drop Table Action** | Deletes both metadata and physical files | Deletes metadata only; physical files persist |
| **File Management** | Handled automatically by Databricks | Handled manually by storage admins |
| **Sharing Ease** | Harder (tied to internal workspace storage) | Easy (external files accessed by multiple tools) |

* **Decision Rule:**
  * Pick **Managed Table** for internal, temporary, or intermediate tables in your Databricks workspace that do not need to be accessed by external tools.
  * Pick **External Table** for production datasets that need to be shared with other services (e.g., Snowflake, PowerBI) or where you want to protect data files from accidental deletion.
* **Real-World Pipeline Scenario:**
  * A team creates an **External Table** pointing to `s3://prod-analytics-gold/transactions/`. If a developer accidentally drops the table in Databricks, the physical Parquet data remains safe on S3, and the catalog table can be rebuilt instantly.

---

## 16. Airflow Local Executor vs Celery Executor vs K8s Executor

| Dimension | Local Executor | Celery Executor | Kubernetes (K8s) Executor |
| :--- | :--- | :--- | :--- |
| **Architecture** | Single VM runs scheduler and tasks | Multi-worker VM cluster with message broker | Dynamic Kubernetes cluster |
| **Scaling** | Vertical scaling only | Horizontal scaling (pre-provisioned VMs) | Dynamic auto-scaling (on-demand pods) |
| **Resource Isolation**| None (tasks share CPU and memory) | Moderate (tasks share worker node RAM) | Perfect (each task runs in its own pod) |
| **Task Start Latency** | Instantaneous | Low | High (requires pod startup time) |
| **Complexity** | Extremely low | Medium-High (requires Redis/RabbitMQ) | High (requires Kubernetes management) |

* **Decision Rule:**
  * Pick **Local** for local testing, small personal projects, or workflows with low task counts.
  * Pick **Celery** for predictable workloads with hundreds of daily tasks where minimal execution delay is critical.
  * Pick **Kubernetes** for variable workloads with diverse resource needs (e.g., GPU tasks vs light API calls) and where scaling down to zero is needed to save costs.
* **Real-World Pipeline Scenario:**
  * An enterprise uses **Kubernetes Executor** for their Airflow cluster. A task running a heavy machine learning model requests a pod with 32GB RAM and GPU resources, while a simple SQL execution task runs on a lightweight pod, optimizing cloud spend.

---

## 17. Append vs Upsert vs Merge in Delta Lake

| Dimension | Append | Upsert (Update-Insert) | Merge |
| :--- | :--- | :--- | :--- |
| **Data Action** | Appends incoming records to the table | Updates matching keys, inserts new ones | Evaluates join conditions to update, insert, or delete |
| **Execution Speed** | Extremely fast (no read-before-write) | Slow (reads target keys to check match) | Slow (performs full source-target join check) |
| **Deduplication** | No (allows duplicate records) | Yes (deduplicates on primary key) | Yes (deduplicates and can purge deleted rows)|
| **Use Case** | Raw log ingestion (Bronze layer) | Master record updating (Silver layer) | Dimension alignment, historical syncs |

* **Decision Rule:**
  * Pick **Append** for streaming ingest pipelines where you only want to write raw data quickly without checking for duplicates.
  * Pick **Upsert** when updating a flat record store (e.g., active user profiles) based on a simple lookup key.
  * Pick **Merge** when implementing complex business logic, such as synchronizing deleted records or building SCD Type 2 dimension tables.
* **Real-World Pipeline Scenario:**
  * An e-commerce pipeline uses **Append** to write incoming pageviews to Bronze. They use **Merge** in the Silver layer to update the `dim_users` table, checking if a user has changed their address or unsubscribed.

---

## 18. At-Least-Once vs At-Most-Once vs Exactly-Once

| Dimension | At-Least-Once | At-Most-Once | Exactly-Once |
| :--- | :--- | :--- | :--- |
| **Data Loss Risk** | Zero | High | Zero |
| **Duplication Risk** | High | Zero | Zero |
| **Performance Cost** | Low | Extremely Low (fire-and-forget) | High (two-phase commits, transactions) |
| **Implementation** | Retries enabled, offset commits after work | Retries disabled, offsets committed early | Kafka transactions + idempotent database sinks |
| **Typical Use Case** | Ad click telemetry, app performance logs | Live video streaming feed, sensor temp | Financial transactions, billing ledgers |

* **Decision Rule:**
  * Pick **Exactly-Once** when data correctness is critical and duplicates cannot be tolerated (e.g., billing, inventory management).
  * Pick **At-Least-Once** when you want to avoid data loss and can easily deduplicate data downstream.
  * Pick **At-Most-Once** when low latency is the priority and losing a few records has no business impact.
* **Real-World Pipeline Scenario:**
  * CRED uses **Exactly-Once** processing when tracking credit card bill payments. They use **At-Least-Once** for clickstream tracking, running a daily Spark batch job to remove duplicate pageview records.

---

## 19. Kafka vs RabbitMQ (Conceptual)

| Dimension | Apache Kafka | RabbitMQ |
| :--- | :--- | :--- |
| **Storage Architecture**| Distributed commit log (persisted to disk) | Smart broker, transient message queue (RAM/disk) |
| **Message Consumption**| Pull model (consumers request messages) | Push model (broker pushes to consumers) |
| **Data Retention** | Replayable (persists data after reading) | Transient (deletes message after ACK) |
| **Routing Options** | Simple (routing by topic partition keys) | Complex (direct, fanout, headers exchanges) |
| **Throughput Scale** | Extremely High (millions of events/sec) | Medium (tens of thousands of messages/sec) |

* **Decision Rule:**
  * Pick **Kafka** for stream processing, event-driven architectures, clickstream tracking, and system integration logs where data replayability is required.
  * Pick **RabbitMQ** for task distribution between microservices, background job routing, and complex request-response patterns.
* **Real-World Pipeline Scenario:**
  * A website uses **RabbitMQ** to send a "welcome email" task to a background worker when a user registers. Concurrently, it publishes a `user_registered` event to **Kafka** to update analytics tables and trigger recommendation engines.

---

## 20. Data Lake vs Delta Lake

| Dimension | Data Lake | Delta Lake |
| :--- | :--- | :--- |
| **Definition** | A storage repository (S3, ADLS) | A file storage format layer built on Parquet |
| **ACID Transactions** | No (handled in pipeline application code) | Yes (guaranteed by transaction logs) |
| **Data Formats** | Stores any file type (CSV, JSON, Images) | Stores data in Parquet files with JSON logs |
| **Time Travel** | No (requires manual directory versioning) | Yes (built-in out-of-the-box) |
| **Schema enforcement**| No (allows writing mismatched files) | Yes (enforces schema layout at write time) |

* **Decision Rule:**
  * Pick **Data Lake** to land raw, unstructured files (like PDFs, images, or raw database extracts).
  * Pick **Delta Lake** when building structured tables on top of your Data Lake that need updates, merges, schema validation, and concurrent access.
* **Real-World Pipeline Scenario:**
  * An analytics platform stores raw JSON payloads in an S3 **Data Lake** landing bucket. A Spark streaming job reads these files, applies validation, and writes them to a **Delta Lake** table, enabling ACID-compliant queries.

---

## 21. Delta Lake vs Apache Iceberg vs Apache Hudi

| Dimension | Delta Lake | Apache Iceberg | Apache Hudi |
| :--- | :--- | :--- | :--- |
| **Primary Creator** | Databricks | Netflix / Apache | Uber / Apache |
| **Metadata Management** | Directory log (`_delta_log/` JSON) | Hierarchical metadata files (snapshots) | Timeline log (`.hoodie/` commit logs) |
| **Table Types** | Unified (Parquet + Delta Log) | Unified (Parquet/ORC/Avro + Metadata) | Copy-on-Write (CoW), Merge-on-Read (MoR) |
| **Partition Evolution** | Manual (requires data rewrite) | Automatic (Partition Evolution via layout spec) | Manual (uses directory layouts) |
| **Concurrency Model** | Optimistic Concurrency Control (OCC) | Optimistic Concurrency Control (OCC) | OCC or Multi-Writer Lock Providers |
| **Optimized Engine** | Apache Spark (Tightly coupled) | Engine Agnostic (Spark, Trino, Flink, etc.) | Apache Spark and Flink |
| **Write Latency** | Low (direct Parquet commits) | Low (metadata updates) | Extremely Low (MoR appends log-based delta writes)|

* **Decision Rule:**
  * Pick **Delta Lake** if your primary execution environment is Databricks/Spark and you require tight integration with Spark SQL and local caching mechanisms.
  * Pick **Apache Iceberg** if you operate a multi-engine environment (e.g., Spark for ingestion, Trino for BI, Flink for stream-reads) and need dynamic partition evolution without rewriting data.
  * Pick **Apache Hudi** if you build streaming data lakehouses requiring low-latency incremental upserts/deletes, particularly using Merge-on-Read structures.
* **Real-World Pipeline Scenario:**
  * Netflix uses **Apache Iceberg** to query petabyte-scale data lakes with different engines (Spark, Trino, and Presto) sharing the same tables. In contrast, Uber uses **Apache Hudi** to stream real-time rides metadata directly to storage, utilizing Merge-on-Read tables to handle massive write volumes without locking read processes.

---

## 22. dbt Materializations: View vs Table vs Incremental vs Ephemeral

| Dimension | View | Table | Incremental | Ephemeral |
| :--- | :--- | :--- | :--- | :--- |
| **Physical Location** | Virtual database view | Physical database table | Physical database table | No physical table (nested SQL query) |
| **Build Execution** | Very fast (creates metadata query) | Slow (creates and writes full table) | Fast (appends only new/updated rows) | None (inlined into downstream SQL) |
| **Query Speed** | Slower (runs full query plan on read) | Fast (reads pre-calculated data) | Fast (reads pre-calculated data) | Depends on downstream query |
| **Storage Cost** | Zero | High (full data storage) | High (full data storage) | Zero |
| **Best Used For** | Fast development, light logic, dynamic data | Complex transformations, reports, ML inputs | Large historical event tables, daily updates | Intermediate steps that don't need sharing |

* **Decision Rule:**
  * Pick **View** for simple, low-volume logic or when the data changes constantly and must be real-time on read.
  * Pick **Table** for complex models that are queried frequently by BI tools (to avoid re-running transformation logic on every dashboard load).
  * Pick **Incremental** for high-volume logs, events, or transactions where fully rewriting the table on every run is too slow and expensive.
  * Pick **Ephemeral** for lightweight intermediate cleanup models that you do not want to clutter the database catalog.
* **Real-World Pipeline Scenario:**
  * In a dbt project, raw clickstream telemetry is built as an **Incremental** table (only new timestamps processed daily). It joins to a customer profile **View** (dynamic updates), and the output serves an executive **Table** for PowerBI.

---

## 23. Spark Operations: Repartition vs Coalesce

| Dimension | Repartition | Coalesce |
| :--- | :--- | :--- |
| **Data Shuffle** | Yes (full network shuffle across cluster) | No (minimizes data movement by merging) |
| **Partition Count** | Can increase or decrease partition count | Can only decrease partition count |
| **Equal Data Sizes** | Yes (distributes data evenly across partitions) | No (can lead to uneven, skewed partition sizes) |
| **Execution Cost** | High (expensive network and disk I/O) | Low (combines local partitions, very cheap) |
| **Performance Gain** | Resolves data skew, optimizes subsequent joins | Resolves small file problem before writing to storage |

* **Decision Rule:**
  * Pick **Coalesce** when you need to decrease the number of partitions (e.g. from 1000 down to 10) before writing data to disk to avoid the small file problem.
  * Pick **Repartition** when you need to increase partitions to scale-out processing, or when you have severe data skew and must shuffle partitions to balance worker loads.
* **Real-World Pipeline Scenario:**
  * After running a heavy filter on a 100-partition Spark DataFrame, only 2MB of data remains. Writing this directly produces 100 tiny files. Running `.coalesce(1).write.parquet(...)` merges partitions locally without a shuffle, writing a single clean Parquet file.

---

## 24. SQL Join Types: INNER vs LEFT vs FULL vs CROSS vs SELF

| Dimension | INNER JOIN | LEFT JOIN | FULL OUTER JOIN | CROSS JOIN | SELF JOIN |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Returned Rows** | Matching keys in both tables | All left rows + matching right rows | All rows from both tables | Cartesian product (Left Rows × Right Rows) | Matches matching records in same table |
| **Unmatched keys** | Discarded | Filled with NULL for right columns | Filled with NULL for missing sides | No keys used; all combinations returned | Depends on join condition used |
| **Cardinality risk** | Low | Low (unless duplicate keys on right) | Medium | Extremely High (creates massive datasets) | Medium |
| **Best Used For** | Strict associations (Order -> Customer) | Enriching data with optional lookups | Reconciling and auditing two datasets | Generating test grids, date hierarchies | Hierarchical structures (Employee -> Manager) |

* **Decision Rule:**
  * Pick **INNER JOIN** when you want to enforce strict matches (e.g. getting details of users who actually placed an order).
  * Pick **LEFT JOIN** when you want to preserve the left table entirely (e.g. listing all registered users regardless of whether they have made a purchase).
  * Pick **CROSS JOIN** only when you intentionally want to generate every possible combination of rows (e.g., matching all products with all regions).
* **Real-World Pipeline Scenario:**
  * An HR analytics pipeline uses a **SELF JOIN** on an `employees` table (`ON emp.manager_id = mgr.employee_id`) to display employees alongside their manager names. Concurrently, it uses a **LEFT JOIN** to load department details so that departments with zero active employees are not dropped from reporting.

---

## 25. dbt compile vs dbt run

| Dimension | dbt compile | dbt run |
| :--- | :--- | :--- |
| **Execution Action** | Generates raw SQL from model files | Executes compiled SQL on target warehouse |
| **Database Writing** | No (read-only verification/generation) | Yes (creates or replaces tables/views) |
| **Primary Output** | Raw SQL files in `target/compiled/` | Live table/view schemas in the warehouse |
| **Speed** | Extremely fast (local execution only) | Slower (bottlenecked by database compile/write) |

* **Decision Rule:**
  * Use **dbt compile** when checking Jinja syntax, validating model relationships, or exporting raw SQL files for manual debugging.
  * Use **dbt run** when deploying changes, refreshing schemas, and populating tables in staging or production warehouses.
* **Real-World Pipeline Scenario:**
  * During CI/CD checks, a PR runner executes `dbt compile` to ensure there are no syntax errors or circular references in the code. Once the PR is merged, the deployment script executes `dbt run` to build the new conformed tables.

---

## 26. dbt sources vs refs

| Dimension | `{{ source() }}` | `{{ ref() }}` |
| :--- | :--- | :--- |
| **Reference Target** | Raw ingestion tables outside dbt project | Downstream compiled model inside dbt project |
| **Lineage Role** | Starts the data lineage graph | Links intermediate models to build the DAG |
| **Environment Aware**| Static (points to predefined source schema) | Dynamic (resolves schema based on target env) |
| **Execution Order** | Represents static landing checkpoints | Defines dynamic run order dependencies |

* **Decision Rule:**
  * Pick **`source()`** when referencing raw files or staging tables loaded directly by data loaders (Fivetran/Airbyte) before applying transformation rules.
  * Pick **`ref()`** when referencing conformed tables or views generated by other models in your active dbt project, ensuring correct execution order.
* **Real-World Pipeline Scenario:**
  * A staging model `stg_payments.sql` reads raw Stripe rows using `{{ source('stripe', 'payment_events') }}`. Downstream, a conformed model `fact_orders.sql` joins staging tables by reading `{{ ref('stg_payments') }}`.

---

## 27. Airflow TaskFlow API vs Classic Operators

| Dimension | TaskFlow API (Decorators) | Classic Operators (e.g. `PythonOperator`) |
| :--- | :--- | :--- |
| **Syntax Style** | Python decorators (`@dag`, `@task`) | Direct operator object instantiation |
| **Data Sharing (XCom)** | Implicit (automatic return value sharing) | Explicit (requires `xcom_pull` and `xcom_push` calls) |
| **Dependency Mapping** | Implicit (based on Python function calls) | Explicit (using bitwise operators like `>>` and `<<`) |
| **Boilerplate Code** | Extremely low | Medium-High |

* **Decision Rule:**
  * Pick **TaskFlow API** for python-heavy operations where you are passing variables, data paths, or simple counts between tasks, as it reduces boilerplate.
  * Pick **Classic Operators** when executing system tasks (like executing external Spark jobs via `DatabricksSubmitRunOperator` or triggering Bash scripts), as they are pre-built templates.
* **Real-World Pipeline Scenario:**
  * An Airflow DAG uses the **TaskFlow API** to fetch database config, passing it implicitly to a downstream parsing task. It then uses a **Classic Operator** (`SQLExecuteQueryOperator`) to run the final conformed update query.

---

## 28. Airflow Task Group vs SubDAG

| Dimension | Task Group | SubDAG (Deprecated) |
| :--- | :--- | :--- |
| **Scheduling Loop** | Runs on the parent DAG scheduler loop | Runs on a separate, nested scheduler loop |
| **Resource Overhead** | None (UI-only grouping mechanism) | High (executes as a separate nested execution run) |
| **Deadlock Risk** | Zero | High (SubDAG tasks can consume worker slots, stalling parent) |
| **UI Interaction** | Clean (expandable/collapsible boxes) | Complex (requires drilling down into a separate frame) |

* **Decision Rule:**
  * Pick **Task Group** for 100% of workflows where tasks must be grouped visually for UI cleanups and logical separation.
  * Avoid **SubDAGs** entirely, as they are officially deprecated due to performance overheads and deadlock risks.
* **Real-World Pipeline Scenario:**
  * An e-commerce pipeline groups its 15 raw ingestion tasks into an "Ingest" **Task Group** and its 20 audit tasks into an "Audit" **Task Group**, keeping the main parent DAG visual interface clean and readable.

---

## 29. Airflow Execution Date (Logical Date) vs Current Date

| Dimension | Execution Date / Logical Date (`{{ ds }}`) | Current Date (`datetime.now()`) |
| :--- | :--- | :--- |
| **Time Reference** | The start of the schedule interval being processed | The actual wall-clock system time of run execution |
| **Value Nature** | Deterministic (constant for a specific DAG run) | Volatile (changes on every retry or execution attempt) |
| **Backfill Support** | Perfect (reprocesses historical date partitions correctly) | Fails (reprocesses current date, causing duplicate data) |
| **Target Partition** | Points to the event date partition (e.g. `2026-06-25`) | Points to the current date of run (e.g. `2026-06-26`) |

* **Decision Rule:**
  * Always use **Execution Date / Logical Date (`{{ ds }}`)** inside database partition paths and query filters to ensure tasks are idempotent and can be safely backfilled.
  * Avoid using **Current Date (`datetime.now()`)** for date filters, as running a backfill for a month ago will incorrectly filter for today's date instead.
* **Real-World Pipeline Scenario:**
  * A daily partition write uses `replaceWhere = "date = '{{ ds }}'"`. If the pipeline fails on Monday and is retried on Wednesday, it successfully overwrites only Monday's partition instead of Wednesday's.


