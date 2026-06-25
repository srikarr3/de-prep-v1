# Section 8: Rapid-Fire Definitions (The "Quick, What Is..." Round)

---

### 1. Predicate Pushdown
Predicate Pushdown is an optimization where the filtering of data (the `WHERE` clause) is moved (pushed down) as close to the physical storage layer as possible. Instead of reading all data blocks into Spark's execution memory and filtering them there, the query engine evaluates the filtering condition at the storage level, reading only relevant rows. In columnar formats like Parquet, metadata in the file footer (such as min/max statistics) is used to skip entire row groups that do not match the filter, reducing disk I/O and CPU utilization.

### 2. Projection Pushdown
Projection Pushdown is a query optimization that limits the fields retrieved from storage to only the specific columns projected (selected) in the query (the `SELECT` clause). In columnar file formats (like Parquet or ORC), columns are stored contiguously on disk. Applying projection pushdown allows the query planner to bypass loading data blocks for unselected columns, reducing memory consumption, network transfer overhead, and disk read latency.

### 3. Data Skew
Data Skew is a condition in distributed processing where some partitions contain a disproportionately larger volume of data than others. Because Spark distributes tasks across partitions, the executor handling the skewed partition takes significantly longer to complete its work, leaving other executors idle. Data skew can be identified in the Spark UI when the maximum task duration in a stage is significantly higher than the median task duration. It is often caused by joining on high-cardinality keys that contain many duplicate or default null values.

### 4. Salting (in Spark)
Salting is an optimization technique used to resolve data skew during join or aggregation operations. It works by appending a random integer (the "salt") to the join key of the skewed dataset, and replicating the matching key in the lookup dataset to correspond to every possible salt value. This artificial modification redistributes the skewed key across multiple partitions, allowing Spark to process the join in parallel across workers and preventing single-executor bottlenecks.

### 5. Broadcast Variable
A Broadcast Variable is a read-only variable that Spark caches on each worker node rather than shipping a copy of it with every individual task. When a task requires static lookup data (such as postal code mappings or tax tables), the driver broadcasts the variable once to the executors using an efficient P2P protocol. This reduces network overhead and serialization costs during distributed task execution.

### 6. Accumulator
An Accumulator is a write-only, shared variable in Spark that can only be added to using an associative and commutative operation (such as counts, sums, or custom aggregations). Executors write update metrics to the accumulator, but only the driver process is permitted to read its final accumulated value. They are used for debugging, monitoring progress, or counting corrupted records during a transformation run.

### 7. Executor Memory vs Driver Memory
Driver Memory is the RAM allocated to the Driver master process, which compiles the DAG, schedules tasks, manages the Spark UI, and coordinates execution metadata. Executor Memory is the RAM allocated to worker processes, which run the actual tasks, manage partition data, and execute joins and aggregations. Allocating insufficient driver memory causes Out-Of-Memory (OOM) errors during `.collect()` actions, while low executor memory leads to execution disk spills.

### 8. GC Pressure in Spark
Garbage Collection (GC) Pressure refers to the CPU overhead incurred when the Java Virtual Machine (JVM) pauses task execution to scan and clean up expired objects in heap memory. In Spark, creating millions of short-lived Java objects during transformations (such as processing data row-by-row or using nested structures) triggers frequent GC cycles. GC pressure can be mitigated by using serialized caching, configuring G1GC, or using the Tungsten engine, which manages data off-heap in raw bytes.

### 9. Tombstone Record
A Tombstone Record is a marker message published to a topic or database indicating that a specific key has been deleted. In Apache Kafka or Change Data Capture (CDC) streams, a tombstone record is created by writing a message with a non-null key and a null payload. When log compaction runs, the broker identifies the tombstone record and deletes both the tombstone and all previous historical messages matching that key from disk.

### 10. Compaction
Compaction is a data management process that reorganizes fragmented data files to optimize read performance and storage footprints. In database engines (like RocksDB or Delta Lake), compaction merges multiple small write-ahead logs or Parquet files into fewer, larger, standardized files. This resolves the small file problem and improves query scan efficiency by reducing file-open operations and metadata overhead.

### 11. Small File Problem
The Small File Problem occurs when a data lake or distributed storage system contains a large number of files that are significantly smaller than the block size of the system (typically under 100MB). Each small file requires a metadata entry in the directory catalog, causing query engines to spend more time opening, closing, and coordinating file transfers than reading the actual data. It is resolved by running compaction commands like Delta's `OPTIMIZE`.

### 12. Z-Ordering
Z-Ordering is a multidimensional clustering algorithm used in Delta Lake to organize data contiguously based on selected columns. By mapping multi-dimensional data points to a one-dimensional space, Z-ordering co-locates related data within the same physical files. This maximizes the efficiency of data skipping during queries, as the engine can skip files that do not overlap with the query's min/max statistics.

### 13. Bloom Filter (in Delta)
A Bloom Filter is a space-efficient, probabilistic data structure used to test set membership (i.e., whether an element is in a dataset). In Delta Lake, a Bloom filter can be enabled on specific columns. It determines whether a specific search value exists within a Parquet file without reading the file. It can return false positives (indicating the value might be in the file) but never false negatives (guaranteeing the value is not in the file, allowing the engine to skip the file entirely).

### 14. Liquid Clustering
Liquid Clustering is a dynamic data layout optimization in Delta Lake that replaces traditional partitioning and Z-ordering. Instead of writing data into rigid, nested directories (which can lead to over-partitioning), Liquid clustering continuously reorganizes the data dynamically based on clustering keys. This simplifies partition management and ensures query performance remains consistent as data scale and access patterns change.

### 15. Photon Engine (Databricks)
Photon is a vectorised query execution engine built by Databricks in C++ to replace the JVM-based execution layer for Spark SQL. Photon processes data in batches of rows using data parallelism (SIMD) at the hardware level, bypassing JVM serialization, garbage collection, and runtime translation overhead. This results in faster query execution times for analytical SQL and ETL workloads.

### 16. Hive Metastore vs Unity Catalog
The Hive Metastore (HMS) is a legacy metadata catalog that stores schema definitions, column data types, and physical directory paths for tables in a cluster. Unity Catalog is a modern, unified governance engine for Databricks. It goes beyond simple schema mapping to provide centralized access controls (row/column masking), search features, data lineage tracking, and audit logging across multiple workspaces and storage accounts.

### 17. External Location (Unity Catalog)
An External Location is a Unity Catalog object that associates a cloud storage path (such as an S3 bucket or Azure ADLS Gen2 path) with an IAM credential (storage credential). This allows administrators to grant users access to specific cloud storage directories using standard SQL `GRANT` commands, without exposing raw cloud access keys or SAS tokens in cluster configurations.

### 18. Service Principal (Azure)
A Service Principal is an identity created in Microsoft Entra ID (Azure AD) for use with automated tools, applications, and pipelines. It functions as a system service account, allowing pipelines (like Azure Data Factory or Databricks) to authenticate and access Azure resources (like ADLS Gen2 storage accounts) securely using token exchanges, without tying permissions to a physical user account.

### 19. Managed Identity
A Managed Identity is a feature in Microsoft Entra ID (Azure AD) that provides Azure resources (such as virtual machines, Databricks clusters, or ADF instances) with an automatically managed identity. It eliminates the need for developers to manage credentials, connection strings, or certificates in code. The resource uses this identity to authenticate directly with other Azure services.

### 20. ADLS Gen2
Azure Data Lake Storage Gen2 (ADLS Gen2) is Microsoft's primary object storage platform for big data analytics. It combines the scalability and low cost of Azure Blob Storage with a **Hierarchical Namespace (HNS)**. The hierarchical namespace organizes files into directories and subdirectories, converting virtual directory paths into physical file paths. This speeds up directory renames and metadata operations compared to flat object stores.

### 21. Medallion vs Data Vault (Conceptual Difference)
Medallion Architecture is a data lifecycle framework that structures data into Bronze (raw), Silver (cleansed/conformed), and Gold (aggregated) layers to refine data quality sequentially. Data Vault is a database modeling methodology that organizes relational structures into Hubs (business keys), Links (relationships), and Satellites (temporal descriptive attributes) to handle changing source schemas cleanly. Medallion describes pipeline flow; Data Vault describes structural modeling.

### 22. Write-Audit-Publish (WAP) Pattern
The Write-Audit-Publish (WAP) pattern is a data quality design pattern that prevents corrupt data from reaching production tables. It works by writing transformed data to a temporary staging location (Write), running automated data quality validation assertions against this staging area (Audit), and moving the data to the final production table only if all checks pass (Publish).

### 23. Dead Letter Queue (DLQ)
A Dead Letter Queue (DLQ) is a service queue where messages that fail validation, serialization, or processing rules are routed. Instead of crashing the active streaming pipeline when a malformed record is received, the message is written to the DLQ alongside execution metadata. This allows the pipeline to continue processing valid messages while developers debug the failures.

### 24. Poison Pill Message (Kafka)
A Poison Pill Message is a malformed or corrupted message published to a Kafka topic that causes consumer applications to crash during deserialization or processing. Because the consumer cannot parse the message, it fails to commit the offset. When the consumer restarts, it attempts to read the same message again, creating an infinite crash loop. It is resolved by implementing error-handling blocks that route poison pills to a Dead Letter Queue.

### 25. Consumer Lag (Kafka)
Consumer Lag is the difference between the latest offset written by producers to a partition and the offset currently read and committed by the consumer group. High consumer lag indicates that the consumer application cannot keep up with the volume of incoming messages, causing processing delays. It is resolved by scaling out the consumer group or optimizing the consumer's write operations.

### 26. Kafka Retention Policy
Kafka Retention Policy defines how long messages are retained on disk within a partition before being deleted. Retention can be configured based on time (e.g., retain messages for 7 days) or size (e.g., retain up to 10GB per partition). Once these limits are exceeded, Kafka purges the oldest segments from disk, regardless of whether they have been read by active consumers.

### 27. Log Compaction (Kafka)
Log Compaction is a data retention policy in Kafka that ensures the topic retains at least the last known value for each message key within a partition log. Instead of deleting old messages based on time, Kafka scans log segments in the background and removes duplicate keys, retaining only the latest state. This is useful for storing state changes (like user profile updates) where historical transition logs are not required.

### 28. Trigger.Once vs Trigger.AvailableNow (Spark Streaming)
`Trigger.Once` is a legacy Spark streaming execution mode that processes a single micro-batch of available data and then stops the query. `Trigger.AvailableNow` is a modern alternative that processes all available data from the source, splitting it into multiple micro-batches based on rate limits before stopping. Both enable running streaming pipelines as cost-efficient batch jobs on ephemeral clusters.

### 29. Structured Streaming vs DStreams
DStreams (Discretized Streams) was Spark's original streaming API, built directly on top of RDDs and processed as mini-batches of Java objects. Structured Streaming is the current, declarative streaming engine built on top of Spark SQL and DataFrames. It leverages the Catalyst Optimizer and Tungsten engine, supports event-time processing with watermarks, and provides a unified interface for both batch and streaming queries.

### 30. Stateful vs Stateless Streaming
Stateless Streaming is a processing pattern where each record is processed independently, requiring no historical context (e.g., filtering out events where status is 'fail'). Stateful Streaming is a pattern where processing a record depends on previous events, requiring the engine to maintain state memory across micro-batches (e.g., calculating a running average or tracking active sessions).

### 31. Window Operations: Tumbling, Sliding, Session
* **Tumbling Windows:** Fixed-size, non-overlapping time intervals (e.g., every 10 minutes, 10:00-10:10, 10:10-10:20).
* **Sliding Windows:** Fixed-size, overlapping time intervals that slide forward by a defined step (e.g., a 10-minute window that updates every 5 minutes).
* **Session Windows:** Dynamic, user-specific windows that group events based on activity, closing when no new events occur within a defined gap period.

### 32. SLA vs SLO vs SLI (in Data Pipelines)
* **SLI (Service Level Indicator):** The quantitative metric used to measure pipeline performance (e.g., the daily pipeline finish time).
* **SLO (Service Level Objective):** The target target threshold agreed upon for the SLI (e.g., pipeline finish time must be before 7:00 AM for 99% of days in a month).
* **SLA (Service Level Agreement):** The commitment that defines the consequences (such as alerts or penalties) if the SLO is not met.

### 33. Data Observability
Data Observability is the practice of monitoring, alerting, and understanding the health of data assets throughout the entire pipeline lifecycle. It goes beyond simple pipeline execution tracking to monitor data quality metrics, volume changes, lineage drift, freshness delays, and schema changes. This helps teams identify and resolve data issues before they affect downstream users.

### 34. Circuit Breaker Pattern (Pipelines)
The Circuit Breaker pattern is a design pattern that halts downstream pipeline execution when a critical upstream failure or data quality issue is detected. If a step (such as an ingestion check) fails to meet defined thresholds, the "circuit opens," stopping downstream transformation tasks. This prevents bad data from corrupting Gold tables and avoids wasting cloud compute costs on invalid runs.

### 35. Backpressure
Backpressure is a flow-control mechanism in streaming engines that slows down data ingestion when downstream sinks or transformations cannot keep up with incoming message volume. By signaling the source to slow down reads (e.g., limiting the maximum offsets processed per trigger in Spark), backpressure prevents out-of-memory errors and keeps worker nodes stable during traffic spikes.

### 36. Fan-Out Pattern
The Fan-Out pattern is an architectural design where an incoming message or event is replicated and routed to multiple target destinations or consumers simultaneously. In data pipelines, this is achieved by publishing an event to a Kafka topic, allowing multiple downstream applications (such as real-time dashboards, audit logs, and fraud detection engines) to consume the same event independently.

### 37. Orchestration vs Choreography
Orchestration is a design pattern where a centralized coordinator (like Apache Airflow) manages task execution, dependencies, and error handling step-by-step. Choreography is a decentralized pattern where individual microservices react independently to event triggers (such as a database update publishing a Kafka message) without a central controller, coordinating workflows using event subscriptions.

### 38. Sidecar Pattern
The Sidecar pattern is an architectural design that attaches an auxiliary helper container next to the main application container to handle supporting tasks. In data engineering, sidecars are used to manage service credentials, stream container logs to central collectors, or run data quality checks on local file systems without polluting the main pipeline application code.

### 39. Slowly Changing Fact
A Slowly Changing Fact is a fact table design pattern that handles situations where historic metrics change or require updates after the event has occurred. Unlike standard append-only fact tables, slowly changing facts are updated to maintain accuracy (such as updating an order's status from 'processing' to 'shipped' and adjusting return estimates), using merge statements or versioned records.

### 40. Junk Dimension
A Junk Dimension is a single dimension table that groups multiple low-cardinality, unrelated flags, indicators, and status codes (such as `is_active`, `payment_status`, and `shipping_method`) together. This avoids creating separate dimension tables for each flag, reducing the number of joins in the database and simplifying the star schema structure.

### 41. Outrigger Dimension
An Outrigger Dimension is a dimension table that joins to another dimension table rather than joining directly to the central fact table. This is used in star schemas when a dimension has descriptive details (such as a customer's company information) that are shared across multiple records, normalizing the schema slightly to reduce storage overhead.

### 42. Dynamic Partition Pruning (DPP)
Dynamic Partition Pruning is an optimization where Spark dynamically skips reading partition directories at run-time based on filter criteria evaluated from a joined dimension table, reducing disk I/O and network transfer.

### 43. Adaptive Query Execution (AQE)
Adaptive Query Execution is a Spark 3+ optimization framework that re-optimizes physical query execution plans (e.g., coalescing shuffle partitions, resolving skewed joins, and converting sort-merge joins to broadcast joins) dynamically at runtime using stage execution statistics.

### 44. Vectorized Execution
Vectorized Execution is an optimization in query engines (like Trino, Photon, or Snowflake) where data operations are applied to arrays of rows (vectors) simultaneously, utilizing CPU hardware-level SIMD operations rather than loop-based row-by-row JVM compilation.

### 45. Schema-on-Read vs Schema-on-Write
Schema-on-Write requires data structures and types to be validated and enforced when writing to storage (e.g., Relational Databases); Schema-on-Read allows writing unstructured raw data and parsing/structuring it dynamically when executing a read query (e.g., Data Lakes).

### 46. Surrogate Key
A Surrogate Key is a system-generated, unique identifier (typically a sequential integer or auto-incrementing key) added to a dimension table, serving as its primary key to isolate the warehouse from volatile source operational keys (natural keys).

### 47. Degenerate Dimension
A Degenerate Dimension is a dimension key (such as an invoice number or order ID) stored directly in the fact table without joining to a corresponding dimension table, because it has no additional descriptive attributes.

### 48. Write-Ahead Log (WAL)
A Write-Ahead Log is an append-only, durable log structure where database updates are written sequentially before being committed to main tables. It ensures durability and atomicity by allowing transactions to be rolled back or recovered after server crashes.

### 49. Two-Phase Commit (2PC)
A Two-Phase Commit is a distributed transaction consensus protocol that guarantees atomicity across multiple nodes by running a Prepare phase (where nodes vote to commit or abort) followed by a Commit phase (where the coordinator commits if all votes were positive).

### 50. Strong Consistency vs Eventual Consistency
Strong Consistency guarantees that every read operation returns the result of the absolute latest completed write; Eventual Consistency allows temporary read drift across database nodes, guaranteeing that they will converge to match eventually when writes stop.

### 51. Data Lineage
Data Lineage is the process of tracking and visualizing the lifecycle, movement, and transformations that data undergoes from its original source systems down to final dashboards, enabling auditability, root-cause debugging, and impact analysis.

### 52. Data Governance
Data Governance is the framework of policies, access controls, catalogs, and standards that defines who owns data, who can access it (security/privacy), and how data quality and metadata compliance are maintained across an enterprise.

### 53. Idempotent Consumer (in Kafka)
An Idempotent Consumer is a message receiver pattern designed to handle duplicates safely by checking if a message's unique ID has already been processed in a transactional tracking store (e.g. database or Redis) before executing any state modifications.

### 54. Spark Task Spill (Memory vs Disk)
Task Spill occurs when a Spark worker running a join, sort, or aggregation runs out of executor execution memory (RAM) and is forced to dump active partitions onto the local worker disk. Spill (Memory) is the size of the data in memory uncompressed, while Spill (Disk) is the size of that data when compressed and written to disk. It dramatically slows down execution.

### 55. Compactor Pattern
The Compactor Pattern is a data lakehouse management layout where a background service periodically merges (compacts) hundreds of tiny real-time streaming Parquet files into a few large files (typically 128MB–500MB), solving the small file problem and restoring fast read query scans.

