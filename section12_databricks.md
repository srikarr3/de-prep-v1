# Section 12: Databricks & Delta Lake Must-Knows

---

## 🏛️ Core Definitions (The "Define This" Round)

### Databricks Workspace
* **What it is:** A collaborative, cloud-based platform that aggregates data engineering, data science, and business analytics services.
* **How it works:** Integrates notebook development environments, multi-node Spark cluster managers, Delta Lake catalogs, and native pipeline execution engines.
* **Where it's used:** Processing petabyte-scale data lakes, running distributed SQL modeling, and orchestrating Databricks Workflows in the cloud.
* **Say this in the interview:** *"Databricks Workspace is a collaborative, cloud-based platform that integrates Apache Spark compute, managed notebooks, task workflows, and Unity Catalog data governance."*

### Delta Lake
* **What it is:** An open-source storage format layer that provides ACID compliance on top of data lakes.
* **How it works:** Encapsulates standard Parquet files with an append-only JSON transaction log to govern data read/write consistency.
* **Where it's used:** Used as the storage layer for Silver and Gold tables to support upserts, deletes, and concurrent queries on cloud object storage.
* **Say this in the interview:** *"Delta Lake is an open-source ACID storage layer built on top of Parquet. It tracks files using a JSON transaction log to enable transactions, time travel, and schema enforcement."*

### Delta Transaction Log
* **What it is:** The single source of truth log repository tracking all data file changes in a Delta table.
* **How it works:** Saved in a `_delta_log/` subdirectory as sequential JSON commit files. Tracks physical Parquet files added or deactivated on every transaction commit.
* **Where it's used:** Used by readers to construct consistent table versions and execute Time Travel queries.
* **Say this in the interview:** *"The Delta Transaction Log is an append-only sequence of JSON files that serves as the single source of truth, detailing file creations and deletions for atomic consistency."*

### All-Purpose Cluster
* **What it is:** A persistent Spark cluster used for ad-hoc queries, interactive notebook executions, and workspace development.
* **How it works:** Scaled and managed manually in the workspace, remaining online to execute user jobs in real-time. High DBU usage rates.
* **Where it's used:** Used during development sandboxing, exploratory data analysis, and dashboard model prototypes.
* **Say this in the interview:** *"An All-Purpose Cluster is an interactive Spark compute node used for collaborative development, notebook execution, and debugging, charging higher usage rates."*

### Job Cluster
* **What it is:** A transient, task-specific Spark cluster created dynamically by orchestration workflows.
* **How it works:** Databricks spins up the cluster automatically when a scheduled pipeline starts, runs the code, and terminates the nodes upon completion.
* **Where it's used:** Scheduling daily/hourly production workflows to save up to 70% in cloud resource fees.
* **Say this in the interview:** *"A Job Cluster is a transient Spark compute node created dynamically to run a specific automated workflow task, terminating upon completion to save up to 70% in costs."*

### Databricks AutoLoader
* **What it is:** An optimized file ingestion tool used to load millions of incoming files into Delta tables in real-time.
* **How it works:** Integrates `cloudFiles` streaming format, utilizing directory listing tracking or pub/sub notifications to discover files without directory scans.
* **Where it's used:** Ingesting high-velocity JSON, CSV, or XML event logs landing on S3 or Azure ADLS landing zones.
* **Say this in the interview:** *"AutoLoader is a file ingestion tool that processes new landing files streaming from cloud storage using directory listing scans or pub/sub notifications."*

### Change Data Feed (CDF)
* **What it is:** A Delta table property that captures and logs row-level mutations (inserts, updates, deletes).
* **How it works:** Once enabled, writes change log files alongside standard Parquet files, exposing transaction before/after values.
* **Where it's used:** Downstream incremental consumers that need to process only table updates rather than performing full table scans.
* **Say this in the interview:** *"Change Data Feed captures row-level updates, inserts, and deletes on a Delta table, storing change logs to enable downstream incremental consumption."*

### Shallow Clone
* **What it is:** A metadata-only copy of an existing Delta table.
* **How it works:** Copies the transaction log files to the target path without duplicating the underlying physical Parquet files.
* **Where it's used:** Instantly copying massive production datasets to development schemas for script dry-runs.
* **Say this in the interview:** *"A Shallow Clone replicates only a Delta table's metadata transaction log, referencing the source Parquet files to create an instant copy without duplication costs."*

### Deep Clone
* **What it is:** A complete duplicate replication of both table metadata and physical data.
* **How it works:** Copies transaction logs and writes all physical Parquet data files to the target storage location.
* **Where it's used:** Staging schema isolation, disaster recovery backups, or migration across cloud storage buckets.
* **Say this in the interview:** *"A Deep Clone creates a complete copy of both metadata and physical Parquet files, yielding an independent table suitable for sandboxes or backups."*

### Z-Ordering
* **What it is:** A data layout clustering technique that groups related records in the same physical files.
* **How it works:** Organizes column values multi-dimensionally within files, allowing Spark to utilize file statistics to skip data scans during query execution.
* **Where it's used:** Compaction tuning on high-cardinality search columns (like customer IDs or locations).
* **Say this in the interview:** *"Z-Ordering co-locates related data within the same physical Parquet files based on high-cardinality keys, maximizing query performance via data skipping."*

### Liquid Clustering
* **What it is:** A dynamic data clustering format that optimizes data layouts based on query keys.
* **How it works:** Bypasses static directory partitions, managing data placement dynamically on write, and supporting cluster key changes without rewriting files.
* **Where it's used:** Modern Delta tables with highly volatile query patterns or high-cardinality keys where static partitioning fails.
* **Say this in the interview:** *"Liquid Clustering replaces static directory partitioning by clustering files dynamically based on query keys, eliminating the small-file problem and allowing partition evolution."*

### Unity Catalog
* **What it is:** A unified data governance catalog providing access control, data lineage, and auditing.
* **How it works:** Implements permission rules on database catalog objects (tables, views, connections), tracking row/column masks and lineage graphs.
* **Where it's used:** Enforcing data access compliance and column masking policies across Databricks workspaces.
* **Say this in the interview:** *"Unity Catalog is Databricks' unified data governance tool, offering fine-grained tables access control, lineage tracing, and row/column security masks."*

### Delta Live Tables (DLT)
* **What it is:** A declarative ETL framework used to build and monitor streaming data pipelines.
* **How it works:** Users declare target table specifications in SQL or Python. DLT manages cluster scaling, dependency ordering, and data quality constraints automatically.
* **Where it's used:** Streamlining ingestion and transformation flows with automated lineage maps and schema validation metrics.
* **Say this in the interview:** *"Delta Live Tables is a declarative framework that automates the deployment, schema orchestration, and monitoring of data processing pipelines."*

---

## 🔄 Intermediate Concepts

### Databricks AutoLoader Code Structure
```python
# AutoLoader streaming read in Databricks
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "s3://my-lake/schemas/users")
      .load("s3://my-lake/raw/users/"))
```

### Delta Lake Concurrency & Locking
Delta Lake uses **Optimistic Concurrency Control (OCC)**. 
* When multiple workers write to a Delta table, they write staging files and attempt to commit to the transaction log.
* If a conflict occurs (two jobs update the same partition), Delta Lake raises a concurrent write exception and rolls back the second transaction.

---

## ⚡ Advanced Concepts

### Change Data Feed (CDF) Implementation
```sql
-- Enable Change Data Feed on a table
ALTER TABLE my_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Query changes between commits
SELECT * FROM table_changes('my_catalog.db.my_table', 1, 5);
```

### Table Replication Footprints
* **Deep Clone:** Compares and copies both files and metadata. Completely decoupled.
* **Shallow Clone:** Zero data copies. It relies on target files staying intact. Useful for staging test runs.

---

## 🏢 Practical Databricks Interview Questions

### Q1: What is the difference between `OPTIMIZE` and `VACUUM`? Why is running `VACUUM table RETAIN 0 HOURS` dangerous?
> **Answer:** "`OPTIMIZE` compacts small files into larger ones for read efficiency, leaving old files logically deactivated but physically on disk. `VACUUM` physically purges those deactivated files to free space.
> 
> Running `VACUUM RETAIN 0 HOURS` is dangerous because it deletes all historical Parquet files immediately. If any concurrent query is reading from older versions, or a streaming job is writing to the table, the job will fail with a `FileNotFoundException` because files were deleted mid-execution."

### Q2: Explain Databricks AutoLoader and how it differs from Structured Streaming file reads.
> **Answer:** "Standard Structured Streaming requires directory listings to identify new files. When directories scale to millions of files, listing becomes slow and runs out of executor memory.
> 
> AutoLoader (`cloudFiles`) uses two modes: Directory Listing (with state tracking) and File Notification (hooking into SQS/SNS to receive push events when files land). It scales to millions of files without directory list bottlenecks."

### Q3: When do you choose a Deep Clone over a Shallow Clone in Databricks?
> **Answer:** "I choose a **Shallow Clone** when I need to quickly run test queries or validate scripts against production data in a dev workspace. It is instant and free because it only copies metadata.
> 
> I choose a **Deep Clone** when copying production tables to a staging sandbox where developers will write updates, or for data archiving. A Deep Clone is an independent table copy, making it safe from source deletions."

### Q4: What is Change Data Feed (CDF) in Delta Lake and how do you use it?
> **Answer:** "CDF is a feature that captures row-level changes (inserts, updates, deletes) in a Delta table. Once enabled, Delta writes change log files. Downstream ETL jobs can consume only the row changes between specific table versions using Spark Structured Streaming. This is highly performant because it avoids scanning the entire table or executing heavy joins to find differences."
