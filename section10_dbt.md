# Section 10: dbt (Data Build Tool) Must-Knows

---

## 🏛️ Core Definitions (The "Define This" Round)

### dbt Model
* **What it is:** A single SQL SELECT query that defines a logical database table or view in a dbt project repository.
* **How it works:** dbt compiles the SELECT query, resolves macros like `source()` and `ref()`, wraps it inside appropriate DDL (e.g. `CREATE VIEW`), and executes it on the target warehouse.
* **Where it's used:** Used to build data transformation steps sequentially throughout the landing (Bronze), staging (Silver), and reporting (Gold) layers.
* **Say this in the interview:** *"A dbt model is a SQL SELECT query defined in a `.sql` file that represents a logical database view or table. dbt handles the compilation and deployment DDL dynamically on the warehouse, enabling modular transformation pipelines."*

### dbt View Materialization
* **What it is:** The default dbt model build configuration that creates a virtual view in the target database.
* **How it works:** Compiles the model query and runs `CREATE OR REPLACE VIEW table_name AS SELECT ...` on the database catalog.
* **Where it's used:** Best for lightweight tables with fast transformations or datasets that change frequently and must reflect real-time source states.
* **Say this in the interview:** *"The View materialization compiles a model into a virtual view in the target database. It requires no physical storage footprint but executes the query logic on every read request, making it ideal for low-volume or dynamic schemas."*

### dbt Table Materialization
* **What it is:** A model configuration that creates a physical table in the database catalog.
* **How it works:** Executes a full rebuild of the database object (`CREATE TABLE AS SELECT ...`) on every dbt run, writing the compiled query output directly to disk.
* **Where it's used:** Best for reporting/presentation tables (Gold layer) queried frequently by BI tools where fast query speeds are the priority.
* **Say this in the interview:** *"The Table materialization creates a physical database table on disk, recalculating and rewriting the entire dataset on every execution. This ensures optimal downstream query speeds at the expense of longer model write times."*

### dbt Incremental Materialization
* **What it is:** A model configuration that appends or merges only new and updated records since the previous pipeline run.
* **How it works:** Filters incoming source rows using the `is_incremental()` macro and applies a merge or insert-overwrite DDL transaction on the target table.
* **Where it's used:** High-volume transaction ledgers or clickstream event logs where rebuilding the entire dataset daily is too slow and costly.
* **Say this in the interview:** *"The Incremental materialization optimizes processing by transforming and loading only new or changed records since the last run. It leverages merge or partition overwrite strategies to reduce compute costs and runtimes on massive datasets."*

### dbt Ephemeral Materialization
* **What it is:** A model configuration that creates no physical database object in the catalog.
* **How it works:** Compiles the model SELECT query and interpolates it as a Common Table Expression (CTE) inside downstream referencing models.
* **Where it's used:** Best for intermediate data cleanups or narrow filter tables that are only referenced once or twice in the project lineage.
* **Say this in the interview:** *"The Ephemeral materialization compiles a model directly into referencing downstream SQL queries as a CTE, avoiding database catalog clutter and saving storage at the expense of repeating the query logic in compiled runs."*

### dbt Seed
* **What it is:** A static lookup table loaded directly from a version-controlled CSV file inside the dbt repository.
* **How it works:** Running `dbt seed` parses CSV files under `seeds/` and loads them as physical tables in the target database schema.
* **Where it's used:** Small lookup dictionaries like zip-to-state indices, country codes, or promotional discount mappings.
* **Say this in the interview:** *"A dbt Seed is a mechanism to version-control and load small CSV files from the repository directly into the database as physical tables. It is designed for static business dictionaries rather than dynamic raw data."*

### dbt Source
* **What it is:** A declaration of raw, external database tables loaded by data ingestion tools (like Fivetran).
* **How it works:** Declared in a `schema.yml` configuration to represent the raw landing zone of data before transformations.
* **Where it's used:** Declaring raw database schemas to establish data freshness thresholds and start the lineage graph.
* **Say this in the interview:** *"A dbt Source is a declaration of a raw, external table in the warehouse. It decouples schema names from transformation logic, provides data freshness tracking, and acts as the root of the project's lineage DAG."*

### dbt Ref
* **What it is:** A macro that references another model within the dbt project.
* **How it works:** Dynamically resolves schema dependencies, ensuring correct run order and pointing to environment-specific schemas.
* **Where it's used:** Replacing hardcoded database table names in the `FROM` clauses of models.
* **Say this in the interview:** *"The `ref()` macro establishes dependencies between models in dbt. It constructs the execution DAG automatically and translates target paths dynamically, allowing identical SQL to execute across development and production schemas."*

### dbt Test
* **What it is:** An assertion mechanism to validate data quality constraints inside database tables.
* **How it works:** Compiles assertion logic into SELECT queries that return invalid rows; any returned row triggers a test failure.
* **Where it's used:** Ensuring key uniqueness, preventing null inputs, or enforcing referential integrity in Silver and Gold layers.
* **Say this in the interview:** *"A dbt Test is a data quality check written as a SQL query. If the query returns any rows, the test fails, alerting developers to bad data before it propagates to downstream dashboards."*

### dbt Snapshot
* **What it is:** A mechanism to track historical records over time (SCD Type 2).
* **How it works:** Runs a comparison query on a source table and appends historical state records using valid-from and valid-to columns.
* **Where it's used:** Tracking billing addresses, subscription status shifts, or product price modifications.
* **Say this in the interview:** *"A dbt Snapshot implements Slowly Changing Dimensions (SCD Type 2) by tracking rows over time. It compares historical changes using update timestamps or check columns, archiving inactive records in a single target table."*

---

## 🔄 Intermediate Concepts

### Sources, Refs, Packages & Variables
* **`source()` Macro:** Decouples hardcoded database/schema names from model code. Declared in a `schema.yml` file.
   * *Syntax:* `FROM {{ source('stripe', 'payments') }}`
* **`ref()` Macro:** References another dbt model in the project, automatically resolving execution order to build the DAG.
   * *Syntax:* `FROM {{ ref('stg_customers') }}`
* **Variables & Environments:**
   * **`var()`:** Used to pass configuration values into models (e.g. limit run targets: `limit {{ var('limit_rows', 100) }}`).
   * **`env_var()`:** Used to pull system environment variables (e.g. database credentials: `{{ env_var('DB_PASSWORD') }}`).
* **dbt Packages (`packages.yml`):**
   * Allows importing external dbt libraries (like `dbt_utils` or `dbt_expectations`) to access pre-written macros (e.g., pivot tables, unique checks across multiple columns) similar to importing python libraries.

### Schema Names & Environments
* **Default schema behavior:** dbt automatically appends the custom schema name to the default target schema defined in `profiles.yml` (e.g., if target schema is `dev_srikarr` and custom schema is `sales`, dbt creates `dev_srikarr_sales`).
* **Custom schema macro:** To change this behavior (e.g. to create exactly `sales` in production), you must override the built-in macro `generate_schema_name`:
  ```sql
  -- macros/generate_schema_name.sql
  {% macro generate_schema_name(custom_schema_name, node) -%}
      {%- if target.name == 'prod' and custom_schema_name is not none -%}
          {{ custom_schema_name | trim }}
      {%- else -%}
          {{ target.schema }}_{{ custom_schema_name | trim }}
      {%- endif -%}
  {%- endmacro %}
  ```

---

## ⚡ Advanced Concepts

### Incremental Strategies & Custom Filters
When configuring an `incremental` model, you must choose an incremental strategy:
* **Merge (Default for Snowflake/BigQuery):** Performs an upsert operation using a configured `unique_key`.
* **Delete+Insert:** Deletes matching records from the target table first, then inserts the new records.
* **Insert Overwrite (BigQuery/Spark/Databricks):** Overwrites specific partitions (e.g., date partitions) matching the current incremental run. Extremely fast and cost-effective on partitioned tables.

```sql
-- Example of an Incremental Model in dbt using a Merge strategy
{{
  config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge',
    cluster_by=['status']
  )
}}

SELECT
    transaction_id,
    amount,
    status,
    updated_at
FROM {{ ref('stg_raw_transactions') }}

{% if is_incremental() %}
    -- Only retrieve records updated after the maximum date in the target table
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

### Zero-Downtime Blue-Green Deployments
In enterprise warehouses (like Snowflake), rebuilding tables directly in production can disrupt business users. 
* **The Pattern:** 
  1. Build all models in a temporary "shadow" schema/database (e.g. `prod_blue`).
  2. Run all data quality checks on the shadow schema.
  3. If all tests pass, swap the production database/schema with the shadow database/schema using native commands (e.g. `ALTER SCHEMA prod SWAP WITH prod_blue`).
  4. This provides sub-second database switches without downstream dashboard outages.

### Jinja & Custom Macros
Macros are reusable pieces of code written in Jinja (a templating language). They are the equivalent of custom functions in SQL.
* **Creating a Custom Macro:**
  ```sql
  -- macros/cents_to_dollars.sql
  {% macro cents_to_dollars(column_name, scale=2) -%}
      round(cast({{ column_name }} as numeric) / 100, {{ scale }})
  {%- endmacro %}
  ```
* **Using the Macro in a Model:**
  ```sql
  SELECT
      order_id,
      {{ cents_to_dollars('amount_cents') }} as amount_dollars
  FROM {{ ref('stg_orders') }}
  ```

### dbt Semantic Layer
* **What it is:** A centralized configuration layer where you define business metrics (e.g. Daily Active Users, Total Revenue) in YAML files on top of dbt models.
* **Benefit:** Ensures that different BI tools (like Tableau, PowerBI, and Hex) query metrics using identical SQL calculations, establishing a single source of truth for business metrics.

---

## 🏢 Practical dbt Interview Questions

### Q1: How do you override dbt's custom schema naming logic?
> **Answer:** "By default, dbt appends custom schemas to the default schema (e.g., `target_schema_custom_schema`). To override this, I create a macro named `generate_schema_name` inside our `macros/` folder. This macro checks the execution target; if the environment is `prod`, it returns the custom schema name directly without prefixes, while retaining the prefixed behavior in development to prevent developers from overwriting each other."

### Q2: What is the difference between `dbt run` and `dbt compile`?
> **Answer:** "`dbt compile` parses the Jinja and SQL templates, resolves dependencies, and compiles them into raw executable SQL queries in the `target/compiled/` directory. It does *not* execute anything against the database.
> 
> `dbt run` compiles the SQL files *and* executes them on the target database, physically creating or updating the tables and views in the warehouse."

### Q3: When should you use a dbt ephemeral model versus a standard view?
> **Answer:** "I choose **ephemeral** models for intermediate, helper models that are only used inside a single downstream model and do not need to be queried by analysts or BI tools. This prevents database catalog pollution.
> 
> I choose **views** if the intermediate model is queried by multiple downstream models or directly by analysts for debugging, as view queries are exposed in the database catalog."

### Q4: How does dbt handle incremental updates on partitioned tables?
> **Answer:** "By utilizing the `insert_overwrite` incremental strategy. In the model config, I define the partition key (e.g., `partition_by={'field': 'date_day', 'data_type': 'date'}`). When executing, dbt compiles a query that drops only the partitions matching the current incremental batch data and inserts the new rows, avoiding a full table scan or merge write lock."
