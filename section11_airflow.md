# Section 11: Apache Airflow Must-Knows

---

## 🏛️ Core Definitions (The "Define This" Round)

### Airflow DAG (Directed Acyclic Graph)
* **What it is:** A collection of tasks organized with explicit dependencies and relations, containing no execution loops.
* **How it works:** Programmed in Python; the Airflow Scheduler parses the script to construct task execution dependency paths and metadata.
* **Where it's used:** Defining scheduling patterns, execution start dates, and task ordering for end-to-end data workflows.
* **Say this in the interview:** *"An Airflow DAG is a Directed Acyclic Graph written in Python that defines the execution schedule, task dependencies, and structural relationships of a pipeline workflow."*

### Airflow Task
* **What it is:** The actual operational unit of execution within an Airflow DAG.
* **How it works:** Instantiated when an Operator or Sensor is declared. Tasks exist in states (e.g. queued, running, success, failed) tracked in the database.
* **Where it's used:** Running single data processing operations (e.g., executing a SQL query, triggering a Spark script, checking a file).
* **Say this in the interview:** *"An Airflow Task is the fundamental unit of execution in a DAG, representing an instance of an Operator, Sensor, or decorated function running on a worker node."*

### Airflow Operator
* **What it is:** A pre-defined template class that defines the execution logic for a specific task.
* **How it works:** Handles communication, parameters mapping, and execution steps on the worker node. Examples include `PythonOperator` and `BashOperator`.
* **Where it's used:** Abstracting connections and logic to execute external processes (like running queries, file copying, or hitting endpoints).
* **Say this in the interview:** *"An Operator is a template class in Airflow that encapsulates execution logic, abstracting integrations with external systems like databases, S3, or compute clusters."*

### Airflow Sensor
* **What it is:** A specialized type of Operator designed to wait for a specific external condition to be met before proceeding.
* **How it works:** Periodically polls (pokes) an external resource (like a path in S3 or a database table row) until it returns `True` or times out.
* **Where it's used:** Orchestrating dependency checkpoints, pausing execution until source data files are loaded or upstream runs are completed.
* **Say this in the interview:** *"A Sensor is a specialized Operator that pauses task execution, polling an external resource at set intervals until a specific condition or event is detected."*

### Airflow Scheduler
* **What it is:** The background service daemon responsible for triggering scheduled DAG runs and queuing tasks.
* **How it works:** Periodically parses DAG files, updates task states in the metadata database, and pushes runnable tasks to execution executors.
* **Where it's used:** Coordinating the timing and concurrency of all pipelines in the Airflow environment.
* **Say this in the interview:** *"The Airflow Scheduler is a background daemon that monitors task states in the database, resolves dependencies, and queues runnable tasks for execution."*

### Airflow Hook
* **What it is:** A low-level interface that manages connections and communications with external database systems.
* **How it works:** Wraps authentication parameters, handles session setups, and provides Python APIs to execute commands.
* **Where it's used:** Powering Operators (e.g. `MySqlOperator` uses `MySqlHook` underneath to connect and run queries).
* **Say this in the interview:** *"An Airflow Hook is a low-level interface that manages connections, authentication credentials, and API protocols to communicate with external databases and cloud platforms."*

### XCom (Cross-Communication)
* **What it is:** A database-backed mechanism enabling tasks within a DAG run to share small metadata values.
* **How it works:** Serializes data (under 48KB) and writes it to the metadata database during execution, allowing downstream tasks to fetch it.
* **Where it's used:** Passing execution parameters, dynamically generated file paths, or row count counts between tasks.
* **Say this in the interview:** *"XCom is Airflow's mechanism for task metadata sharing. It serializes and stores small execution variables or paths in the metadata database for consumption by downstream tasks."*

### Airflow Executor
* **What it is:** The component that dictates how and where tasks are executed in the Airflow cluster.
* **How it works:** Receives queued tasks from the scheduler and assigns them to worker processes (Local, Celery, or Kubernetes).
* **Where it's used:** Scaling task processing capabilities in enterprise clusters (e.g. Celery for VMs, Kubernetes for container auto-scaling).
* **Say this in the interview:** *"An Airflow Executor determines how tasks are run. Local Executor runs tasks on a single node; Celery Executor distributes tasks to persistent workers; Kubernetes Executor spins up isolated pods per task."*

### Logical Date (Execution Date)
* **What it is:** A constant time reference identifying the specific scheduling period partition for a DAG run.
* **How it works:** Resolved as the start of the schedule interval period. Represented as `{{ ds }}` in jinja templates.
* **Where it's used:** Parameterizing query dates to ensure pipelines are idempotent and support historical backfills.
* **Say this in the interview:** *"The Logical Date represents the start of the scheduling interval being processed by a DAG run. It ensures deterministic backfills and is accessed via the `{{ ds }}` macro."*

### Airflow Pool
* **What it is:** A concurrency limit boundary used to control resource consumption of tasks.
* **How it works:** Allocates a set number of slots to a pool. Tasks assigned to the pool consume slots and queue if all slots are occupied.
* **Where it's used:** Restricting simultaneous writes to transactional databases or limiting API requests to external rate-limited APIs.
* **Say this in the interview:** *"An Airflow Pool is a concurrency control mechanism used to limit the number of active tasks running against a specific resource, preventing target database overloads."*

### Trigger Rule
* **What it is:** An execution rule defining when a task should run based on parent task execution outcomes.
* **How it works:** Evaluates parent states (success, failure, skip) before marking task as ready. Default is `all_success`.
* **Where it's used:** Setting up error alerts (`one_failed`), cleanup operations (`all_done`), or conditional branching paths.
* **Say this in the interview:** *"Trigger Rules define when a task should execute based on the completion status of its parents, enabling complex branches and failure callbacks."*

### Dynamic Task Mapping
* **What it is:** A framework to spawn a variable number of task instances at runtime based on upstream results.
* **How it works:** Compiles list outputs dynamically, expanding execution units horizontally in parallel using `.expand()`.
* **Where it's used:** Processing a list of files dynamically discovered in an S3 directory, or transforming custom partition lists.
* **Say this in the interview:** *"Dynamic Task Mapping enables a DAG to dynamically spawn multiple tasks at runtime based on upstream list outputs, scaling execution units horizontally."*

### Deferrable Operator
* **What it is:** An asynchronous execution operator that releases worker resources during long external waits.
* **How it works:** Defers task execution, relinquishing worker threads and registering polling tasks to a lightweight async **Triggerer** daemon.
* **Where it's used:** Waiting for long-running Spark jobs, Snowflake queries, or external REST API callbacks.
* **Say this in the interview:** *"A Deferrable Operator executes tasks asynchronously, yielding its worker slot back to the cluster queue while a lightweight Triggerer service polls the external API for completion."*

---

## 🔄 Intermediate Concepts

### Classic Operators vs. TaskFlow API
Airflow 2.0 introduced the TaskFlow API, which simplifies writing Python DAGs using decorators.

| Dimension | Classic Operators (e.g., `PythonOperator`) | TaskFlow API (Decorators) |
| :--- | :--- | :--- |
| **Code Style** | Verbose; requires instantiating operator objects | Clean; uses `@dag` and `@task` decorators on Python functions |
| **Dependency Definition**| Explicit using bitwise operators (`t1 >> t2`) | Implicit based on function call inputs and outputs |
| **Data Sharing (XCom)** | Requires manual `xcom_push` and `xcom_pull` | Handles XCom sharing automatically behind the scenes |

```python
# TaskFlow API Example
from airflow.decorators import dag, task
from datetime import datetime

@dag(start_date=datetime(2026, 6, 25), schedule_interval="@daily", catchup=False)
def simple_taskflow_pipeline():
    
    @task
    def extract():
        return {"data": [1, 2, 3]}

    @task
    def transform(raw_payload: dict):
        processed = [x * 2 for x in raw_payload["data"]]
        return processed

    @task
    def load(transformed_data: list):
        print(f"Loaded: {transformed_data}")

    # Dependency is established implicitly via function call mapping
    raw = extract()
    cleaned = transform(raw)
    load(cleaned)

simple_taskflow_pipeline()
```

### Scheduling: The execution_date (Logical Date) Gotcha
One of the most common points of confusion in Airflow is how scheduling intervals relate to task execution times:
* **The Rule:** A DAG run is triggered *only at the end* of its scheduling interval, not at the start.
* **Example:** If a DAG has `start_date = datetime(2026, 6, 20)` and `schedule_interval = '@daily'`, the first run will execute at midnight on **June 21st**.
* **Why?** Airflow waits for the entire day (June 20th) to complete so that all transactional events for that period have finished before the pipeline processes that date's data.

### Task Groups vs. SubDAGs
* **SubDAGs (Deprecated):** Nested DAG loops inside a parent DAG. They are resource-heavy, run on a separate scheduling loop, and can cause scheduler deadlocks.
* **Task Groups (Best Practice):** A UI-only grouping mechanism introduced in Airflow 2.0. Tasks run on the same parent DAG scheduler loop, occupy no overhead, and collapse visually in the UI.

---

## ⚡ Advanced Concepts

### Custom Operators and Hooks
If the default operators do not support a source, you can build a custom operator by extending `BaseOperator`:
```python
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

class PostgresToS3Operator(BaseOperator):
    def __init__(self, sql, s3_bucket, **kwargs):
        super().__init__(**kwargs)
        self.sql = sql
        self.s3_bucket = s3_bucket

    def execute(self, context):
        # Hooks encapsulate communication logic with external databases
        db_hook = PostgresHook(postgres_conn_id='postgres_default')
        df = db_hook.get_pandas_df(self.sql)
        print(f"Writing {len(df)} rows to S3 bucket {self.s3_bucket}")
```

### Dynamic DAG Generation (DAG Factory)
Rather than writing separate Python scripts for similar pipelines, you can generate DAGs dynamically:
```python
# dynamic_dag_generator.py
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# Define configurations for multiple clients
pipelines = ["client_alpha", "client_beta", "client_gamma"]

def create_dag(dag_id, schedule):
    with DAG(dag_id=dag_id, start_date=datetime(2026,6,25), schedule_interval=schedule) as dag:
        EmptyOperator(task_id="start") >> EmptyOperator(task_id="end")
    return dag

# Instantiates 3 distinct DAGs dynamically in the scheduler
for client in pipelines:
    globals()[f"dag_{client}"] = create_dag(f"dag_sync_{client}", "@daily")
```

---

## 🏢 Practical Airflow Interview Questions

### Q1: Why is using `datetime.now()` in a DAG's `start_date` parameter bad practice?
> **Answer:** "If you set `start_date = datetime.now()`, the start date advances on every scheduling heartbeat tick. Because the Scheduler parses the DAG file dynamically, the current time will always be ahead of the last parsed start date, preventing the DAG from ever resolving its schedule interval and executing."

### Q2: How do you pass data or state between tasks in Airflow?
> **Answer:** "For small metadata and parameters (under 48KB), I use **XComs** (Cross-Communication), which serializes variables and stores them in Airflow's metadata database.
> 
> For large datasets (such as a 10GB pandas dataframe), using XCom is a database anti-pattern. Instead, I write the data to S3 or GCS as a staging Parquet file in task A, pass the S3 file path to task B via XCom, and have task B read the Parquet file from object storage."

### Q3: What is the difference between `mode='poke'` and `mode='reschedule'` in Airflow Sensors?
> **Answer:** "In `poke` mode (default), the sensor occupies a worker slot and runs a continuous loop waiting for the condition to be met. This blocks other tasks. In `reschedule` mode, the sensor checks the condition; if unmet, it sleeps and releases the worker node slot back to the queue, rescheduling a new task check instance later, which optimizes cluster resources."

### Q4: Explain how Deferrable Operators work.
> **Answer:** "Deferrable operators submit a task execution request to an external service and then enter a deferred state, relinquishing their execution worker slots back to the cluster pool. A lightweight, async background process called the **Triggerer** monitors the task's completion. Once the job is complete, the Triggerer pushes the task back to the scheduler queue to run the teardown or final validation steps. This enables scaling out task execution without worker thread exhaustion."
