# Section 6: Python for Data Engineers (The "Show Me Your Code Quality" Round)

---

## 1. Generators vs Lists (Memory Efficiency)

* **Concept:** 
  * A **List** stores all its elements in memory at once.
  * A **Generator** yields items one-by-one lazily on-demand, using a constant memory footprint regardless of data size.
* **Minimal Code:**
```python
import sys
import requests

# Inefficient List Approach: Loads all paginated items into a list before returning
def fetch_all_orders_list(api_url):
    orders = []
    url = api_url
    while url:
        # Mock payload: {"results": [...], "next_url": "https://api.com/v1/orders?page=2"}
        response = requests.get(url).json()
        orders.extend(response.get("results", []))
        url = response.get("next_url")
    return orders  # Massive list loaded in RAM (can cause OOM on millions of records)

# Efficient Generator Approach: Yields records lazily record-by-record
def fetch_orders_generator(api_url):
    url = api_url
    while url:
        response = requests.get(url).json()
        for record in response.get("results", []):
            yield record  # Yields individual record, releasing memory of the page after consumption
        url = response.get("next_url")

# Usage in pipeline:
# for order in fetch_orders_generator("https://api.payments.com/v1/orders"):
#     process_order(order)
```
* **Why it Matters in Production Pipelines:**
  * Ingesting gigabytes of API payloads, database rows, or log lines using lists can exhaust memory and trigger Out Of Memory (OOM) crashes. Generators allow processing infinite streams or file payloads record-by-record using a constant, minimal memory footprint.

---

## 2. Decorators (Retry & Execution Logging Logic)

* **Concept:** A decorator is a design pattern that wraps a function to extend its behavior without modifying the function's code directly.
* **Minimal Code:**
```python
import time
import random
import logging
from functools import wraps
import requests

logging.basicConfig(level=logging.INFO)

# Retry decorator with exponential backoff and jitter for API requests
def retry_with_backoff(max_retries=4, base_delay=2, backoff_factor=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(1, max_retries + 1):
                try:
                    response = func(*args, **kwargs)
                    # Raise HTTPError for bad responses to trigger retry loops on failure
                    response.raise_for_status()
                    return response
                except (requests.exceptions.RequestException, Exception) as e:
                    status_code = getattr(getattr(e, 'response', None), 'status_code', None)
                    
                    if attempt == max_retries:
                        logging.error(f"Failed after {max_retries} attempts: {e}")
                        raise
                    
                    # Back off longer on 429 Rate Limit
                    current_delay = delay * 2.5 if status_code == 429 else delay
                    # Add random jitter to prevent synchronized retry storms
                    jittered_delay = current_delay + random.uniform(0.1, 1.0)
                    
                    logging.warning(
                        f"Attempt {attempt}/{max_retries} failed (Status: {status_code}). "
                        f"Retrying in {jittered_delay:.2f}s... Error: {e}"
                    )
                    time.sleep(jittered_delay)
                    delay *= backoff_factor
        return wrapper
    return decorator

# Apply decorator to fragile connection call
@retry_with_backoff(max_retries=3, base_delay=1)
def fetch_payments_api(endpoint):
    return requests.get(f"https://api.razorpay.com/v1/{endpoint}")
```
* **Why it Matters in Production Pipelines:**
  * Network requests to third-party APIs or cloud database connections fail frequently. Instead of copy-pasting `try/except` loops in every function, decorators centralize retry policies, exponential backoffs, and jitter calculations, keeping the core pipeline code dry.

---

## 3. Context Managers (Resource Management)

* **Concept:** Context managers automate the allocation and deallocation of system resources (e.g., closing database connections, S3 connections, or clean-up tasks) using the `with` statement, preventing resource leaks.
* **Minimal Code:**
```python
import sqlite3
import logging

class WAPTransaction:
    """Context Manager implementing Write-Audit-Publish (WAP) pattern in database writes."""
    def __init__(self, db_path, target_table, audit_fn):
        self.db_path = db_path
        self.target_table = target_table
        self.staging_table = f"staging_{target_table}"
        self.audit_fn = audit_fn
        self.conn = None

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        
        # WRITE Stage: Create staging table duplicate
        cursor.execute(f"DROP TABLE IF EXISTS {self.staging_table}")
        cursor.execute(f"CREATE TABLE {self.staging_table} AS SELECT * FROM {self.target_table} WHERE 1=0")
        self.conn.commit()
        return self.conn, self.staging_table

    def __exit__(self, exc_type, exc_val, exc_tb):
        cursor = self.conn.cursor()
        try:
            if exc_type is not None:
                # If an error happened in the block, discard staging writes and rollback
                logging.error(f"Execution block failed. Rolling back: {exc_val}")
                self.conn.rollback()
                return False # Re-raise original error
            
            # AUDIT Stage: Execute data quality check
            if not self.audit_fn(self.conn, self.staging_table):
                raise ValueError("Data quality constraints violated on staging table!")
                
            # PUBLISH Stage: Load conformed rows into the target table
            cursor.execute(f"INSERT INTO {self.target_table} SELECT * FROM {self.staging_table}")
            self.conn.commit()
            logging.info("WAP Pipeline succeeded. Staged data published.")
        except Exception as e:
            logging.critical(f"Audit/Publish transaction aborted: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {self.staging_table}")
            self.conn.close()

# Validation callback function
def check_for_null_keys(conn, table):
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE transaction_id IS NULL")
    return cursor.fetchone()[0] == 0

# Usage
# with WAPTransaction("app.db", "fact_orders", check_for_null_keys) as (conn, staging_table):
#     cursor = conn.cursor()
#     cursor.execute(f"INSERT INTO {staging_table} VALUES (101, 'SUCCESS')")
```
* **Why it Matters in Production Pipelines:**
  * If a pipeline crashes midway through a write without closing connections, it locks database tables and exhausts pool connections. Incorporating WAP checks inside custom context managers ensures that any failure automatically cleans up temporary files/tables and rolls back partial writes, preventing corrupt data from ever landing in production tables.

---

## 4. Error Handling Patterns & Custom Exceptions

* **Concept:** Structuring exception handling to log errors, clean up state, and raise domain-specific custom exceptions that alerting tools can parse easily.
* **Minimal Code:**
```python
class PipelineExecutionError(Exception):
    """Custom exception raised when a critical data pipeline step fails."""
    pass

def process_data(file_path):
    try:
        # Simulate processing logic
        with open(file_path, "r") as f:
            data = f.read()
            if not data:
                raise ValueError("Source file is empty.")
    except FileNotFoundError as e:
        # Log specifically, run cleanup, then re-raise custom error
        print(f"CRITICAL: Input file missing: {e}")
        raise PipelineExecutionError("Data ingestion failed due to missing source.") from e
    except Exception as e:
        # Fallback catcher
        print(f"Unexpected error: {e}")
        raise PipelineExecutionError("General pipeline failure.") from e
    finally:
        # Code block that always executes (e.g., resource cleanup)
        print("Cleaning up temporary processing files...")
```
* **Why it Matters in Production Pipelines:**
  * Standard stack traces can be difficult to parse in log aggregators. Custom exceptions let orchestration engines (like Airflow) classify failures quickly, isolate data quality issues from infrastructure failures, and ensure cleanup blocks (like deleting temporary folders) run every time.

---

## 5. Working with Datetime and Timezones

* **Concept:** Standardizing dates to UTC during ingestion and parsing timezones explicitly to prevent offset errors.
* **Minimal Code:**
```python
from datetime import datetime
import pytz

# Define UTC timezone and local Indian timezone
utc_tz = pytz.utc
ist_tz = pytz.timezone("Asia/Kolkata")

# Get current time in UTC (Standard practice for DB writes)
utc_now = datetime.now(utc_tz)
print(f"UTC Now: {utc_now.isoformat()}")

# Convert UTC time to Local Indian Standard Time (IST) for reporting
ist_now = utc_now.astimezone(ist_tz)
print(f"IST Now: {ist_now.isoformat()}")

# Parse timezone-naive date string to explicit UTC
naive_str = "2026-06-25 12:00:00"
localized_dt = utc_tz.localize(datetime.strptime(naive_str, "%Y-%m-%d %H:%M:%S"))
```
* **Why it Matters in Production Pipelines:**
  * Naive datetime calculations assume the server's local system clock. When pipelines migrate from local development machines to servers running in different cloud zones (like UTC-configured VMs), naive date offsets will corrupt time-series partitions.

---

## 6. Reading and Writing Parquet with PyArrow

* **Concept:** Working with Parquet files in pure Python environments without using a heavy Spark cluster, using PyArrow for high-performance memory layout structures.
* **Minimal Code:**
```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Create simple DataFrame
df = pd.DataFrame({
    'user_id': [101, 102, 103],
    'revenue': [450.5, 300.0, 1200.75]
})

# Convert Pandas DataFrame to PyArrow Table
arrow_table = pa.Table.from_pandas(df)

# Write PyArrow Table to a Parquet file on disk using Snappy compression
pq.write_table(arrow_table, "users.parquet", compression="snappy")

# Read Parquet file back into PyArrow Table, projecting only specific columns
subset_table = pq.read_table("users.parquet", columns=["user_id"])
pandas_df = subset_table.to_pandas()
```
* **Why it Matters in Production Pipelines:**
  * For lightweight tasks (like processing configuration registries or small dimension updates in AWS Lambda functions), spinning up a Spark cluster is slow and expensive. PyArrow allows fast, columnar reads and writes within simple, low-cost serverless environments.

---

## 7. Modular Pipeline Functions

* **Concept:** Structuring pipeline scripts into testable, reusable functions that accept input arguments rather than running linear, monolithic code blocks.
* **Minimal Code:**
```python
def extract_source(api_url: str) -> dict:
    """Extracts raw API payload."""
    # Network request simulation
    return {"status": "success", "data": [{"id": 1, "value": 100}]}

def transform_data(raw_data: dict) -> list:
    """Validates and cleanses raw payloads."""
    records = raw_data.get("data", [])
    for record in records:
        record["value"] = float(record["value"]) * 1.18  # Add tax
    return records

def load_destination(cleaned_data: list, target_path: str) -> None:
    """Writes conformed records to data lake destination."""
    # Write logic simulation
    print(f"Successfully loaded {len(cleaned_data)} records to {target_path}")

def run_pipeline(api_url: str, target_path: str):
    """Main controller execution loop."""
    raw = extract_source(api_url)
    cleaned = transform_data(raw)
    load_destination(cleaned, target_path)
```
* **Why it Matters in Production Pipelines:**
  * Monolithic pipeline scripts are difficult to unit-test. Structuring pipelines as decoupled extract, transform, and load functions allows developers to mock inputs, run unit tests locally, and re-use transformation utilities across multiple DAGs.

---

## 8. Pipeline Configuration Management with Pydantic

* **Concept:** Decoupling execution parameters, database connections, and path directories from code using Pydantic settings management. This validates pipeline variables (like batch sizes, port ranges, paths, and date patterns) at startup, preventing mid-run failures.
* **Minimal Code:**
```python
import os
from typing import Literal
from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings

# Setup temporary environment variables
os.environ["ENV"] = "production"
os.environ["DB_PASSWORD"] = "secure-pass-123"
os.environ["SLACK_WEBHOOK"] = "https://hooks.slack.com/services/T00/B00/X00"
os.environ["BATCH_SIZE"] = "5000"

class PipelineConfig(BaseSettings):
    env: Literal["development", "staging", "production"] = Field(..., alias="ENV")
    db_host: str = Field("localhost", alias="DB_HOST")
    db_port: int = Field(5432, alias="DB_PORT")
    db_password: str = Field(..., alias="DB_PASSWORD")
    
    batch_size: int = Field(1000, alias="BATCH_SIZE")
    s3_landing_bucket: str = Field("s3://dev-landing-bucket/", alias="S3_LANDING_BUCKET")
    slack_webhook: HttpUrl = Field(..., alias="SLACK_WEBHOOK")

    # Pydantic loads configuration automatically from environment variables
    class Config:
        populate_by_name = True

# Instantiate config
try:
    config = PipelineConfig()
    print(f"Pipeline Config Loaded: env={config.env}, port={config.db_port}, batch_size={config.batch_size}")
except Exception as e:
    print(f"Configuration Loading Failed: {e}")
```
* **Why it Matters in Production Pipelines:**
  * Hardcoding settings or reading environment variables using raw `os.getenv` doesn't enforce types or check for missing credentials. A Pydantic settings schema acts as a data contract for your infrastructure, failing fast during pipeline initialization before connecting to databases or starting heavy clusters.

---

## 9. Logging Best Practices

* **Concept:** Utilizing structured logging outputs with timestamps, severity levels, and execution tracking rather than using simple `print` statements.
* **Minimal Code:**
```python
import logging

def setup_logger(pipeline_name):
    # Configure structured logging format
    logger = logging.getLogger(pipeline_name)
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Stream logs to stdout/console (critical for cloud log forwarders)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger

# Initialize logger
logger = setup_logger("transactions_pipeline")

def run_job():
    logger.info("Initializing Daily Transactions Pipeline job...")
    try:
        # Simulate loading file
        logger.debug("Loading source files from ADLS...")
        raise FileNotFoundError("Missing schema validation mapping.")
    except Exception as e:
        logger.error(f"Execution failed: {e}", exc_info=True)
```

---

## 10. OOP Principles in Data Engineering (Abstract Classes & Encapsulation)

* **Concept:** 
  * **Abstract Base Classes (ABCs):** Define a strict interface contract. By subclassing `ABC` and using `@abstractmethod`, you force all custom pipeline components to implement specific stages, ensuring standardization across a data platform team.
  * **Encapsulation:** Restricts direct access to class attributes (using single or double leading underscores), protecting configuration credentials or database handles from accidental modification by downstream scripts.
* **Minimal Code:**
```python
from abc import ABC, abstractmethod

class BaseDataConnector(ABC):
    """Abstract class establishing connection requirements for any source DB."""
    
    def __init__(self, connection_string: str):
        self._connection_string = connection_string  # Encapsulated (protected)
        
    @abstractmethod
    def connect(self):
        """Establishes active connection. Must be implemented by concrete subclass."""
        pass
        
    @abstractmethod
    def fetch_data(self, query: str):
        """Fetches payload. Must be implemented by concrete subclass."""
        pass

class SnowflakeConnector(BaseDataConnector):
    """Concrete implementation for Snowflake."""
    
    def connect(self):
        print(f"Connecting to Snowflake using string: {self._connection_string}")
        return "SnowflakeSession"
        
    def fetch_data(self, query: str):
        print(f"Executing analytical query: {query}")
        return [{"id": 1, "status": "active"}]
```
* **Why it Matters in Production Pipelines:**
  * When a data platform scales to dozens of sources (e.g., Salesforce, MySQL, Stripe), coding connectors as free-form scripts creates custom logic spaghetti. Enforcing standard class interfaces ensures that the orchestration engine can run all extractors using identical commands.

---

## 11. Classmethods and Staticmethods (Factory Patterns for Connectors)

* **Concept:**
  * `@classmethod`: Binds the method to the class rather than the instance. It receives `cls` as the first argument, allowing it to build and return instances of the class (the Factory Pattern).
  * `@staticmethod`: Binds to the class but receives no implicit arguments. It acts as a standard utility function nested within the namespace of the class.
* **Minimal Code:**
```python
class DatabaseConfig:
    def __init__(self, host: str, port: int, username: str):
        self.host = host
        self.port = port
        self.username = username
        
    @classmethod
    def from_environment(cls):
        """Factory method: Instantiates config from environment variables."""
        import os
        return cls(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            username=os.getenv("DB_USER", "postgres")
        )
        
    @staticmethod
    def validate_connection_string(conn_str: str) -> bool:
        """Utility method: Validates protocol prefixes without referencing state."""
        return conn_str.startswith("postgresql://") or conn_str.startswith("snowflake://")
```
* **Why it Matters in Production Pipelines:**
  * Airflow executors and container tasks initialize connections dynamically depending on target execution environments (Dev/Prod). Factory classmethods encapsulate configuration logic inside the object itself, rather than cluttering entrypoint scripts.

---

## 12. Dunder Methods in Pipeline Infrastructure (e.g., __call__, __repr__)

* **Concept:** Dunder (double-underscore) or magic methods customize standard Python behaviors. 
  * `__call__`: Makes a class instance callable like a standard function.
  * `__repr__`: Returns a detailed string representation of the object, invaluable for detailed logging and debugging.
* **Minimal Code:**
```python
class DataQualityCheck:
    def __init__(self, rule_name: str, threshold: float):
        self.rule_name = rule_name
        self.threshold = threshold
        
    def __repr__(self) -> str:
        """Clean string representation for monitoring dashboards."""
        return f"DataQualityCheck(rule='{self.rule_name}', threshold={self.threshold})"
        
    def __call__(self, record: dict) -> bool:
        """Makes instances of this class callable, enabling simple filter mappings."""
        value = record.get("score", 0.0)
        return value >= self.threshold

# Usage
is_premium_user = DataQualityCheck(rule_name="Loyalty threshold check", threshold=80.0)

# Logger prints __repr__ string
print(f"Registering check: {is_premium_user}") 

# Object is called directly like a function
user_payload = {"user_id": 105, "score": 92.5}
is_valid = is_premium_user(user_payload)
```
* **Why it Matters in Production Pipelines:**
  * Using `__call__` allows data quality rules, validation functions, and custom operators to be passed directly to Python map operations, Spark worker threads, or list comprehensions, aligning structural classes with functional processing styles.

---

## 13. Unit Testing and Mocking (pytest & unittest.mock)

* **Concept:** Unit testing in data pipelines verifies transformation logic in isolation. Because pipelines interact with databases and cloud stores (e.g. S3), we use `unittest.mock` to intercept network connections and return fake data, allowing tests to run locally and quickly in CI/CD without hitting APIs.
* **Minimal Code:**
```python
# etl_logic.py
import requests

def transform_user_data(raw_data: dict) -> dict:
    """Pure transformation logic - easily testable."""
    if "email" not in raw_data:
        raise ValueError("Missing email field")
    return {
        "user_id": raw_data["id"],
        "email": raw_data["email"].strip().lower(),
        "is_active": raw_data.get("status") == "active"
    }

def fetch_and_process(api_url: str) -> dict:
    """Fragile method that makes external API calls."""
    response = requests.get(api_url)
    response.raise_for_status()
    return transform_user_data(response.json())


# test_etl.py
import pytest
from unittest.mock import patch, MagicMock
from etl_logic import transform_user_data, fetch_and_process

# 1. Testing a pure function (No mocking needed)
def test_transform_user_data_success():
    input_data = {"id": 101, "email": " Test@CRED.club ", "status": "active"}
    result = transform_user_data(input_data)
    assert result == {"user_id": 101, "email": "test@cred.club", "is_active": True}

def test_transform_user_data_missing_email():
    with pytest.raises(ValueError, match="Missing email field"):
        transform_user_data({"id": 101, "status": "active"})

# 2. Testing with Mocking (Simulating API Response)
@patch("etl_logic.requests.get")
def test_fetch_and_process_success(mock_get):
    # Setup mock return value
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": 202, "email": "user@cred.club", "status": "active"}
    mock_get.return_value = mock_response
    
    # Run function (will hit mock instead of actual internet URL)
    result = fetch_and_process("https://api.mockurl.com/v1/user")
    
    # Assertions
    assert result["user_id"] == 202
    assert result["is_active"] is True
    mock_get.assert_called_once_with("https://api.mockurl.com/v1/user")
```
* **Why it Matters in Production Pipelines:**
  * Running tests that hit physical production databases or cloud resources in CI/CD is slow, expensive, and can cause flaky test failures due to network hiccups. Mocking guarantees deterministic test runs.

---

## 14. Multithreading vs Multiprocessing in Data Engineering

* **Concept:**
  * **Multithreading (`ThreadPoolExecutor`):** Ideal for **I/O-bound** tasks (e.g. fetching data from 50 HTTP APIs or downloading 100 S3 files). Threads share memory and wait on I/O, bypassing the Global Interpreter Lock (GIL) during blocking network calls.
  * **Multiprocessing (`ProcessPoolExecutor`):** Ideal for **CPU-bound** tasks (e.g. parsing 50 heavy local XML files, computing hashes, or running local ML logic). It spins up separate Python interpreters with their own memory space to utilize multi-core CPUs.
* **Minimal Code:**
```python
import time
import requests
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# I/O Bound Task (Fetching APIs)
API_URLS = [f"https://httpbin.org/delay/1" for _ in range(5)]

def fetch_url(url):
    response = requests.get(url)
    return response.status_code

# CPU Bound Task (Heavy computation)
def compute_factorial(n):
    result = 1
    for i in range(1, n + 1):
        result *= i
    return len(str(result))

if __name__ == "__main__":
    # 1. Multithreading for I/O Bound Tasks
    start_io = time.time()
    with ThreadPoolExecutor(max_workers=5) as executor:
        results_io = list(executor.map(fetch_url, API_URLS))
    print(f"Parallel I/O Completed in: {time.time() - start_io:.2f} seconds")  # ~1.2s instead of 5s
    
    # 2. Multiprocessing for CPU Bound Tasks
    numbers = [50000, 60000, 70000, 80000]
    start_cpu = time.time()
    with ProcessPoolExecutor(max_workers=4) as executor:
        results_cpu = list(executor.map(compute_factorial, numbers))
    print(f"Parallel CPU Completed in: {time.time() - start_cpu:.2f} seconds")
```
* **Why it Matters in Production Pipelines:**
  * Understanding this boundary prevents developers from using multithreading for data transformations (which locks the interpreter and runs sequentially due to the GIL) or using multiprocessing for API endpoints (which incurs high overhead to spin up processes).

---

## 15. Data Validation with Pydantic

* **Concept:** Data validation and parsing using Python type annotations. Pydantic enforces type hints at runtime, validates semi-structured payloads (like JSON API results), and raises clean, catchable validation errors when data constraints are violated.
* **Minimal Code:**
```python
from typing import Optional
from pydantic import BaseModel, EmailStr, Field, field_validator

# Define target model schema with constraints
class UserModel(BaseModel):
    user_id: int = Field(..., gt=0)  # Must be greater than 0
    email: EmailStr                  # Enforces valid email syntax
    age: Optional[int] = Field(None, ge=18, le=120)  # Age must be between 18 and 120
    status: str = "active"           # Default value
    
    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str) -> str:
        allowed = {"active", "inactive", "suspended"}
        if value.lower() not in allowed:
            raise ValueError(f"Status must be one of {allowed}")
        return value.lower()

# 1. Successful Validation & Ingestion
valid_json = {"user_id": 101, "email": "test@cred.club", "age": 28}
user = UserModel(**valid_json)
print(f"Validated Model: {user.user_id} - {user.email}")

# 2. Failed Validation
invalid_json = {"user_id": -5, "email": "not-an-email", "status": "pending"}
try:
    UserModel(**invalid_json)
except Exception as e:
    print(f"Validation failed with errors:\n{e}")
```
* **Why it Matters in Production Pipelines:**
  * Raw JSON inputs from APIs frequently change without notice. Checking fields using manual `if/else` checks is complex and prone to bugs. Pydantic schemas declare data contracts directly in code, throwing structured errors when invalid payloads arrive.

