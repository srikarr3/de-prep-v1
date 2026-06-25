# Section 4: SQL Must-Knows (The "Write a Query" Round)

---

## 1. Window Functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTILE

### Concept Explanation
Window functions perform calculations across a set of table rows that are related to the current row, without collapsing the rows into a single output row (unlike `GROUP BY`).
* `ROW_NUMBER()`: Assigns a unique sequential integer starting at 1.
* `RANK()`: Assigns a rank, skipping numbers if there are ties (e.g., 1, 2, 2, 4).
* `DENSE_RANK()`: Assigns a rank, without skipping numbers for ties (e.g., 1, 2, 2, 3).
* `LAG(col, n)`: Accesses data from `n` rows prior to the current row.
* `LEAD(col, n)`: Accesses data from `n` rows after the current row.
* `NTILE(n)`: Divides ordered rows into `n` roughly equal buckets.

### Interview Problem
* **Problem:** Given an `employees` table with columns `employee_id`, `department_id`, and `salary`, find the top 2 highest-paid employees in each department, and calculate the salary difference between the current employee and the next-highest-paid employee in that same department.

### Solution Query
```sql
WITH ranked_salaries AS (
    SELECT 
        employee_id,
        department_id,
        salary,
        DENSE_RANK() OVER (
            PARTITION BY department_id 
            ORDER BY salary DESC
        ) as rnk,
        LEAD(salary, 1) OVER (
            PARTITION BY department_id 
            ORDER BY salary DESC
        ) as next_lower_salary
    FROM employees
)
SELECT 
    employee_id,
    department_id,
    salary,
    rnk,
    (salary - COALESCE(next_lower_salary, salary)) as salary_gap
FROM ranked_salaries
WHERE rnk <= 2;
```

---

## 2. Running Totals and Moving Averages

### Concept Explanation
Calculating aggregates over a sliding window frame defined relative to the current row. We use the window frame clause:
* `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` (for cumulative sums/running totals).
* `ROWS BETWEEN N PRECEDING AND CURRENT ROW` (for moving averages over a fixed size frame).

### Interview Problem
* **Problem:** Given a `daily_sales` table with `sale_date` and `amount`, calculate the cumulative running total of sales for the month and a 7-day rolling moving average of sales.

### Solution Query
```sql
SELECT 
    sale_date,
    amount,
    -- Running total from start of data to current row
    SUM(amount) OVER (
        ORDER BY sale_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_total,
    -- Moving average of current day + 6 previous days
    AVG(amount) OVER (
        ORDER BY sale_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7_day
FROM daily_sales;
```

---

## 3. Finding Nth Highest Value (Multiple Approaches)

### Concept Explanation
Retrieving a record at a specific rank position. Interviewers look for:
* **Approach 1 (Window Functions):** Cleanest and handles duplicates correctly using `DENSE_RANK`.
* **Approach 2 (Subqueries/Offset):** Common in MySQL/PostgreSQL but fragile with duplicate values.
* **Approach 3 (Correlated Subquery):** Classical database theory approach.

### Interview Problem
* **Problem:** Find the 3rd highest unique salary from the `employees` table.

### Solution Query
```sql
-- Approach 1: DENSE_RANK (Best practice, handles ties)
WITH ranked_salaries AS (
    SELECT 
        salary,
        DENSE_RANK() OVER (ORDER BY salary DESC) as rnk
    FROM employees
)
SELECT DISTINCT salary 
FROM ranked_salaries 
WHERE rnk = 3;

-- Approach 2: LIMIT OFFSET (Simple, but requires DISTINCT to handle ties)
SELECT DISTINCT salary 
FROM employees 
ORDER BY salary DESC 
LIMIT 1 OFFSET 2;

-- Approach 3: Correlated Subquery (Generic ANSI SQL)
SELECT e1.salary
FROM employees e1
WHERE 2 = (
    SELECT COUNT(DISTINCT e2.salary)
    FROM employees e2
    WHERE e2.salary > e1.salary
);
```

---

## 4. Deduplication (Keep Latest Record per Key)

### Concept Explanation
A frequent requirement in Silver layer ETL pipelines where source tables send update events. We must keep only the latest state of each primary key based on an update timestamp.

### Interview Problem
* **Problem:** An `orders` table contains duplicate records for the same `order_id` due to status updates. Keep only the latest record for each `order_id` based on `updated_at`.

### Solution Query
```sql
WITH deduplicated_orders AS (
    SELECT 
        order_id,
        customer_id,
        status,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY updated_at DESC
        ) as row_num
    FROM orders
)
SELECT 
    order_id,
    customer_id,
    status,
    updated_at
FROM deduplicated_orders
WHERE row_num = 1;
```

---

## 5. CTEs vs Subqueries

### Concept Explanation
* **CTE (Common Table Expression):** A temporary named result set defined using the `WITH` clause. Highly readable, reusable within the query, and supports recursion.
* **Subquery:** A query nested inside another statement.
* **Performance Note:** In modern engines (Spark SQL, Snowflake, Postgres 12+), CTEs are not materialized blindly; they are optimized inline by the planner just like subqueries, meaning CTEs should always be preferred for readability.

### Interview Problem
* **Problem:** Identify customers who spent more than the average customer spend in the year 2026. Solve using a readable CTE structure.

### Solution Query
```sql
WITH customer_spend AS (
    -- Calculate total spend per customer
    SELECT 
        customer_id, 
        SUM(amount) as total_spend
    FROM orders
    WHERE YEAR(order_date) = 2026
    GROUP BY customer_id
),
average_spend AS (
    -- Calculate overall average customer spend
    SELECT AVG(total_spend) as avg_spend 
    FROM customer_spend
)
SELECT 
    c.customer_id,
    c.total_spend
FROM customer_spend c
CROSS JOIN average_spend a
WHERE c.total_spend > a.avg_spend;
```

---

## 6. All JOIN Types with When-to-Use

### Concept Explanation
* **INNER JOIN:** Returns records with matching values in both tables. Use for strict associations.
* **LEFT JOIN:** Returns all records from the left table and matching from the right. Use when preserving left-side entities is critical.
* **RIGHT JOIN:** Reverse of LEFT. Rarely used; rewrite as LEFT JOIN for consistency.
* **FULL OUTER JOIN:** Returns all records when there is a match in either table. Use for reconciling or merging datasets.
* **CROSS JOIN:** Returns the Cartesian product (all combinations). Use for generating matrix test cases or date hierarchies.
* **SELF JOIN:** Joins a table to itself. Use for hierarchical relationships (e.g., Employee-Manager).

### Interview Problem
* **Problem:** Given an `employees` table containing `employee_id`, `name`, and `manager_id` (referencing `employee_id`), write a query to display each employee's name along with their manager's name. If they have no manager, display "No Manager".

### Solution Query
```sql
SELECT 
    emp.name as employee_name,
    COALESCE(mgr.name, 'No Manager') as manager_name
FROM employees emp
LEFT JOIN employees mgr 
    ON emp.manager_id = mgr.employee_id;
```

---

## 7. GROUP BY vs PARTITION BY

### Concept Explanation
* `GROUP BY`: Collapses multiple rows into a single aggregated row. The output cardinality matches the number of unique grouping keys.
* `PARTITION BY`: Used inside window functions. It groups rows for calculation but preserves the original row-level granularity, meaning output cardinality is equal to input cardinality.

### Interview Problem
* **Problem:** Write two queries using `sales` (containing `department_id`, `salesperson_id`, and `revenue`):
  1. Show total revenue per department.
  2. Show each individual salesperson's revenue next to their department's total revenue.

### Solution Query
```sql
-- Query 1: GROUP BY (Collapses rows)
SELECT 
    department_id,
    SUM(revenue) as total_dept_revenue
FROM sales
GROUP BY department_id;

-- Query 2: PARTITION BY (Preserves rows)
SELECT 
    salesperson_id,
    department_id,
    revenue,
    SUM(revenue) OVER (PARTITION BY department_id) as total_dept_revenue
FROM sales;
```

---

## 8. MERGE / UPSERT Statement

### Concept Explanation
Conditionally updates target rows that match a key and inserts new rows that do not match. This is the core operation for syncing upstream transactions into target tables without duplicates.

### Interview Problem
* **Problem:** Write a SQL MERGE statement to upsert records from a staging table `staging_users` into the target production table `dim_users` using `user_id` as the key.

### Solution Query
```sql
MERGE INTO dim_users AS target
USING staging_users AS source
ON target.user_id = source.user_id
WHEN MATCHED AND target.email <> source.email THEN
    UPDATE SET 
        target.email = source.email,
        target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (user_id, name, email, created_at, updated_at)
    VALUES (source.user_id, source.name, source.email, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
```

---

## 9. Handling NULLs: COALESCE, NULLIF, IS NULL

### Concept Explanation
* `COALESCE(val1, val2, ...)`: Returns the first non-null value in the arguments list.
* `NULLIF(val1, val2)`: Returns NULL if `val1` equals `val2`; otherwise returns `val1`. Used to prevent division-by-zero errors.
* **NULLs in JOINs:** A join condition like `ON a.id = b.id` will skip rows where `id` is NULL because `NULL = NULL` evaluates to Unknown in SQL three-valued logic.

### Interview Problem
* **Problem:** Calculate the conversion rate of active campaigns: `conversions / clicks`. If clicks are zero or NULL, return 0. If data fields are missing, fallback to defaults.

### Solution Query
```sql
SELECT 
    campaign_id,
    -- Prevent division-by-zero using NULLIF and handle outer NULLs with COALESCE
    COALESCE(
        conversions / NULLIF(clicks, 0), 
        0
    ) as conversion_rate
FROM campaign_performance;
```

---

## 10. Query Optimization: Reading an EXPLAIN Plan

### Concept Explanation
* **Explain Plan:** An execution map generated by the database showing how it will retrieve data.
* **Key Operations to check:**
  * `Table Scan / Seq Scan`: Reading the entire physical table (bad for point lookups).
  * `Index Scan`: Using indexes to find specific keys (good).
  * `Hash Join / Merge Join`: Multi-pass matches for large tables.
* **SELECT * is bad** because it defeats projection pushdown, forces the engine to read unnecessary columns from disk, wastes network bandwidth, and prevents the use of covering indexes.

### Interview Problem
* **Problem:** Given an execution plan showing a Seq Scan on a large customer table during a join on `customer_id`, how do you optimize it? Explain the difference.

### Solution Query
```sql
-- Before: Inefficient join that forces a Sequential Table Scan
EXPLAIN SELECT * FROM orders o JOIN customers c ON o.customer_id = c.customer_id;

-- Optimization steps:
-- 1. Create an index on the join key
CREATE INDEX idx_customers_id ON customers(customer_id);

-- 2. Select only needed columns to allow index-only scans
EXPLAIN SELECT o.order_id, o.amount, c.customer_name 
FROM orders o 
JOIN customers c 
    ON o.customer_id = c.customer_id;
```

---

## 11. Pivot / Unpivot

### Concept Explanation
* **Pivot:** Rotates rows into columns (denormalization for presentation).
* **Unpivot:** Rotates columns into rows (normalization for aggregation pipelines).

### Interview Problem
* **Problem:** You have a `monthly_revenue` table: `year`, `month`, and `revenue`. Pivot this so that each month (Jan, Feb, Mar) becomes a column displaying its respective revenue for each year.

### Solution Query
```sql
-- Pivot using conditional aggregation (ANSI SQL compliant)
SELECT 
    year,
    SUM(CASE WHEN month = 'Jan' THEN revenue ELSE 0 END) as Jan_revenue,
    SUM(CASE WHEN month = 'Feb' THEN revenue ELSE 0 END) as Feb_revenue,
    SUM(CASE WHEN month = 'Mar' THEN revenue ELSE 0 END) as Mar_revenue
FROM monthly_revenue
GROUP BY year;

-- Alternative Pivot using native Snowflake / Spark SQL PIVOT function
SELECT * FROM (
    SELECT year, month, revenue FROM monthly_revenue
)
PIVOT (
    SUM(revenue)
    FOR month IN ('Jan' AS Jan_revenue, 'Feb' AS Feb_revenue, 'Mar' AS Mar_revenue)
);
```

---

## 12. Recursive CTEs

### Concept Explanation
A CTE that references itself, allowing you to traverse hierarchical structures or generate series loops. It runs iteratively: an anchor query runs first, then a recursive query runs and joins to the previous iteration's result until no new rows are returned.

### Interview Problem
* **Problem:** You have a database table of organizational hierarchies: `employee_id`, `name`, and `manager_id`. Write a query to display the reporting hierarchy for employee ID 45, showing their organizational level from the top down.

### Solution Query
```sql
WITH RECURSIVE org_hierarchy AS (
    -- Anchor member: Start with the top-level CEO (manager_id is NULL)
    SELECT 
        employee_id,
        name,
        manager_id,
        1 as org_level
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive member: Join the table back to the anchor results
    SELECT 
        e.employee_id,
        e.name,
        e.manager_id,
        h.org_level + 1 as org_level
    FROM employees e
    INNER JOIN org_hierarchy h 
        ON e.manager_id = h.employee_id
)
SELECT * 
FROM org_hierarchy
ORDER BY org_level;
```

---

## 13. Date/Time Operations

### Concept Explanation
Temporal manipulation is fundamental in data warehousing to construct partitions, calculate age/tenure metrics, and align timezones.
* `DATE_TRUNC('day/month/year', timestamp)`: Truncates timestamp to the beginning of the specified unit.
* `DATEDIFF(unit, start, end)`: Calculates the difference between two timestamps.

### Interview Problem
* **Problem:** Calculate the average time (in minutes) between a user placing an order (`order_placed_time`, stored in UTC) and the shipping confirmation (`shipped_time`, stored in Local Indian Standard Time GMT+5:30), aggregated by order month.

### Solution Query
```sql
SELECT 
    -- Truncate order date to the first of the month
    DATE_TRUNC('month', order_placed_time) as order_month,
    AVG(
        -- Convert shipper local time to UTC to make calculation accurate
        DATEDIFF(
            minute, 
            order_placed_time, 
            CONVERT_TIMEZONE('Asia/Kolkata', 'UTC', shipped_time)
        )
    ) as avg_minutes_to_ship
FROM orders
WHERE shipped_time IS NOT NULL
GROUP BY DATE_TRUNC('month', order_placed_time)
ORDER BY order_month;
```

---

## 14. UNION vs UNION ALL (Performance & Set Operations)

### Concept Explanation
* `UNION`: Combines the results of two SELECT queries, filtering out duplicate records. To locate duplicates, the query engine must perform a distinct sort-and-hash operation across all returned columns, which requires a heavy CPU sort and network shuffle in distributed databases.
* `UNION ALL`: Concatenates the results of two SELECT queries together, retaining all rows (including duplicates). It requires no distinct check, making it a high-performance map-only operation that bypasses shuffles.
* **Performance Rule:** Always prefer `UNION ALL` unless you explicitly require duplicate removal. If deduplication is needed, it is sometimes faster to use `UNION ALL` and then apply a custom `ROW_NUMBER()` filter if you need to control which record to keep based on timestamps.

### Interview Problem
* **Problem:** Write a query to combine user IDs from `website_signups` and `mobile_signups` for the day. Explain the query plan differences between using UNION vs UNION ALL.

### Solution Query
```sql
-- Query 1: UNION ALL (High performance, map-only concatenation)
SELECT user_id, signup_time, 'web' as platform 
FROM website_signups
UNION ALL
SELECT user_id, signup_time, 'mobile' as platform 
FROM mobile_signups;

-- Query 2: UNION (Deduplicated union - forces a network shuffle and sort)
SELECT user_id 
FROM website_signups
UNION
SELECT user_id 
FROM mobile_signups;
```

---

## 15. The "Gaps and Islands" Problem (Consecutive Sequences)

### Concept Explanation
The "Gaps and Islands" problem involves identifying continuous sequences (islands) of records and the gaps between them. In interviews, this is typically asked as: *"Find the consecutive days a user has logged in, or the periods of active customer subscription streaks."*
We solve this using window functions:
1. Generate row numbers sorted by date: `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date)`.
2. Subtract the row number (as an interval of days) from the `login_date` to compute a "grouping key" timestamp: `(login_date - ROW_NUMBER())`.
3. If dates are consecutive, both the date and the row number will increase by 1, meaning their difference remains constant. This constant date difference becomes the "Island ID" that we group by.

### Interview Problem
* **Problem:** Given a `user_logins` table with columns `user_id` and `login_date`, write a query to find the length (in days) and the start/end dates of each consecutive login streak for every user.

### Solution Query
```sql
WITH unique_logins AS (
    -- Deduplicate logins per user per day first
    SELECT DISTINCT 
        user_id, 
        login_date
    FROM user_logins
),
streak_groups AS (
    SELECT 
        user_id,
        login_date,
        -- Subtract row number days from login date to create a constant grouping key
        DATEADD(day, -ROW_NUMBER() OVER (
            PARTITION BY user_id 
            ORDER BY login_date
        ), login_date) as streak_group_key
    FROM unique_logins
)
SELECT 
    user_id,
    MIN(login_date) as streak_start,
    MAX(login_date) as streak_end,
    -- Count the number of days in the streak
    COUNT(*) as streak_length
FROM streak_groups
GROUP BY user_id, streak_group_key
HAVING COUNT(*) >= 2 -- Only return streaks of 2 or more consecutive days
ORDER BY user_id, streak_start;
```

---

## 16. Cumulative Sum with Reset Condition

### Concept Explanation
Standard cumulative sums (`SUM(col) OVER (ORDER BY date)`) grow indefinitely. A "Cumulative Sum with Reset" is a running total that resets back to zero whenever a specific event occurs (e.g. a balance goes negative, a status flag flips, or a session changes).
We solve this in SQL using a two-pass window function approach:
1. Create a reset boundary indicator (e.g. `CASE WHEN status = 'reset' THEN 1 ELSE 0 END`).
2. Run a running sum over this indicator to create a grouping ID (this groups rows into separate partition blocks, where each block starts after a reset event).
3. Run the final running sum, partitioning by both the user and this new grouping ID.

### Interview Problem
* **Problem:** Given a `bank_transactions` table with columns `account_id`, `transaction_date`, and `amount` (which can be positive or negative), calculate the running balance. However, the running balance must reset back to zero the day after the account balance dips below $0 (negative balance).

### Solution Query
```sql
WITH running_balances AS (
    -- Step 1: Calculate standard running balance
    SELECT 
        account_id,
        transaction_date,
        amount,
        SUM(amount) OVER (
            PARTITION BY account_id 
            ORDER BY transaction_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as standard_balance
    FROM bank_transactions
),
reset_triggers AS (
    -- Step 2: Mark dates where balance was negative, creating a boundary flag
    SELECT 
        account_id,
        transaction_date,
        amount,
        standard_balance,
        CASE WHEN LAG(standard_balance, 1) OVER (
            PARTITION BY account_id 
            ORDER BY transaction_date
        ) < 0 THEN 1 ELSE 0 END as is_reset_point
    FROM running_balances
),
reset_groups AS (
    -- Step 3: Run a cumulative sum over the boundary flag to create distinct partition blocks
    SELECT 
        account_id,
        transaction_date,
        amount,
        SUM(is_reset_point) OVER (
            PARTITION BY account_id 
            ORDER BY transaction_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as balance_group_id
    FROM reset_triggers
)
-- Step 4: Run a running sum within each partition block
SELECT 
    account_id,
    transaction_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY account_id, balance_group_id 
        ORDER BY transaction_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_balance_with_reset
FROM reset_groups
ORDER BY account_id, transaction_date;
```

