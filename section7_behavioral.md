# Section 7: Behavioral Questions (The "Tell Me About" Round)

---

## 1. Tell me about a data pipeline you built end to end

* **What the Interviewer is Really Testing:**
  * Ownership, end-to-end design thinking, and understanding of the business problem. They want to make sure you didn't just write a single SQL query but understand the source systems, schemas, staging layers, orchestration, and metrics.
* **STAR Framework Answer Template (Tailored for Junior DE):**
  * **Situation:** During my data engineering internship/first role, our business team relied on manual daily CSV exports from Stripe to reconcile payment records, which delayed finance reports by 24 to 48 hours.
  * **Task:** As my onboarding project, my tech lead assigned me to design and build an automated ingestion pipeline to fetch daily payment events, clean them, and load them into a conformed target table in our Delta Lakehouse.
  * **Action:** Working under the guidance of my lead, I wrote a Python script utilizing generators to pull paginated payment records from Stripe's REST API. I implemented schema validation checks using Pydantic to filter out bad rows and write them to a quarantine directory. I loaded conformed rows to a Bronze table, and wrote a PySpark job to merge them into our Silver table dynamically. Finally, I scheduled the pipeline in Apache Airflow, setting up Slack alert callbacks for failures.
  * **Result:** The automated pipeline successfully went live, eliminating manual pulls and ensuring reports were updated by 8:00 AM daily. It reduced ingestion latency from 48 hours to daily SLA-compliant updates and taught me how to design for idempotency.

---

## 2. Describe a time you found a data quality issue in production

* **What the Interviewer is Really Testing:**
  * Attention to detail, debugging ability, proactive attitude, and understanding of data quality principles.
* **STAR Framework Answer Template (Tailored for Junior DE):**
  * **Situation:** A week after my team deployed a new user-profile reporting dashboard, the product analysts noticed a discrepancy: the dashboard reported 10% more active profiles than our transactional database count.
  * **Task:** My tech lead asked me to investigate the pipeline, locate the source of the duplicate records, and apply a fix.
  * **Action:** I opened the Spark UI and analyzed the query plan for the daily Spark aggregation job. I noticed a large network shuffle during a join. I wrote validation queries to compare the source tables and discovered that a join between `fact_orders` and `dim_users` was producing a partial Cartesian product because some user rows had duplicate records due to concurrent profile updates. I refactored the PySpark query to deduplicate the user profiles on their latest `updated_at` timestamp using window functions before running the join.
  * **Result:** This resolved the duplication and restored the dashboard's consistency. Under the supervision of my lead, I also added a quality assurance constraint check in our pipeline that flags an error and halts execution if the join result row count exceeds the source fact table count.

---

## 3. How do you handle a situation where requirements change midway through building a pipeline?

* **What the Interviewer is Really Testing:**
  * Adaptability, communication skills, and how you design modular code to handle change.
* **STAR Framework Answer Template (Tailored for Junior DE):**
  * **Situation:** During my second month, I was building an API ingestion script for user shipping profiles when the backend team changed the API payload, introducing a nested address JSON block instead of flat fields.
  * **Task:** I needed to adapt my extraction and validation script to support this new structure without pushing back our target deployment date.
  * **Action:** Because I had built my ingestion script using modular Python functions and defined the data contracts using Pydantic schemas, I did not have to rewrite the core flow. I updated the Pydantic configuration schema to include the nested `Address` model and modified our parsing helper function to flatten the nested object before saving. I ran unit tests using mock inputs and reviewed the changes with a senior team member before deploying.
  * **Result:** The modular code design allowed me to adapt the script and complete the changes in less than a day, allowing us to successfully integrate with the new backend API on schedule.

---

## 4. Tell me about a time you optimized something — query, job, or process

* **What the Interviewer is Really Testing:**
  * System performance knowledge, optimization techniques, and understanding of costs (IO, network, compute).
* **STAR Framework Answer Template (Tailored for Junior DE):**
  * **Situation:** A daily PySpark transformation job that joined order events with item description lookup tables was taking over 45 minutes to execute on our cluster, which was high given the data size.
  * **Task:** My mentor asked me to analyze the execution metrics and optimize the job runtime to improve resource efficiency.
  * **Action:** I checked the Spark UI and inspected the physical execution plan. I noticed a massive shuffle stage that wrote gigabytes of data to disk. The bottleneck was a default Sort-Merge Join between our large `orders` table (50GB) and a small `item_categories` mapping table (5MB). I refactored the PySpark code to explicitly use a `broadcast()` join on the mapping table, keeping it in executor memory and bypassing the expensive network shuffle.
  * **Result:** The job execution time fell from 45 minutes to under 8 minutes. This optimization reduced the compute core-hours needed for the run, lowering cloud costs and teaching me how to analyze Spark UI shuffles.

---

## 5. How do you decide which tool or approach to use when there are multiple options?

* **What the Interviewer is Really Testing:**
  * Pragmatism, cost-benefit analysis, and avoiding the "hype cycle." They want to see if you choose tools based on engineering trade-offs rather than just wanting to play with new technologies.
* **STAR Framework Answer Template (Tailored for Junior DE):**
  * **Situation:** Our team was setting up a new pipeline to build daily aggregated metrics inside our Snowflake data warehouse, and my lead asked me to prototype and compare two approaches: writing custom PySpark jobs in Databricks versus writing models using dbt (Data Build Tool) directly in Snowflake.
  * **Task:** I had to evaluate both options for a basic aggregation model and document the trade-offs regarding developer speed, testing, and team skills.
  * **Action:** I built a prototype for both. I documented that while Spark was highly flexible, our team consisted of SQL-fluent analysts who needed to modify these models frequently. Writing the models in dbt allowed transformations to run natively in Snowflake, utilized Snowflake's query planner, and allowed analysts to manage their own SQL models using built-in testing features.
  * **Result:** I presented my prototype findings to my tech lead. We chose dbt for this reporting project, which allowed me to help onboard our analytics team onto the new framework and freed up the core DE team from writing repetitive aggregation queries.

---

## 6. How do stay updated with what's happening in the data engineering space?

* **What the Interviewer is Really Testing:**
  * Continuous learning habits, passion for the discipline, and keeping up with industry best practices.
* **STAR Framework Answer Template (Tailored for Junior DE):**
  * **Situation:** As a junior data engineer, keeping up with the rapid pace of data tools (like Apache Iceberg, dbt, and modern orchestrators) felt overwhelming.
  * **Task:** I wanted to establish a structured habit to learn modern design patterns and keep my technical skills aligned with industry standards.
  * **Action:** I subscribed to the *Data Engineering Weekly* newsletter and began reading engineering blogs from companies like Swiggy, Uber, and Razorpay to see how they solve scaling challenges. To practice, I set up a local Postgres and Airflow instance on my personal machine, writing simple DAGs to pull public API data and load it into a local warehouse.
  * **Result:** This self-learning habit helped me understand the differences between table formats (Delta vs Iceberg), which allowed me to participate actively in our team's discussions about modernizing our storage formats.

---

## 7. Describe a mistake you made in a pipeline and what you learned from it

* **What the Interviewer is Really Testing:**
  * Humility, accountability, and your capacity to learn and build safety mechanisms to prevent future failures.
* **STAR Framework Answer Template (Tailored for Junior DE):**
  * **Situation:** In my second month as a junior engineer, I was asked to update a country code mapping field in a production dimension table.
  * **Task:** My task was to run an update script on historical customer records to correct a spelling error.
  * **Action:** I was eager to complete the ticket and ran the SQL update script directly on our production workspace. Unfortunately, my filter condition was slightly incorrect, which caused the country code field to be updated for a much larger partition of users than intended, corrupting historical profiles.
  * **Result:** I immediately owned up to my mistake and notified my team lead. We rolled back the table to its previous state in under 10 minutes using Delta Lake's Time Travel. I learned a critical lesson about production safety. Since then, I always test update scripts in a staging database first, verify row count changes, and seek a senior code review before running any manual production modifications.

---

## 8. How do you work with data analysts or data scientists who consume your pipelines?

* **What the Interviewer is Really Testing:**
  * Collaboration skills, empathy, and your ability to treat data consumers as customers.
* **STAR Framework Answer Template (Tailored for Junior DE):**
  * **Situation:** Our data analysts frequently complained that the daily conformed tables they used for BI dashboards would occasionally change schemas or miss columns without notice, breaking their reports.
  * **Task:** I was assigned to act as the primary contact to help stabilize the schema interfaces and support their analytical workflows.
  * **Action:** I sat down with the analysts to understand their queries. I helped them set up version-controlled SQL views on top of our Silver tables, which isolated their dashboards from minor upstream database changes. I also set up validation checks using Pydantic on our raw ingestion layer to catch upstream schema changes early, alerting the analysts in advance.
  * **Result:** This collaborative approach stopped dashboard outages, rebuilt trust between our teams, and taught me the importance of data contracts and treating data consumers as direct clients.

---

## 9. Tell me about a time you had to manage expectations during a critical pipeline outage

* **What the Interviewer is Really Testing:**
  * Communication clarity, grace under pressure, prioritizing business impact, and stakeholder management.
* **STAR Framework Answer Template (Tailored for Junior DE):**
  * **Situation:** During my morning monitoring rotation, a critical Airflow DAG that generated daily sales reporting failed at 6:30 AM due to an unexpected format change in our source payment gateway API, risking our 8:00 AM SLA.
  * **Task:** As the on-call engineer, I had to investigate the failure, implement a fix, and communicate the delay to stakeholders.
  * **Action:** I immediately posted a high-priority update in our Slack alerts channel to acknowledge the outage and let consumers know I was investigating. I opened the Spark UI, found the parsing error on the new API field format, and implemented a temporary try-except parser fallback in our ingestion script. I verified the fix locally using mock inputs, deployed it, and restarted the DAG.
  * **Result:** I posted status updates on our progress as promised. The pipeline successfully completed at 7:50 AM, meeting the 8:00 AM dashboard SLA. Transparent communication kept stakeholders calm and taught me how to resolve production outages systematically.
