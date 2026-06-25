# Section 7: Behavioral Questions (The "Tell Me About" Round)

---

## 1. Tell me about a data pipeline you built end to end

* **What the Interviewer is Really Testing:**
  * Ownership, end-to-end design thinking, and understanding of the business problem. They want to make sure you didn't just write a single SQL query but understand the source systems, schemas, staging layers, orchestration, and metrics.
* **STAR Framework Answer Template:**
  * **Situation:** At my previous role/internship, our business operations team was using manual spreadsheet exports to track daily user retention rates, which delayed marketing decisions by 48 hours.
  * **Task:** My task was to design and build an automated daily pipeline to ingest user clickstream events and transactional order databases, consolidating them into an aggregated user retention table in our Delta Lakehouse.
  * **Action:** I set up an ingestion schema in PySpark to read raw JSON events from our S3 staging folder. I implemented a Medallion Architecture: writing raw files to Bronze (append-only), cleansing and deduplicating customer profiles in Silver using a `MERGE` statement, and calculating a 7-day rolling retention cohort table in Gold. I scheduled the entire workflow in Apache Airflow, setting up Slack alerts for any task failures.
  * **Result:** The pipeline finished successfully before 7:00 AM daily. It reduced reporting latency from 48 hours to near real-time, allowing the marketing team to optimize active acquisition campaigns based on accurate daily conversion rates.

---

## 2. Describe a time you found a data quality issue in production

* **What the Interviewer is Really Testing:**
  * Attention to detail, debugging ability, proactive attitude, and understanding of data quality principles.
* **STAR Framework Answer Template:**
  * **Situation:** A week after deploying our sales reporting dashboard, the finance team noticed a discrepancy: the dashboard reported 12% higher sales revenue than the billing ledger.
  * **Task:** I had to investigate the pipeline, locate the root cause of the data inflation, and fix it without disrupting the active dashboards.
  * **Action:** I traced the lineage back to the Silver transformation. I analyzed the Spark execution logs and wrote validation queries to check key fields. I found that a join between `fact_sales` and `dim_promotions` was producing a partial Cartesian product because some promotional coupon keys were duplicated in the source database.
  * **Result:** I fixed the issue by applying a deduplication step on the promotions table using `row_number()` before joining. I then implemented a check in our pipeline using a validation utility that aborted the loading stage if the join output row count exceeded the input row count. I resolved the revenue inflation, restoring the dashboard's accuracy.

---

## 3. How do you handle a situation where requirements change midway through building a pipeline?

* **What the Interviewer is Really Testing:**
  * Adaptability, communication skills, and how you design modular code to handle change.
* **STAR Framework Answer Template:**
  * **Situation:** Midway through building a monthly merchant billing pipeline, the product team decided to change the business rules, adding support for multi-currency transactions instead of processing Indian Rupees (INR) exclusively.
  * **Task:** I needed to pivot my pipeline design to support multi-currency calculations without pushing back our target deployment date.
  * **Action:** Because I had built my pipeline using modular PySpark functions instead of a single script, I was able to isolate the pricing calculations. I created a new configuration parameter to ingest daily exchange rates from a currency API, and added a currency conversion step to the transformation module, mapping transaction currencies to local equivalents.
  * **Result:** I communicated the revised testing plan to our QA analysts. Because the modular code was easy to adapt, I implemented and tested the change, delivering the pipeline on schedule and supporting the new product requirements.

---

## 4. Tell me about a time you optimized something — query, job, or process

* **What the Interviewer is Really Testing:**
  * System performance knowledge, optimization techniques, and understanding of costs (IO, network, compute).
* **STAR Framework Answer Template:**
  * **Situation:** A critical daily Spark job that joined user profiles with transactional records was taking over 2 hours to execute, which delayed downstream dashboards and increased cloud compute costs.
  * **Task:** I had to analyze the job's execution plan, identify the bottleneck, and optimize the execution time to under 30 minutes.
  * **Action:** I opened the Spark UI and checked the SQL execution DAG. I noticed a massive shuffle stage that was writing gigabytes of data to disk. The bottleneck was a Sort-Merge Join between a massive 300GB transaction table and a small 15MB store-mapping table. I modified the PySpark code to use a `broadcast()` join on the store table, which kept the lookup table in executor memory and bypassed the network shuffle entirely.
  * **Result:** The execution time fell from over 2 hours to 18 minutes. This change also allowed us to downgrade the Spark cluster size, reducing our monthly cloud spend on this job by 70%.

---

## 5. How do you decide which tool or approach to use when there are multiple options?

* **What the Interviewer is Really Testing:**
  * Pragmatism, cost-benefit analysis, and avoiding the "hype cycle." They want to see if you choose tools based on engineering trade-offs rather than just wanting to play with new technologies.
* **STAR Framework Answer Template:**
  * **Situation:** We needed to implement a data transformation layer for our new Snowflake Data Warehouse, and the team was deciding between writing custom PySpark jobs on Databricks or using dbt (data build tool) for SQL transformations.
  * **Task:** I was asked to evaluate both approaches and present a recommendation based on cost, maintainability, and team skills.
  * **Action:** I set up a prototype of both solutions. I analyzed the performance, pricing, and ease of deployment. I found that while Spark was highly flexible for complex transformations, our team consisted mostly of SQL-fluent analysts. Using dbt allowed us to run transformations directly inside Snowflake, utilizing Snowflake's optimized query planner and keeping our pipeline code in SQL.
  * **Result:** I recommended dbt for our project. This choice allowed our analysts to write and maintain their own data models, freeing up the data engineering team to focus on core ingestion infrastructure.

---

## 6. How do you stay updated with what's happening in the data engineering space?

* **What the Interviewer is Really Testing:**
  * Continuous learning habits, passion for the discipline, and keeping up with industry best practices.
* **STAR Framework Answer Template:**
  * **Situation:** The data engineering space evolves rapidly with new open table formats, orchestration tools, and serverless compute models.
  * **Task:** I needed to build a habit of structured learning to keep my technical skills aligned with modern industry standards.
  * **Action:** I subscribed to industry publications like the *Data Engineering Weekly* newsletter and read engineering blogs from tech companies like Uber, Airbnb, and Razorpay. I also joined local data engineering community channels and spent time building small hands-on side projects (like experimenting with Apache Iceberg locally) to understand the technical details.
  * **Result:** This continuous learning habit helped me suggest using Delta Lake's Z-Ordering in our team's pipelines, which improved query performance on our analytics tables and reduced operational overhead.

---

## 7. Describe a mistake you made in a pipeline and what you learned from it

* **What the Interviewer is Really Testing:**
  * Humility, accountability, and your capacity to learn and build safety mechanisms to prevent future failures.
* **STAR Framework Answer Template:**
  * **Situation:** During my first month as an engineer, I was tasked with updating an address field in a production dimension table.
  * **Task:** I had to deploy a script to update historical records with correct spelling adjustments.
  * **Action:** I ran an update script without validating the row count first. The script did not have a clear WHERE clause condition, which inadvertently updated the address field for all customers in the table to the same value, corrupting our historical records.
  * **Result:** I immediately notified my lead, and we restored the table using Delta Lake's Time Travel to roll back to the version before my write. I learned from this mistake and designed a safety policy: all database updates must be tested in a staging environment first, and any manual execution scripts must run through a code review process before being applied to production.

---

## 8. How do you work with data analysts or data scientists who consume your pipelines?

* **What the Interviewer is Really Testing:**
  * Collaboration skills, empathy, and your ability to treat data consumers as customers.
* **STAR Framework Answer Template:**
  * **Situation:** Our data science team was complaining that the columns in our Gold marketing tables changed frequently without notice, which caused their customer churn models to fail in production.
  * **Task:** I had to design a collaborative process to stabilize our schemas and support the data science workflow.
  * **Action:** I set up bi-weekly alignment meetings with the data scientists. I designed a formal **Data Contract** using a version-controlled YAML schema. We agreed that any schema changes must run through a pull-request review process, and I set up a staging view that mapped the physical tables to a stable schema.
  * **Result:** This collaborative approach stopped schema failures in production, rebuilt trust between our teams, and accelerated the deployment of new machine learning models from weeks to days.

---

## 9. Tell me about a time you had to manage expectations during a critical pipeline outage
* **What the Interviewer is Really Testing:**
  * Communication clarity, grace under pressure, prioritizing business impact, and stakeholder management.
* **STAR Framework Answer Template:**
  * **Situation:** At 6:30 AM on a Monday, a critical data pipeline that feeds our daily executive sales dashboard failed due to an API change in our third-party payment gateway, threatening to miss our 8:00 AM SLA.
  * **Task:** I needed to isolate and fix the pipeline failure while proactively keeping senior business leaders informed of the delay.
  * **Action:** I immediately posted a high-priority update in our Slack announcement channel, acknowledging the outage, outlining the business impact, and promising a progress update by 7:15 AM. I then opened the Spark UI, found the parsing error on the new API field format, and implemented a fallback default parsing block. I ran a manual staging verification, deployed the fix to production, and started the backfill. 
  * **Result:** I sent a second update at 7:15 AM as promised, letting stakeholders know the fix was live and backfilling. The pipeline successfully completed at 7:45 AM, meeting the 8:00 AM dashboard SLA. Proactive communication kept business leaders calm and avoided customer service complaints.

