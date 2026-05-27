# Challenge Package 4: Loan Application Pre-Decision and Credit Risk

## One-page challenge brief

### Scenario

A consumer bank wants a near-real-time pre-decision flow for incoming loan applications.

### Team goal

Build Flink SQL to enrich applications with customer financial context and route applications into approve, review, or reject bands.

### Business objective

Speed up pre-screening while improving transparency.

### Inputs

- Kafka stream: `loan_application`
- Kafka stream: `document_event`
- Postgres CDC tables: `customer_credit_profile`, `existing_obligation`
- Minio reference: `product_policy`

### Mandatory tasks

1. Join loan applications with customer credit data.
2. Check outstanding obligations and compute basic affordability indicators.
3. Detect missing document or policy exceptions.
4. Produce `loan_predecision` with band, reason, and reviewer queue flag.

### Stretch goals

- Add applicant segmentation by product type.
- Prioritize high-value low-risk applications.
- Store latest application state.

### Deliverables

- Running SQL pipeline
- Output decision feed
- Demo with good and bad applications

### Difficulty

Medium

### Judge focus

Policy logic, explainability, and correctness.

## Sample schemas

### Kafka source: `loan_application`

```sql
CREATE TABLE loan_application (
  application_id STRING,
  customer_id STRING,
  product_code STRING,
  requested_amount DECIMAL(18,2),
  declared_income DECIMAL(18,2),
  tenor_months INT,
  channel STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'loan_application',
  'properties.bootstrap.servers' = '<broker>:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

### Kafka source: `document_event`

```sql
CREATE TABLE document_event (
  application_id STRING,
  doc_type STRING,
  doc_status STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'document_event',
  'properties.bootstrap.servers' = '<broker>:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

### Postgres CDC: `customer_credit_profile`

```sql
CREATE TABLE customer_credit_profile (
  customer_id STRING,
  bureau_score INT,
  monthly_income DECIMAL(18,2),
  current_delinquency_flag BOOLEAN,
  existing_monthly_obligation DECIMAL(18,2),
  updated_at TIMESTAMP(3),
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = '<pg-host>',
  'port' = '5432',
  'username' = '<user>',
  'password' = '<password>',
  'database-name' = 'banking',
  'schema-name' = 'public',
  'table-name' = 'customer_credit_profile'
);
```

### Minio reference: `product_policy`

```sql
CREATE TABLE product_policy (
  product_code STRING,
  min_bureau_score INT,
  max_amount DECIMAL(18,2),
  max_dti DECIMAL(10,2)
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://data/ref/product_policy.csv',
  'format' = 'csv'
);
```

### Sink: `loan_predecision`

```sql
CREATE TABLE loan_predecision (
  application_id STRING,
  customer_id STRING,
  decision_band STRING,
  review_queue STRING,
  explanation STRING,
  event_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'loan_predecision',
  'properties.bootstrap.servers' = '<broker>:9092',
  'format' = 'json'
);
```

## Starter Flink SQL

```sql
CREATE TEMPORARY VIEW document_status AS
SELECT
  application_id,
  MAX(CASE WHEN doc_type = 'ID' AND doc_status = 'RECEIVED' THEN 1 ELSE 0 END) AS has_id_doc,
  MAX(CASE WHEN doc_type = 'INCOME_PROOF' AND doc_status = 'RECEIVED' THEN 1 ELSE 0 END) AS has_income_doc
FROM document_event
GROUP BY application_id;

INSERT INTO loan_predecision
SELECT
  a.application_id,
  a.customer_id,
  CASE
    WHEN ds.has_id_doc = 0 OR ds.has_income_doc = 0 THEN 'REVIEW'
    WHEN c.current_delinquency_flag THEN 'REJECT'
    WHEN c.bureau_score < p.min_bureau_score THEN 'REJECT'
    WHEN a.requested_amount > p.max_amount THEN 'REVIEW'
    WHEN (c.existing_monthly_obligation / NULLIF(c.monthly_income, 0)) > p.max_dti THEN 'REVIEW'
    ELSE 'PRE_APPROVE'
  END AS decision_band,
  CASE
    WHEN ds.has_id_doc = 0 OR ds.has_income_doc = 0 THEN 'DOC_CHECK'
    WHEN c.current_delinquency_flag THEN 'CREDIT_RISK'
    WHEN c.bureau_score < p.min_bureau_score THEN 'CREDIT_RISK'
    WHEN a.requested_amount > p.max_amount THEN 'MANUAL_UNDERWRITING'
    WHEN (c.existing_monthly_obligation / NULLIF(c.monthly_income, 0)) > p.max_dti THEN 'AFFORDABILITY_REVIEW'
    ELSE 'AUTO_FLOW'
  END AS review_queue,
  CASE
    WHEN ds.has_id_doc = 0 OR ds.has_income_doc = 0 THEN 'missing mandatory documents'
    WHEN c.current_delinquency_flag THEN 'active delinquency on profile'
    WHEN c.bureau_score < p.min_bureau_score THEN 'bureau score below policy'
    WHEN a.requested_amount > p.max_amount THEN 'requested amount above product policy'
    WHEN (c.existing_monthly_obligation / NULLIF(c.monthly_income, 0)) > p.max_dti THEN 'debt-to-income exceeds policy'
    ELSE 'meets baseline policy'
  END AS explanation,
  a.event_time
FROM loan_application a
LEFT JOIN document_status ds
  ON a.application_id = ds.application_id
LEFT JOIN customer_credit_profile FOR SYSTEM_TIME AS OF a.event_time AS c
  ON a.customer_id = c.customer_id
LEFT JOIN product_policy p
  ON a.product_code = p.product_code;
```

## Fake banking datasets

### `loan_application.csv`

```
application_id,customer_id,product_code,requested_amount,declared_income,tenor_months,channel,event_time
APP001,CUST100,PL_STD,10000.00,3500.00,36,WEB,2026-04-17 11:00:00
APP002,CUST200,PL_STD,45000.00,2800.00,48,WEB,2026-04-17 11:02:00
APP003,CUST300,CAR_PLUS,18000.00,4200.00,60,BRANCH,2026-04-17 11:05:00
```

### `document_event.csv`

```
application_id,doc_type,doc_status,event_time
APP001,ID,RECEIVED,2026-04-17 11:00:10
APP001,INCOME_PROOF,RECEIVED,2026-04-17 11:00:20
APP002,ID,RECEIVED,2026-04-17 11:02:10
APP003,ID,RECEIVED,2026-04-17 11:05:10
APP003,INCOME_PROOF,RECEIVED,2026-04-17 11:05:20
```

### `customer_credit_profile.csv`

```
customer_id,bureau_score,monthly_income,current_delinquency_flag,existing_monthly_obligation,updated_at
CUST100,720,3600.00,false,850.00,2026-04-17 09:00:00
CUST200,590,2900.00,true,1200.00,2026-04-17 09:00:00
CUST300,680,4300.00,false,900.00,2026-04-17 09:00:00
```

### `product_policy.csv`

```
product_code,min_bureau_score,max_amount,max_dti
PL_STD,650,30000.00,0.40
CAR_PLUS,660,25000.00,0.45
```

## Scoring sheet for judges

| Category | Max | Notes |
| :---- | ----: | :---- |
| Policy rule coverage | 20 | Documents, score, amount, affordability |
| Correct enrichment | 20 | Uses profile and policy data correctly |
| Decision explainability | 20 | Clear reasons and queues |
| SQL structure | 15 | Views, readability, maintainability |
| Demo and business framing | 10 | Underwriting story is convincing |
| Stretch goals | 10 | Prioritization or Fluss state |
| Data validation | 5 | Good test cases shown |
