# Challenge Package 2: AML Transaction Monitoring and Structuring Detection

## One-page challenge brief

### Scenario

A bank compliance team needs near-real-time monitoring for suspicious cash and transfer patterns.

### Team goal

Build a Flink SQL pipeline that flags suspicious activity for AML review.

### Business objective

Help investigators prioritize suspicious patterns quickly without waiting for end-of-day batch reports.

### Inputs

- Kafka stream: `account_transfer`
- Kafka stream: `cash_deposit`
- Postgres CDC tables: `customer_kyc`, `account_master`
- Minio reference: `high_risk_country`

### Mandatory tasks

1. Detect potential structuring:
   - multiple near-threshold cash deposits in a short period
2. Detect rapid movement of funds:
   - inbound then outbound transfers within a tight time window
3. Flag transfers to high-risk countries
4. Emit `aml_alerts` with rule name, severity, and explanation

### Stretch goals

- Link alerts by customer rather than account only
- Add customer segment and KYC mismatch logic
- Create a single `case_feed` sink with aggregated alert counts

### Deliverables

- AML monitoring SQL
- Example alert output
- Rule explanation during demo

### Difficulty

Medium to high

### Judge focus

Detection logic quality, enrichment with KYC data, and clarity of alert reasons.

## Sample schemas

### Kafka source: `cash_deposit`

```sql
CREATE TABLE cash_deposit (
  deposit_id STRING,
  account_id STRING,
  customer_id STRING,
  branch_id STRING,
  amount DECIMAL(18,2),
  currency STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'cash_deposit',
  'properties.bootstrap.servers' = '<broker>:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

### Kafka source: `account_transfer`

```sql
CREATE TABLE account_transfer (
  transfer_id STRING,
  from_account_id STRING,
  to_account_id STRING,
  customer_id STRING,
  beneficiary_country STRING,
  amount DECIMAL(18,2),
  transfer_type STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'account_transfer',
  'properties.bootstrap.servers' = '<broker>:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

### Postgres CDC: `customer_kyc`

```sql
CREATE TABLE customer_kyc (
  customer_id STRING,
  occupation STRING,
  expected_cash_level STRING,
  kyc_risk_tier STRING,
  pep_flag BOOLEAN,
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
  'table-name' = 'customer_kyc'
);
```

### Postgres CDC: `account_master`

```sql
CREATE TABLE account_master (
  account_id STRING,
  customer_id STRING,
  account_type STRING,
  country_code STRING,
  status STRING,
  opened_at TIMESTAMP(3),
  updated_at TIMESTAMP(3),
  PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = '<pg-host>',
  'port' = '5432',
  'username' = '<user>',
  'password' = '<password>',
  'database-name' = 'banking',
  'schema-name' = 'public',
  'table-name' = 'account_master'
);
```

### Minio reference: `high_risk_country`

```sql
CREATE TABLE high_risk_country (
  country_code STRING,
  risk_reason STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://data/ref/high_risk_country.csv',
  'format' = 'csv'
);
```

### Sink: `aml_alerts`

```sql
CREATE TABLE aml_alerts (
  alert_id STRING,
  customer_id STRING,
  account_id STRING,
  rule_name STRING,
  severity STRING,
  amount DECIMAL(18,2),
  explanation STRING,
  event_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'aml_alerts',
  'properties.bootstrap.servers' = '<broker>:9092',
  'format' = 'json'
);
```

## Starter Flink SQL

```sql
CREATE TEMPORARY VIEW deposit_2h_rollup AS
SELECT
  window_start,
  window_end,
  account_id,
  customer_id,
  COUNT(*) AS deposit_cnt_2h,
  SUM(amount) AS deposit_sum_2h
FROM TABLE(
  HOP(
    TABLE cash_deposit,
    DESCRIPTOR(event_time),
    INTERVAL '30' MINUTE,
    INTERVAL '2' HOUR
  )
)
WHERE amount BETWEEN 8000 AND 9999
GROUP BY window_start, window_end, account_id, customer_id;

INSERT INTO aml_alerts
SELECT
  CONCAT('AML-STRUCT-', d.account_id, '-', CAST(d.window_end AS STRING)) AS alert_id,
  d.customer_id,
  d.account_id,
  'STRUCTURING' AS rule_name,
  CASE
    WHEN d.deposit_cnt_2h >= 4 OR d.deposit_sum_2h >= 30000 THEN 'HIGH'
    ELSE 'MEDIUM'
  END AS severity,
  d.deposit_sum_2h AS amount,
  CONCAT('Multiple near-threshold cash deposits in 2 hours: ', CAST(d.deposit_cnt_2h AS STRING)) AS explanation,
  d.window_end AS event_time
FROM deposit_2h_rollup d
WHERE d.deposit_cnt_2h >= 3
UNION ALL
SELECT
  CONCAT('AML-COUNTRY-', t.transfer_id) AS alert_id,
  t.customer_id,
  t.from_account_id AS account_id,
  'HIGH_RISK_COUNTRY' AS rule_name,
  'HIGH' AS severity,
  t.amount,
  CONCAT('Transfer to high-risk country: ', t.beneficiary_country) AS explanation,
  t.event_time
FROM account_transfer t
JOIN high_risk_country h
  ON t.beneficiary_country = h.country_code;
```

## Fake banking datasets

### `cash_deposit.csv`

```
deposit_id,account_id,customer_id,branch_id,amount,currency,event_time
D001,AC100,CUST100,B001,9800.00,EUR,2026-04-17 09:10:00
D002,AC100,CUST100,B001,9700.00,EUR,2026-04-17 09:40:00
D003,AC100,CUST100,B002,9900.00,EUR,2026-04-17 10:05:00
D004,AC200,CUST200,B001,1200.00,EUR,2026-04-17 10:15:00
D005,AC300,CUST300,B003,9500.00,EUR,2026-04-17 10:20:00
```

### `account_transfer.csv`

```
transfer_id,from_account_id,to_account_id,customer_id,beneficiary_country,amount,transfer_type,event_time
T001,AC100,AC900,CUST100,AE,15000.00,WIRE,2026-04-17 10:30:00
T002,AC200,AC901,CUST200,ES,3500.00,SEPA,2026-04-17 10:32:00
T003,AC300,AC902,CUST300,IR,22000.00,WIRE,2026-04-17 10:35:00
T004,AC300,AC903,CUST300,GB,5000.00,SEPA,2026-04-17 10:40:00
```

### `customer_kyc.csv`

```
customer_id,occupation,expected_cash_level,kyc_risk_tier,pep_flag,updated_at
CUST100,RESTAURANT_OWNER,MEDIUM,MEDIUM,false,2026-04-17 08:00:00
CUST200,CONSULTANT,LOW,LOW,false,2026-04-17 08:00:00
CUST300,IMPORT_EXPORT,HIGH,HIGH,true,2026-04-17 08:00:00
```

### `high_risk_country.csv`

```
country_code,risk_reason
IR,Sanctions or elevated AML exposure
AE,Additional review required for selected corridors
```

## Scoring sheet for judges

| Category | Max | Notes |
| :---- | ----: | :---- |
| Structuring logic | 20 | Frequency and threshold logic |
| Cross-border risk detection | 15 | High-risk geography enrichment |
| KYC / CDC enrichment | 15 | Customer context used correctly |
| Alert quality | 15 | Clear rule naming and explanation |
| SQL design | 15 | Good views and modularity |
| Demo clarity | 10 | Shows why alerts matter |
| Stretch goals | 10 | Account linking, case feed, prioritization |
