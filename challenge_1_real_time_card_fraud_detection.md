# Challenge Package 1: Real-Time Card Fraud Detection

### Scenario

A retail bank wants to detect suspicious card transactions in real time across POS, e-commerce, and ATM channels.

### Team goal

Build a Flink SQL pipeline that identifies suspicious card behavior in seconds and emits a prioritized alert stream for investigators.

### Business objective

Reduce fraud losses while avoiding noisy alerts.

### Inputs

- Kafka stream: `card_txn`
- Postgres CDC tables: `customer_profile`, `cards`
- Minio reference file: `merchant_risk`
- Optional Fluss sink: `customer_risk_state`

### Mandatory tasks

1. Ingest card transactions from Kafka with event time and watermarking.
2. Join transaction events with customer and card-status data from CDC.
3. Enrich with merchant risk reference data from Minio.
4. Detect at least 1 suspicious pattern:
   - high transaction velocity
   - large amount spike
   - card used while blocked or inactive
   - transaction in a high-risk merchant category or country
5. Emit `fraud_alerts` with severity and human-readable reason.

### Stretch goals

- Add impossible-travel style logic.
- Suppress duplicate alerts within a rolling period.
- Maintain latest customer/card fraud score.

### Deliverables

- Running VVP job(s)
- SQL definitions and final alert query
- Sample alert output
- 5-minute demo

### Difficulty

Medium to high

### Judge focus

Event-time handling, enrichment quality, signal quality, and alert explainability.

## Sample schemas

### Kafka source: `card_txn`

```sql
CREATE TABLE card_txn (
  txn_id STRING,
  card_id STRING,
  customer_id STRING,
  merchant_id STRING,
  merchant_country STRING,
  channel STRING,
  amount DECIMAL(18,2),
  currency STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'card_txn',
  'properties.bootstrap.servers' = '<broker>:9092',
  'properties.group.id' = 'fraud-hackathon',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

### Postgres CDC: `customer_profile`

```sql
CREATE TABLE customer_profile (
  customer_id STRING,
  segment STRING,
  home_country STRING,
  risk_tier STRING,
  account_status STRING,
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
  'table-name' = 'customer_profile'
);
```

### Postgres CDC: `cards`

```sql
CREATE TABLE cards (
  card_id STRING,
  customer_id STRING,
  card_status STRING,
  card_type STRING,
  daily_limit DECIMAL(18,2),
  updated_at TIMESTAMP(3),
  PRIMARY KEY (card_id) NOT ENFORCED
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = '<pg-host>',
  'port' = '5432',
  'username' = '<user>',
  'password' = '<password>',
  'database-name' = 'banking',
  'schema-name' = 'public',
  'table-name' = 'cards'
);
```

### Minio reference: `merchant_risk`

```sql
CREATE TABLE merchant_risk (
  merchant_id STRING,
  mcc STRING,
  merchant_name STRING,
  merchant_risk_level STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://data/ref/merchant_risk.csv',
  'format' = 'csv'
);
```

### Sink: `fraud_alerts`

```sql
CREATE TABLE fraud_alerts (
  alert_id STRING,
  txn_id STRING,
  card_id STRING,
  customer_id STRING,
  severity STRING,
  reason STRING,
  amount DECIMAL(18,2),
  merchant_id STRING,
  event_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'fraud_alerts',
  'properties.bootstrap.servers' = '<broker>:9092',
  'format' = 'json'
);
```

## Starter Flink SQL

```sql
CREATE TEMPORARY VIEW card_5m_features AS
SELECT
  window_start,
  window_end,
  card_id,
  COUNT(*) AS txn_cnt_5m,
  SUM(amount) AS total_amt_5m,
  MAX(amount) AS max_amt_5m
FROM TABLE(
  HOP(
    TABLE card_txn,
    DESCRIPTOR(event_time),
    INTERVAL '1' MINUTE,
    INTERVAL '5' MINUTES
  )
)
GROUP BY window_start, window_end, card_id;

INSERT INTO fraud_alerts
SELECT
  CONCAT('ALERT-', t.txn_id) AS alert_id,
  t.txn_id,
  t.card_id,
  t.customer_id,
  CASE
    WHEN c.card_status <> 'ACTIVE' THEN 'HIGH'
    WHEN f.txn_cnt_5m >= 4 THEN 'HIGH'
    WHEN t.amount > c.daily_limit * 0.60 THEN 'MEDIUM'
    WHEN mr.merchant_risk_level = 'HIGH' THEN 'MEDIUM'
    ELSE 'LOW'
  END AS severity,
  CASE
    WHEN c.card_status <> 'ACTIVE' THEN 'card not active'
    WHEN f.txn_cnt_5m >= 4 THEN 'high card transaction velocity in 5 minutes'
    WHEN t.amount > c.daily_limit * 0.60 THEN 'transaction amount unusually high vs daily limit'
    WHEN mr.merchant_risk_level = 'HIGH' THEN 'merchant marked high risk'
    ELSE 'review recommended'
  END AS reason,
  t.amount,
  t.merchant_id,
  t.event_time
FROM card_txn t
LEFT JOIN cards FOR SYSTEM_TIME AS OF t.event_time AS c
  ON t.card_id = c.card_id
LEFT JOIN customer_profile FOR SYSTEM_TIME AS OF t.event_time AS cp
  ON t.customer_id = cp.customer_id
LEFT JOIN merchant_risk AS mr
  ON t.merchant_id = mr.merchant_id
LEFT JOIN card_5m_features f
  ON t.card_id = f.card_id
 AND t.event_time >= f.window_start
 AND t.event_time < f.window_end
WHERE
  c.card_status <> 'ACTIVE'
  OR f.txn_cnt_5m >= 4
  OR t.amount > c.daily_limit * 0.60
  OR mr.merchant_risk_level = 'HIGH';
```

## Fake banking datasets

### `card_txn.csv`

```
txn_id,card_id,customer_id,merchant_id,merchant_country,channel,amount,currency,event_time
TX1001,CARD100,CUST100,M001,DE,POS,42.50,EUR,2026-04-17 10:00:05
TX1002,CARD100,CUST100,M009,GB,ECOM,499.99,EUR,2026-04-17 10:01:10
TX1003,CARD100,CUST100,M009,GB,ECOM,510.00,EUR,2026-04-17 10:02:11
TX1004,CARD100,CUST100,M009,GB,ECOM,520.00,EUR,2026-04-17 10:03:15
TX1005,CARD200,CUST200,M005,ES,ATM,900.00,EUR,2026-04-17 10:03:20
TX1006,CARD300,CUST300,M777,NG,ECOM,1500.00,EUR,2026-04-17 10:04:55
```

### `customer_profile.csv`

```
customer_id,segment,home_country,risk_tier,account_status,updated_at
CUST100,RETAIL,ES,STANDARD,ACTIVE,2026-04-17 09:00:00
CUST200,SME,ES,HIGH,ACTIVE,2026-04-17 09:00:00
CUST300,RETAIL,FR,HIGH,ACTIVE,2026-04-17 09:00:00
```

### `cards.csv`

```
card_id,customer_id,card_status,card_type,daily_limit,updated_at
CARD100,CUST100,ACTIVE,CREDIT,2000.00,2026-04-17 09:00:00
CARD200,CUST200,BLOCKED,DEBIT,1000.00,2026-04-17 09:00:00
CARD300,CUST300,ACTIVE,CREDIT,2500.00,2026-04-17 09:00:00
```

### `merchant_risk.csv`

```
merchant_id,mcc,merchant_name,merchant_risk_level
M001,5411,City Market,LOW
M005,6011,Downtown ATM,LOW
M009,5967,Online Luxury Outlet,HIGH
M777,7995,Fast Cash Betting,HIGH
```

## Scoring sheet for judges

| Category | Max | Notes |
| :---- | ----: | :---- |
| Correct fraud signals implemented | 25 | At least 3 working fraud rules |
| CDC and reference enrichment quality | 15 | Correct joins, time semantics |
| Event-time and windows | 15 | Proper watermarks and rolling logic |
| Alert usefulness | 15 | Severity and reason are meaningful |
| Demo quality | 10 | Clear architecture and result walkthrough |
| Stretch goals | 10 | Impossible travel, suppression, Fluss state |
| SQL clarity and maintainability | 10 | Readable and structured |
