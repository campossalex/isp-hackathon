# Challenge Package 3: Intraday Liquidity and Large Exposure Monitoring

## One-page challenge brief

### Scenario

The treasury function wants live visibility into intraday cash position and concentration risk.

### Team goal

Build streaming SQL that computes near-real-time net cash movement and detects exposure breaches by legal entity and counterparty.

### Business objective

Surface large outflows and concentration issues before end-of-day reconciliation.

### Inputs

- Kafka stream: `payment_flow`
- Kafka stream: `treasury_movement`
- Postgres CDC tables: `legal_entity_account`, `counterparty_limit`
- Optional Fluss sink: `liquidity_dashboard_state`

### Mandatory tasks

1. Calculate net inflow/outflow by legal entity in rolling windows.
2. Aggregate exposure by counterparty.
3. Compare exposure to configured thresholds from CDC.
4. Emit liquidity and exposure breach alerts.

### Stretch goals

- Add desk or currency-level slicing.
- Create top-5 counterparties by current exposure.
- Store latest entity position.

### Deliverables

- SQL job
- Alert and summary outputs
- Treasury-oriented demo

### Difficulty

Medium to high

### Judge focus

Financial logic, grouping strategy, and actionable alerts.

## Sample schemas

### Kafka source: `payment_flow`

```sql
CREATE TABLE payment_flow (
  payment_id STRING,
  account_id STRING,
  counterparty_id STRING,
  direction STRING,
  amount DECIMAL(18,2),
  currency STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'payment_flow',
  'properties.bootstrap.servers' = '<broker>:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

### Kafka source: `treasury_movement`

```sql
CREATE TABLE treasury_movement (
  movement_id STRING,
  legal_entity_id STRING,
  movement_type STRING,
  amount DECIMAL(18,2),
  currency STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'treasury_movement',
  'properties.bootstrap.servers' = '<broker>:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

### Postgres CDC: `legal_entity_account`

```sql
CREATE TABLE legal_entity_account (
  account_id STRING,
  legal_entity_id STRING,
  desk STRING,
  country_code STRING,
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
  'table-name' = 'legal_entity_account'
);
```

### Postgres CDC: `counterparty_limit`

```sql
CREATE TABLE counterparty_limit (
  counterparty_id STRING,
  exposure_limit DECIMAL(18,2),
  limit_currency STRING,
  risk_bucket STRING,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (counterparty_id) NOT ENFORCED
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = '<pg-host>',
  'port' = '5432',
  'username' = '<user>',
  'password' = '<password>',
  'database-name' = 'banking',
  'schema-name' = 'public',
  'table-name' = 'counterparty_limit'
);
```

### Sink: `liquidity_alerts`

```sql
CREATE TABLE liquidity_alerts (
  alert_id STRING,
  legal_entity_id STRING,
  counterparty_id STRING,
  alert_type STRING,
  severity STRING,
  metric_value DECIMAL(18,2),
  explanation STRING,
  event_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'liquidity_alerts',
  'properties.bootstrap.servers' = '<broker>:9092',
  'format' = 'json'
);
```

## Starter Flink SQL

```sql
CREATE TEMPORARY VIEW entity_flow_15m AS
SELECT
  window_start,
  window_end,
  le.legal_entity_id,
  SUM(CASE WHEN p.direction = 'IN' THEN p.amount ELSE -1 * p.amount END) AS net_flow_15m
FROM TABLE(
  HOP(
    TABLE payment_flow,
    DESCRIPTOR(event_time),
    INTERVAL '5' MINUTE,
    INTERVAL '15' MINUTES
  )
) AS p
LEFT JOIN legal_entity_account FOR SYSTEM_TIME AS OF p.event_time AS le
  ON p.account_id = le.account_id
GROUP BY window_start, window_end, le.legal_entity_id;

CREATE TEMPORARY VIEW counterparty_exposure_15m AS
SELECT
  window_start,
  window_end,
  p.counterparty_id,
  SUM(CASE WHEN p.direction = 'OUT' THEN p.amount ELSE 0 END) AS gross_out_15m
FROM TABLE(
  HOP(
    TABLE payment_flow,
    DESCRIPTOR(event_time),
    INTERVAL '5' MINUTE,
    INTERVAL '15' MINUTES
  )
) AS p
GROUP BY window_start, window_end, p.counterparty_id;

INSERT INTO liquidity_alerts
SELECT
  CONCAT('LIQ-', legal_entity_id, '-', CAST(window_end AS STRING)) AS alert_id,
  legal_entity_id,
  CAST(NULL AS STRING) AS counterparty_id,
  'NET_OUTFLOW_SPIKE' AS alert_type,
  CASE WHEN ABS(net_flow_15m) > 500000 THEN 'HIGH' ELSE 'MEDIUM' END AS severity,
  net_flow_15m AS metric_value,
  'Net entity flow breach in rolling 15-minute window' AS explanation,
  window_end AS event_time
FROM entity_flow_15m
WHERE net_flow_15m < -250000
UNION ALL
SELECT
  CONCAT('EXP-', c.counterparty_id, '-', CAST(c.window_end AS STRING)) AS alert_id,
  CAST(NULL AS STRING) AS legal_entity_id,
  c.counterparty_id,
  'COUNTERPARTY_LIMIT_BREACH' AS alert_type,
  'HIGH' AS severity,
  c.gross_out_15m AS metric_value,
  'Counterparty outflow exceeds configured exposure limit' AS explanation,
  c.window_end AS event_time
FROM counterparty_exposure_15m c
JOIN counterparty_limit l
  ON c.counterparty_id = l.counterparty_id
WHERE c.gross_out_15m > l.exposure_limit;
```

## Fake banking datasets

### `payment_flow.csv`

```
payment_id,account_id,counterparty_id,direction,amount,currency,event_time
P001,ACC01,CP01,OUT,120000.00,EUR,2026-04-17 09:00:00
P002,ACC01,CP01,OUT,180000.00,EUR,2026-04-17 09:04:00
P003,ACC02,CP02,IN,90000.00,EUR,2026-04-17 09:06:00
P004,ACC01,CP01,OUT,250000.00,EUR,2026-04-17 09:09:00
P005,ACC03,CP03,OUT,70000.00,EUR,2026-04-17 09:10:00
```

### `treasury_movement.csv`

```
movement_id,legal_entity_id,movement_type,amount,currency,event_time
TM001,LE01,FUNDING_IN,500000.00,EUR,2026-04-17 08:50:00
TM002,LE01,FUNDING_OUT,100000.00,EUR,2026-04-17 09:20:00
TM003,LE02,FUNDING_IN,250000.00,EUR,2026-04-17 09:00:00
```

### `legal_entity_account.csv`

```
account_id,legal_entity_id,desk,country_code,updated_at
ACC01,LE01,TREASURY,DE,2026-04-17 08:00:00
ACC02,LE01,TREASURY,DE,2026-04-17 08:00:00
ACC03,LE02,PAYMENTS,FR,2026-04-17 08:00:00
```

### `counterparty_limit.csv`

```
counterparty_id,exposure_limit,limit_currency,risk_bucket,updated_at
CP01,400000.00,EUR,HIGH,2026-04-17 08:00:00
CP02,300000.00,EUR,MEDIUM,2026-04-17 08:00:00
CP03,100000.00,EUR,LOW,2026-04-17 08:00:00
```

## Scoring sheet for judges

| Category | Max | Notes |
| :---- | ----: | :---- |
| Net liquidity calculations | 20 | Correct direction logic and rollups |
| Counterparty exposure logic | 20 | Uses limits correctly |
| CDC joins and model quality | 15 | Entity/account mapping is sound |
| Alert actionability | 15 | Treasury team can use the result |
| Demo clarity | 10 | Good explanation of risk story |
| Stretch goals | 10 | Top-N, Fluss state, desk slicing |
| SQL quality | 10 | Understandable and modular |
