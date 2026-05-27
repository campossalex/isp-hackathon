# Challenge Package 5: Payment Operations SLA and Transfer Monitoring

## One-page challenge brief

### Scenario

The payment operations team needs live visibility into stuck, delayed, and rejected payments.

### Team goal

Build Flink SQL to correlate payment lifecycle events and prioritize operational incidents.

### Business objective

Reduce SLA breaches and speed up payment repair.

### Inputs

- Kafka stream: `payment_event`
- Postgres CDC tables: `payment_sla_rule`, `customer_priority`
- Minio reference: `reason_code_map`

### Mandatory tasks

1. Reconstruct a payment lifecycle from event stream data.
2. Detect rejected or delayed payments.
3. Compare processing time against the corridor or product SLA.
4. Produce an `ops_priority_queue` sink.

### Stretch goals

- Add corridor-level failure rate dashboard feed.
- Rank stuck payments by customer tier.
- Add reason-code grouping.

### Deliverables

- Lifecycle SQL
- SLA or rejection alerts
- Demo with operations queue

### Difficulty

Medium to high

### Judge focus

Lifecycle correlation, operational usefulness, and prioritization.

## Sample schemas

### Kafka source: `payment_event`

```sql
CREATE TABLE payment_event (
  payment_id STRING,
  customer_id STRING,
  corridor STRING,
  product_type STRING,
  event_type STRING,
  reason_code STRING,
  amount DECIMAL(18,2),
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'payment_event',
  'properties.bootstrap.servers' = '<broker>:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

### Postgres CDC: `payment_sla_rule`

```sql
CREATE TABLE payment_sla_rule (
  corridor STRING,
  product_type STRING,
  sla_minutes INT,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (corridor, product_type) NOT ENFORCED
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = '<pg-host>',
  'port' = '5432',
  'username' = '<user>',
  'password' = '<password>',
  'database-name' = 'banking',
  'schema-name' = 'public',
  'table-name' = 'payment_sla_rule'
);
```

### Postgres CDC: `customer_priority`

```sql
CREATE TABLE customer_priority (
  customer_id STRING,
  customer_tier STRING,
  relationship_flag BOOLEAN,
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
  'table-name' = 'customer_priority'
);
```

### Sink: `ops_priority_queue`

```sql
CREATE TABLE ops_priority_queue (
  queue_id STRING,
  payment_id STRING,
  customer_id STRING,
  priority STRING,
  issue_type STRING,
  explanation STRING,
  event_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'ops_priority_queue',
  'properties.bootstrap.servers' = '<broker>:9092',
  'format' = 'json'
);
```

## Starter Flink SQL

```sql
CREATE TEMPORARY VIEW payment_lifecycle AS
SELECT
  payment_id,
  MAX(customer_id) AS customer_id,
  MAX(corridor) AS corridor,
  MAX(product_type) AS product_type,
  MAX(CASE WHEN event_type = 'INITIATED' THEN event_time END) AS initiated_ts,
  MAX(CASE WHEN event_type = 'VALIDATED' THEN event_time END) AS validated_ts,
  MAX(CASE WHEN event_type = 'SETTLED' THEN event_time END) AS settled_ts,
  MAX(CASE WHEN event_type = 'REJECTED' THEN event_time END) AS rejected_ts,
  MAX(reason_code) AS latest_reason_code
FROM payment_event
GROUP BY payment_id;

INSERT INTO ops_priority_queue
SELECT
  CONCAT('OPS-', p.payment_id) AS queue_id,
  p.payment_id,
  p.customer_id,
  CASE
    WHEN p.rejected_ts IS NOT NULL AND cp.customer_tier = 'VIP' THEN 'P1'
    WHEN p.rejected_ts IS NOT NULL THEN 'P2'
    WHEN TIMESTAMPDIFF(MINUTE, p.initiated_ts, COALESCE(p.settled_ts, CURRENT_TIMESTAMP)) > s.sla_minutes
      THEN 'P2'
    ELSE 'P3'
  END AS priority,
  CASE
    WHEN p.rejected_ts IS NOT NULL THEN 'REJECTED_PAYMENT'
    WHEN TIMESTAMPDIFF(MINUTE, p.initiated_ts, COALESCE(p.settled_ts, CURRENT_TIMESTAMP)) > s.sla_minutes
      THEN 'SLA_BREACH'
    ELSE 'REVIEW'
  END AS issue_type,
  CASE
    WHEN p.rejected_ts IS NOT NULL THEN CONCAT('Payment rejected with reason code ', COALESCE(p.latest_reason_code, 'UNKNOWN'))
    WHEN TIMESTAMPDIFF(MINUTE, p.initiated_ts, COALESCE(p.settled_ts, CURRENT_TIMESTAMP)) > s.sla_minutes
      THEN 'Payment elapsed time exceeded configured SLA'
    ELSE 'Operational review recommended'
  END AS explanation,
  COALESCE(p.rejected_ts, p.settled_ts, p.initiated_ts) AS event_time
FROM payment_lifecycle p
JOIN payment_sla_rule s
  ON p.corridor = s.corridor
 AND p.product_type = s.product_type
LEFT JOIN customer_priority FOR SYSTEM_TIME AS OF COALESCE(p.initiated_ts, CURRENT_TIMESTAMP) AS cp
  ON p.customer_id = cp.customer_id
WHERE
  p.rejected_ts IS NOT NULL
  OR TIMESTAMPDIFF(MINUTE, p.initiated_ts, COALESCE(p.settled_ts, CURRENT_TIMESTAMP)) > s.sla_minutes;
```

## Fake banking datasets

### `payment_event.csv`

```
payment_id,customer_id,corridor,product_type,event_type,reason_code,amount,event_time
PAY001,CUST100,ES-DE,SEPA,INITIATED,,5000.00,2026-04-17 09:00:00
PAY001,CUST100,ES-DE,SEPA,VALIDATED,,5000.00,2026-04-17 09:01:00
PAY001,CUST100,ES-DE,SEPA,SETTLED,,5000.00,2026-04-17 09:08:00
PAY002,CUST200,ES-GB,SWIFT,INITIATED,,25000.00,2026-04-17 09:00:00
PAY002,CUST200,ES-GB,SWIFT,REJECTED,R57,25000.00,2026-04-17 09:12:00
PAY003,CUST300,ES-US,SWIFT,INITIATED,,45000.00,2026-04-17 09:05:00
PAY003,CUST300,ES-US,SWIFT,VALIDATED,,45000.00,2026-04-17 09:08:00
```

### `payment_sla_rule.csv`

```
corridor,product_type,sla_minutes,updated_at
ES-DE,SEPA,15,2026-04-17 08:00:00
ES-GB,SWIFT,10,2026-04-17 08:00:00
ES-US,SWIFT,20,2026-04-17 08:00:00
```

### `customer_priority.csv`

```
customer_id,customer_tier,relationship_flag,updated_at
CUST100,STANDARD,false,2026-04-17 08:00:00
CUST200,VIP,true,2026-04-17 08:00:00
CUST300,STANDARD,false,2026-04-17 08:00:00
```

## Scoring sheet for judges

| Category | Max | Notes |
| :---- | ----: | :---- |
| Lifecycle reconstruction | 20 | Good state interpretation |
| SLA breach logic | 15 | Accurate timing logic |
| Prioritization quality | 20 | Customer tier and issue severity used |
| Operational usefulness | 15 | Queue is actionable |
| SQL design | 10 | Clean modeling choices |
| Demo quality | 10 | Clear payment story |
| Stretch goals | 10 | Corridor dashboard, grouping, ranking |
