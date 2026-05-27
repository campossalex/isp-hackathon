# Challenge Package 6: Customer 360, Churn Risk, and Next-Best-Action

## One-page challenge brief

### Scenario

A retail bank wants a real-time customer engagement view to identify churn risk and trigger the next best action.

### Team goal

Combine digital, payment, and support signals into an actionable customer state feed.

### Business objective

Improve retention and increase timely outreach.

### Inputs

- Kafka stream: `customer_activity`
- Kafka stream: `support_ticket`
- Postgres CDC tables: `customer_segment`, `product_holding`
- Optional Fluss sink: `customer_360_state`

### Mandatory tasks

1. Build rolling engagement metrics.
2. Detect basic churn-risk conditions:
   - reduced digital activity
   - failed payment or card decline
   - open support issues
3. Emit a `customer_action_feed` with segment, risk level, and recommended action.

### Stretch goals

- Personalize action by customer segment
- Add product-holding-aware upsell action
- Maintain latest customer summary

### Deliverables

- Customer state SQL
- Action feed output
- Demo of at least 3 customer stories

### Difficulty

Medium

### Judge focus

Customer reasoning, simplicity, and actionability.

## Sample schemas

### Kafka source: `customer_activity`

```sql
CREATE TABLE customer_activity (
  event_id STRING,
  customer_id STRING,
  activity_type STRING,
  outcome STRING,
  channel STRING,
  amount DECIMAL(18,2),
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'customer_activity',
  'properties.bootstrap.servers' = '<broker>:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

### Kafka source: `support_ticket`

```sql
CREATE TABLE support_ticket (
  ticket_id STRING,
  customer_id STRING,
  ticket_status STRING,
  priority STRING,
  topic STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'support_ticket',
  'properties.bootstrap.servers' = '<broker>:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

### Postgres CDC: `customer_segment`

```sql
CREATE TABLE customer_segment (
  customer_id STRING,
  segment STRING,
  age_band STRING,
  digital_first_flag BOOLEAN,
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
  'table-name' = 'customer_segment'
);
```

### Sink: `customer_action_feed`

```sql
CREATE TABLE customer_action_feed (
  action_id STRING,
  customer_id STRING,
  risk_level STRING,
  recommended_action STRING,
  explanation STRING,
  event_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'customer_action_feed',
  'properties.bootstrap.servers' = '<broker>:9092',
  'format' = 'json'
);
```

## Starter Flink SQL

```sql
CREATE TEMPORARY VIEW customer_activity_1h AS
SELECT
  window_start,
  window_end,
  customer_id,
  COUNT(*) AS activity_cnt_1h,
  SUM(CASE WHEN outcome = 'FAIL' THEN 1 ELSE 0 END) AS failed_cnt_1h
FROM TABLE(
  HOP(
    TABLE customer_activity,
    DESCRIPTOR(event_time),
    INTERVAL '15' MINUTE,
    INTERVAL '1' HOUR
  )
)
GROUP BY window_start, window_end, customer_id;

CREATE TEMPORARY VIEW open_tickets AS
SELECT
  customer_id,
  SUM(CASE WHEN ticket_status <> 'CLOSED' THEN 1 ELSE 0 END) AS open_ticket_cnt
FROM support_ticket
GROUP BY customer_id;

INSERT INTO customer_action_feed
SELECT
  CONCAT('ACT-', a.customer_id, '-', CAST(a.window_end AS STRING)) AS action_id,
  a.customer_id,
  CASE
    WHEN a.failed_cnt_1h >= 2 OR o.open_ticket_cnt >= 2 THEN 'HIGH'
    WHEN a.activity_cnt_1h <= 1 OR o.open_ticket_cnt = 1 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS risk_level,
  CASE
    WHEN a.failed_cnt_1h >= 2 THEN 'proactive outreach from service team'
    WHEN o.open_ticket_cnt >= 2 THEN 'escalate customer care callback'
    WHEN a.activity_cnt_1h <= 1 THEN 'send engagement reminder'
    ELSE 'no action'
  END AS recommended_action,
  CASE
    WHEN a.failed_cnt_1h >= 2 THEN 'multiple failed customer interactions in last hour'
    WHEN o.open_ticket_cnt >= 2 THEN 'multiple open support tickets'
    WHEN a.activity_cnt_1h <= 1 THEN 'low recent engagement'
    ELSE 'customer activity stable'
  END AS explanation,
  a.window_end AS event_time
FROM customer_activity_1h a
LEFT JOIN open_tickets o
  ON a.customer_id = o.customer_id
LEFT JOIN customer_segment FOR SYSTEM_TIME AS OF a.window_end AS s
  ON a.customer_id = s.customer_id
WHERE a.failed_cnt_1h >= 1 OR a.activity_cnt_1h <= 1 OR o.open_ticket_cnt >= 1;
```

## Fake banking datasets

### `customer_activity.csv`

```
event_id,customer_id,activity_type,outcome,channel,amount,event_time
E001,CUST100,LOGIN,SUCCESS,APP,0.00,2026-04-17 10:00:00
E002,CUST100,CARD_PAYMENT,FAIL,APP,120.00,2026-04-17 10:10:00
E003,CUST100,CARD_PAYMENT,FAIL,APP,80.00,2026-04-17 10:20:00
E004,CUST200,LOGIN,SUCCESS,WEB,0.00,2026-04-17 10:15:00
E005,CUST300,LOGIN,SUCCESS,APP,0.00,2026-04-17 09:00:00
```

### `support_ticket.csv`

```
ticket_id,customer_id,ticket_status,priority,topic,event_time
TK001,CUST100,OPEN,HIGH,CARD_ISSUE,2026-04-17 10:05:00
TK002,CUST100,OPEN,MEDIUM,MOBILE_APP,2026-04-17 10:25:00
TK003,CUST200,CLOSED,LOW,GENERAL_QUERY,2026-04-17 10:00:00
```

### `customer_segment.csv`

```
customer_id,segment,age_band,digital_first_flag,updated_at
CUST100,PREMIUM,35_44,true,2026-04-17 08:00:00
CUST200,MASS,25_34,true,2026-04-17 08:00:00
CUST300,AFFLUENT,45_54,false,2026-04-17 08:00:00
```

## Scoring sheet for judges

| Category | Max | Notes |
| :---- | ----: | :---- |
| Customer state logic | 20 | Good engagement and ticket signals |
| Recommended action quality | 20 | Practical and justified |
| CDC / enrichment use | 15 | Segment context incorporated |
| SQL quality | 15 | Clean and understandable |
| Demo narrative | 10 | Good customer stories |
| Stretch goals | 10 | Segment-aware actions or Fluss state |
| Validation and sample outputs | 10 | Clear evidence of correctness |
