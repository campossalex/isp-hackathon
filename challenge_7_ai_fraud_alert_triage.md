# Challenge Package 7: AI Extension - Fraud Alert Triage Agent

## One-page challenge brief

### Scenario

Fraud analysts receive too many technical alerts. The bank wants each alert translated into a short, case-ready explanation and next-step recommendation.

### Team goal

Keep fraud detection in Flink SQL, then add an AI stage that consumes only high-risk alerts and produces analyst-ready summaries.

### Business objective

Reduce investigation triage time while preserving deterministic detection logic.

### Design rule

**The LLM does not decide whether a transaction is fraud.**
Flink SQL decides the alert.
The AI layer explains, classifies, or routes the alert.

### Inputs

- Kafka stream: `high_risk_alerts`
- Kafka stream: optional `customer_note`
- Python agent service
- Choice of LLM:
  - internal model endpoint
  - public cloud model API

### Mandatory tasks

1. Produce a clean `high_risk_alerts` stream from the fraud challenge.
2. Call an LLM from a Python service or Flink Agent layer.
3. Return structured JSON with:
   - `priority`
   - `analyst_summary`
   - `recommended_next_step`
   - `confidence_band`
4. Write the enriched result to `analyst_case_queue`.

### Stretch goals

- Add prompt grounding with customer and merchant context
- Add redaction or masking before model call
- Add deterministic fallback when the model is unavailable
- Add a small evaluation set for summary quality

### Deliverables

- SQL + Python workflow
- Prompt design
- Example alert and case summary
- Demo of masking or governance approach

### Difficulty

High

### Judge focus

Grounded AI usage, safety, explainability, and operational realism.

### Recommended hackathon posture

- Use **synthetic or masked alerts**
- Keep the model focused on **explanation and routing**
- Prefer **internal hosting** for a more production-like banking story
- Use **public cloud LLM service** (Flink SQL ML_PREDICT)

## Sample schemas

### Kafka source: `high_risk_alerts`

```sql
CREATE TABLE high_risk_alerts (
  alert_id STRING,
  txn_id STRING,
  card_id STRING,
  customer_id STRING,
  severity STRING,
  reason STRING,
  amount DECIMAL(18,2),
  merchant_id STRING,
  merchant_country STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'high_risk_alerts',
  'properties.bootstrap.servers' = '<broker>:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

### Sink: `alerts_for_ai`

```sql
CREATE TABLE alerts_for_ai (
  alert_id STRING,
  customer_id STRING,
  masked_card_id STRING,
  severity STRING,
  reason STRING,
  amount_band STRING,
  merchant_id STRING,
  merchant_country STRING,
  event_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'alerts_for_ai',
  'properties.bootstrap.servers' = '<broker>:9092',
  'format' = 'json'
);
```

### Sink from AI service: `analyst_case_queue`

```sql
CREATE TABLE analyst_case_queue (
  case_id STRING,
  alert_id STRING,
  priority STRING,
  analyst_summary STRING,
  recommended_next_step STRING,
  confidence_band STRING,
  event_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'analyst_case_queue',
  'properties.bootstrap.servers' = '<broker>:9092',
  'format' = 'json'
);
```

## Starter Flink SQL

```sql
INSERT INTO alerts_for_ai
SELECT
  alert_id,
  customer_id,
  CONCAT('****', RIGHT(card_id, 4)) AS masked_card_id,
  severity,
  reason,
  CASE
    WHEN amount >= 1000 THEN 'GE_1000'
    WHEN amount >= 500 THEN '500_TO_999'
    ELSE 'LT_500'
  END AS amount_band,
  merchant_id,
  merchant_country,
  event_time
FROM high_risk_alerts
WHERE severity IN ('HIGH', 'CRITICAL');
```

## Open AI LLM service

https://github.com/campossalex/flink_cookbook/blob/main/ml_predict.md

## Fake banking datasets

### `high_risk_alerts.csv`

```
alert_id,txn_id,card_id,customer_id,severity,reason,amount,merchant_id,merchant_country,event_time
ALERT-TX1003,TX1003,CARD100,CUST100,HIGH,high card transaction velocity in 5 minutes,510.00,M009,GB,2026-04-17 10:02:11
ALERT-TX1005,TX1005,CARD200,CUST200,CRITICAL,card not active,900.00,M005,ES,2026-04-17 10:03:20
ALERT-TX1006,TX1006,CARD300,CUST300,HIGH,merchant marked high risk,1500.00,M777,NG,2026-04-17 10:04:55
```

### Optional evaluation set: `expected_case_labels.csv`

```
alert_id,expected_priority,expected_next_step
ALERT-TX1003,P2,manual_review
ALERT-TX1005,P1,block_card
ALERT-TX1006,P2,call_customer
```

## Scoring sheet for judges

| Category | Max | Notes |
| :---- | ----: | :---- |
| Grounded AI usage | 20 | Output stays within supplied facts |
| Workflow design | 15 | Clean handoff from SQL to AI |
| Data safety posture | 15 | Masking, redaction, internal/public-cloud reasoning |
| Output usefulness | 20 | Summary and next step help analysts |
| Fallback strategy | 10 | Deterministic behavior if LLM is unavailable |
| Demo quality | 10 | Clear before/after triage story |
| Stretch goals | 10 | Evaluation set, prompt guardrails, routing |
