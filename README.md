# AML Transaction Data Quality Pipeline

A production-style data platform that ingests streaming financial transactions, validates and quarantines bad data, produces trusted AML-ready metrics, and enforces data quality with observable PASS/FAIL checks.

This project demonstrates how **data reliability and trust** are built end-to-end.

---

## Tech Stack

- **Streaming**: Kafka
- **Processing**: Apache Spark (Structured Streaming + Batch)
- **Storage**: S3 (LocalStack)
- **Quality & Observability**: Soda
- **Analytics Engine**: DuckDB
- **Exports**: Pandas (CSV / Excel)
- **Infrastructure**: Docker Compose

---

## Why this project

In early 2024, the collapse of fintech middleware provider Synapse left thousands of customers temporarily unable to access their money. What stood out in the reporting was not just the business failure, but the confusion around transaction records and balances across systems.

Different parties had data. Not all of it agreed. Reconciling what actually happened took time, during which customers were left in limbo.

That incident highlighted a pattern that appears again and again in financial systems. Failures rarely start with infrastructure going down. They start with data becoming unreliable while systems continue to run.

Transaction pipelines assume that upstream data is correct, fresh, and complete. When those assumptions quietly break, downstream reporting, risk monitoring, and compliance workflows are affected long before anyone notices.

I built this project to explore how a financial data pipeline can be designed so that those assumptions are enforced explicitly. The goal is to surface incorrect or stale transaction data early, isolate it clearly, and prevent it from silently influencing aggregates and reports.

This project focuses on transaction data because it is where mistakes carry real consequences. Money moves. Accounts are flagged. Decisions are made. A system handling this data should make data quality visible by design, not by accident.

This pipeline shows how to:
- Prevent bad data from silently contaminating reports
- Isolate invalid records with clear failure reasons
- Continuously validate data health using automated checks
---
## The Data Profile

This pipeline processes financial transaction events — the most sensitive kind of operational data in fintech.

Each event represents a moment where money attempted to move:

a transaction amount

a currency

a status (success, failure, flagged)

a timestamp

a risk signal

---

## Architecture Overview

**Streaming → Validation → Trusted Metrics → Quality Checks**

Kafka (Transactions)
↓
Spark Structured Streaming
↓
Bronze (Raw JSON, immutable)
↓
Silver (Validated records)
├── Quarantine (Invalid records + error_reason)
↓
Gold (Daily AML metrics)
↓
Exports (CSV / Excel)
↓
Soda Data Quality Checks (PASS / FAIL)


![Architecture](https://github.com/AtharvaGitProfile/financial-data-quality-pipeline/blob/main/Architecture.png)

## Streaming Ingestion with Kafka

Kafka is used as the entry point for transaction events.

A Kafka topic acts like a shared conveyor belt:
Producers place transaction events onto it; 
Consumers read at their own pace;
Events are ordered, durable, and replayable

This matters because in financial systems:

Data often arrives out of order;
Downstream consumers may fail and recover;
Replaying history is not optional;

Kafka gives us time as a first-class concept, not an accident.

## Data Layers

### Bronze
- Raw transaction events from Kafka
- No transformations
- Used for traceability and replay

### Silver
- Schema validation and type checks
- Required fields enforced
- Invalid records routed to Quarantine** with explicit `error_reason`

### Quarantine
- Preserves bad records for audit and debugging
- Ensures data issues are visible

### Gold
- Business-ready AML metrics:
  - Transaction count
  - Total and average amount
  - High-risk transaction count
- Used for reporting and downstream analytics
  
![Gold Metrics](https://github.com/AtharvaGitProfile/financial-data-quality-pipeline/blob/main/Gold%20Metrics%20Specfications%20Document.png)

---

## Data Quality & Observability (Soda)

Quality checks are executed against Silver, Quarantine, and Gold using Soda + DuckDB.

### Examples
- Row count > 0
- No missing critical fields
- No negative or invalid amounts
- Quarantine must contain records when bad data exists
- **Freshness check (intentional FAIL) to demonstrate alerting

---

## What the Soda checks are actually doing ?

### ✅ Gold data quality PASS report
A passing Gold Soda scan means more than “the job ran successfully.”

It means:
> Aggregates were computed from validated data only
> Business dimensions are complete and usable
> Metrics behave consistently and predictably
> The dataset is safe to consume without manual inspection

In other words, the Gold layer can be treated as a trusted source, not a provisional one.

This is the standard that financial reporting and compliance workflows depend on, and it is enforced explicitly in this pipeline.
![goldsoda](https://github.com/AtharvaGitProfile/financial-data-quality-pipeline/blob/main/Gold%20Soda%20Check.png)

### ✅ Silver + Quarantine validation PASS report 
These checks confirm that the pipeline’s defensive logic is working as intended.

They verify that:

> Validated transactions are complete
> Required business fields are always present
> Numeric values behave as numbers
> Quarantine is populated when invalid data exists
> Every quarantined record explains its failure

When these checks pass, it means the system is actively enforcing data contracts, not passively assuming them.

![Silver + Quarantine Validation Report](https://github.com/AtharvaGitProfile/financial-data-quality-pipeline/blob/main/Silver%20Quarantine%20Scan.png)

### ❌ Freshness FAIL report (intentional, to show alerting works)  

This check asserts that transaction data must be recent.

In real financial systems, freshness failures often indicate:
> Stalled ingestion
> Upstream outages
> Delayed processing masked by “successful” job runs

The failure shown in this project is intentional.
It demonstrates how the system surfaces stale data as a first-class issue, not an afterthought.

![FreshnessCheck](https://github.com/AtharvaGitProfile/financial-data-quality-pipeline/blob/main/Freshness%20fail.png)

---

