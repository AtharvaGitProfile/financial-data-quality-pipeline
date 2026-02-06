# AML Transaction Data Quality Pipeline

A production-style data platform that ingests streaming financial transactions, validates and quarantines bad data, produces trusted AML-ready metrics, and enforces data quality with observable PASS/FAIL checks.

This project demonstrates how **data reliability and trust** are built end-to-end.

---

## Why this project

In AML, risk, and compliance workflows:
- **Incorrect or stale data is worse than missing data**
- Teams must **prove data quality**, not assume it
- Failures must be **detectable, explainable, and auditable**

This pipeline shows how to:
- Prevent bad data from silently contaminating reports
- Isolate invalid records with clear failure reasons
- Continuously validate data health using automated checks

---

## Architecture Overview

**Streaming → Validation → Trusted Metrics → Quality Gates**


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


---

## Data Layers

### Bronze
- Raw transaction events from Kafka
- No transformations
- Used for traceability and replay

### Silver
- Schema validation and type checks
- Required fields enforced
- Invalid records routed to **Quarantine** with explicit `error_reason`

### Quarantine
- Preserves bad records for audit and debugging
- Ensures data issues are **visible, not silent**

### Gold
- Business-ready AML metrics:
  - Transaction count
  - Total and average amount
  - High-risk transaction count
- Used for reporting and downstream analytics

---

## Data Quality & Observability (Soda)

Quality checks are executed against Silver, Quarantine, and Gold using **Soda + DuckDB**.

### Examples
- Row count > 0
- No missing critical fields
- No negative or invalid amounts
- Quarantine must contain records when bad data exists
- **Freshness check (intentional FAIL)** to demonstrate alerting

All scans produce **auditable PASS / FAIL reports** saved locally.

---

## Evidence (What a reviewer can see)

- ✅ Gold data quality PASS report  
- ✅ Silver + Quarantine validation PASS report  
- ❌ Freshness FAIL report (intentional, to show alerting works)  
- 📊 Excel export of Gold AML metrics (screenshot-ready)

These artifacts demonstrate **trustworthiness**, not just computation.

---

## Tech Stack

- **Streaming**: Kafka
- **Processing**: Apache Spark (Structured Streaming + Batch)
- **Storage**: S3 (LocalStack)
- **Quality & Observability**: Soda
- **Analytics Engine**: DuckDB
- **Exports**: Pandas (CSV / Excel)
- **Infra**: Docker Compose

---

## How to Run (High Level)

```bash
docker compose up -d
./scripts/run_bronze.sh
./scripts/run_silver.sh
./scripts/run_gold.sh
./scripts/export_gold.sh
./scripts/soda_scan.sh
