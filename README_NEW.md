# uBear Eats Data Warehouse
## Production-Ready Lakehouse for Food Delivery Analytics

[![Databricks](https://img.shields.io/badge/Platform-Databricks-FF3621?style=flat-square&logo=databricks)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta_Lake-00ADD8?style=flat-square&logo=apache-spark)](https://delta.io)
[![GCP](https://img.shields.io/badge/Cloud-Google_Cloud_Platform-4285F4?style=flat-square&logo=google-cloud)](https://cloud.google.com)
[![Debezium](https://img.shields.io/badge/CDC-Debezium-4EA94B?style=flat-square)](https://debezium.io)

---

## 📋 Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Data Model](#data-model)
- [Installation & Setup](#installation--setup)
- [Pipeline Execution](#pipeline-execution)
- [Monitoring & Troubleshooting](#monitoring--troubleshooting)
- [Project Statistics](#project-statistics)

---

## 🎯 Overview

**uBear Eats Data Warehouse** is a production-grade Lakehouse solution built on Databricks that implements a real-time change data capture (CDC) pipeline for a food delivery platform. The system ingests transactional data from Cloud SQL, processes it through a three-layer Medallion architecture (Bronze-Silver-Gold), and produces analytics-ready fact and dimension tables.

### Key Capabilities

| Capability | Implementation |
|-----------|----------------|
| **Real-time Ingestion** | Debezium CDC + Confluent Kafka streaming |
| **Data Processing** | Databricks Delta Live Tables (DLT) Serverless |
| **Medallion Architecture** | Bronze (raw) → Silver (clean) → Gold (analytics) |
| **Data Quality** | Delta constraints, expectations, and quality checks |
| **Dimensional Modeling** | SCD Type 2 dimensions with slowly changing attributes |
| **Geospatial Analytics** | Geohash and H3 spatial indexing for location clustering |
| **Fact Tables** | Comprehensive trip_fact with derived metrics |

---

## 🏗️ Architecture

### System Design

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      uBear Eats Data Warehouse                           │
└─────────────────────────────────────────────────────────────────────────┘

┌──────────────────┐     ┌───────────────────┐     ┌─────────────────────┐
│  Cloud SQL       │────▶│  Debezium Server  │────▶│  Confluent Kafka    │
│  PostgreSQL      │     │  (Cloud Run)      │     │  (Europe-West1)     │
│  - eater         │     │  - pgoutput       │     │  - ubear.public.*   │
│  - merchant      │     │  - slot repl.     │     │  - SASL_SSL auth    │
│  - courier       │     │  - CDC streaming  │     │  - 4 topics         │
│  - trip_events   │     │  - 2.5.4.Final    │     │  - Auto-replicated  │
└──────────────────┘     └───────────────────┘     └─────────────────────┘
                                                             │
                                                             ▼
                         ┌─────────────────────────────────────────────┐
                         │    Databricks Workspace (Lakehouse)         │
                         │  - Unity Catalog enabled                    │
                         │  - Serverless compute                       │
                         │  - Delta Live Tables (DLT)                  │
                         └─────────────────────────────────────────────┘
                                      │
                    ┌──────────────────┼──────────────────┐
                    ▼                  ▼                  ▼
         ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
         │   BRONZE LAYER   │ │  SILVER LAYER    │ │   GOLD LAYER     │
         │  (Raw Ingestion) │ │  (Transformed)   │ │   (Analytics)    │
         ├──────────────────┤ ├──────────────────┤ ├──────────────────┤
         │ • eater_bronze   │ │ • eater_silver   │ │ • dim_eater      │
         │ • merchant_*     │ │ • merchant_*     │ │ • dim_merchant   │
         │ • courier_*      │ │ • courier_*      │ │ • dim_courier    │
         │ • trip_events_*  │ │ • trip_events_*  │ │ • dim_location   │
         │                  │ │  (parsed JSONB)  │ │ • dim_date       │
         │ Stream Ingestion │ │  Clean, typed    │ │ • dim_time       │
         │ via Kafka        │ │  Data Quality ✓  │ │ • trip_fact      │
         │ 1-2 min latency  │ │ 2-3 min latency  │ │ • SCD2 History   │
         └──────────────────┘ └──────────────────┘ │ • Batch daily    │
                                                    └──────────────────┘
```

### Data Flow Timeline

```
Insert in Cloud SQL → WAL capture (pgoutput) → Debezium processes
     ↓                                                    ↓
  < 1 second            Kafka broker receives → DLT Bronze ingests
                             ↓                         ↓
                         (~2-5 sec)          Bronze table updated (1-2 min)
                                                    ↓
                                          Silver pipeline transforms (2-3 min)
                                                    ↓
                                          Gold batch daily (2 AM UTC)
                                                    ↓
                                       Analytics tables ready
```

---

## 💾 Technology Stack

### Cloud Infrastructure (GCP)

| Service | Configuration | Purpose |
|---------|---------------|---------|
| **Cloud SQL** | PostgreSQL 15, 35.189.224.202:5432 | Source OLTP database |
| **Cloud Run** | 2 CPU, 2Gi RAM, Cloud SQL Proxy | Debezium server container |
| **Compute Engine** | e2-small instance (1.7GB RAM) | Kafka broker host |
| **Cloud Storage** | GCS buckets for Kafka | Persistent storage |

### Data Pipeline (Confluent Cloud)

| Component | Version | Configuration |
|-----------|---------|---------------|
| **Kafka Cluster** | KRaft | lkc-zx156z, europe-west1.gcp.confluent.cloud |
| **Bootstrap Server** | - | pkc-z1o60.europe-west1.gcp.confluent.cloud:9092 |
| **Authentication** | SASL_SSL (PLAIN) | API Key: HYPO6LDVPLC2EAYE |
| **Topics** | 4 | ubear.public.{eater, merchant, courier, trip_events} |
| **Partitions** | 3 each | For parallelism and scalability |

### Data Warehouse (Databricks)

| Component | Edition | Configuration |
|-----------|---------|---------------|
| **Workspace** | Premium (Trial) | Core with Unity Catalog |
| **Catalog** | ubear_catalog | 3 schemas (bronze, silver, gold) |
| **Compute** | Serverless | Auto-scaling clusters |
| **Storage Format** | Delta Lake | ACID transactions, time travel |

### Data Processing Languages

```python
# Bronze Pipeline (Python + DLT)
- PySpark DataFrame API
- Debezium envelope parsing
- Kafka SASL_SSL authentication
- Z-order for clustering

# Silver Pipeline (Python + DLT)
- JSON payload parsing (from_json)
- Column extraction and casting
- Data quality constraints
- Null handling with coalesce

# Gold Pipeline (Python + Batch)
- SCD Type 2 implementation
- Window functions for metrics
- Geohash & H3 spatial indexing
- MERGE for upserts
```

---

## 📊 Data Model

### Source Tables (Cloud SQL)

#### eater
```sql
eater_id (PK) | eater_uuid | first_name | last_name | email | phone | 
address_line_1 | address_line_2 | city | state_province | postal_code | 
country | default_payment_method | is_active | created_at | updated_at
```

#### merchant
```sql
merchant_id (PK) | merchant_uuid | name | email | phone | 
business_type | cuisine_type | address_line_1 | address_line_2 | 
city | state_province | postal_code | country | 
operating_hours (JSONB) | is_active | created_at | updated_at
```

#### courier
```sql
courier_id (PK) | courier_uuid | first_name | last_name | email | phone | 
vehicle_type | license_plate | is_active | onboarding_date | 
created_at | updated_at
```

#### trip_events (Event Log - Core Table)
```sql
event_id (PK) | trip_id (FK) | order_id | 
eater_id (FK) | merchant_id (FK) | courier_id (FK) |
event_type | event_time | trip_status | 
payload (JSONB) | created_at

-- Indexed on: trip_id, order_id, eater_id, merchant_id, courier_id, 
--             event_type, event_time, trip_status
```

**payload JSONB contains:**
```json
{
  "subtotal_amount": 35.50,
  "delivery_fee": 3.50,
  "service_fee": 2.00,
  "tax_amount": 3.00,
  "tip_amount": 5.00,
  "total_amount": 44.00,
  "discount_amount": 0.00,
  "distance_miles": 2.5,
  "actual_prep_time_minutes": 16,
  "delivery_time_minutes": 17,
  "eater_rating": 5,
  "courier_rating": 5,
  "merchant_rating": 4,
  "weather_condition": "cloudy",
  "promo_code": "PROMO10",
  "is_group_order": false
}
```

### Medallion Architecture

#### Bronze Layer (ubear_bronze)
- **eater_bronze**: Raw CDC events from eater table
- **merchant_bronze**: Raw CDC events from merchant table
- **courier_bronze**: Raw CDC events from courier table
- **trip_events_bronze**: Raw CDC events (event log structure)

**Key Characteristics:**
- Contains full Debezium envelope (before, after, op, ts_ms, etc.)
- Streaming ingestion from Kafka
- CDC operation tracking (INSERT, UPDATE, DELETE)
- No transformations applied

#### Silver Layer (ubear_silver)
- **eater_silver**: Cleaned, deduplicated eater data
- **merchant_silver**: Normalized merchant data with validated emails
- **courier_silver**: Standardized courier information
- **trip_events_silver**: **EVENT LOG WITH EXTRACTED PAYLOAD**

**Key Characteristics:**
- Payload JSONB parsed into structured columns
- Financial metrics: subtotal_amount, delivery_fee, service_fee, tax_amount, tip_amount, total_amount, discount_amount
- Logistics metrics: distance_miles, preparation_time_minutes, delivery_time_minutes
- Quality ratings: eater_rating, courier_rating, merchant_rating
- Context: weather_condition, promo_code_used, is_group_order
- Data quality expectations enforced
- Ready for analytics consumption

#### Gold Layer (ubear_gold)

**Dimensions (SCD Type 2):**
- **dim_eater**: Customer profiles with lifetime metrics
  - Columns: eater_id, eater_uuid, name, email, address, loyalty_tier, customer_segment, effective_start_date, effective_end_date, is_current, version_number
  - Metrics: total_lifetime_orders, total_lifetime_spend, avg_rating_given

- **dim_merchant**: Restaurant profiles with performance metrics
  - Columns: merchant_id, merchant_uuid, name, cuisine_type, address, price_range, merchant_tier, commission_rate
  - Metrics: overall_rating, total_ratings_count, average_preparation_minutes, total_orders_completed

- **dim_courier**: Delivery partner profiles with performance tracking
  - Columns: courier_id, courier_uuid, name, vehicle_type, license_plate, courier_tier
  - Metrics: total_deliveries_completed, overall_rating, average_delivery_time, on_time_delivery_rate, total_lifetime_earnings

**Dimensions (Static):**
- **dim_location**: Geographically enriched locations
  - Includes: location_id, address, city, state, postal_code, latitude, longitude, geohash, h3_index, neighborhood, region_zone, timezone
  - Partitioned by region_zone for query optimization

- **dim_date**: Calendar dimension
  - 4,018 days covering 10+ years
  - Includes: full_date, date_key, day, month, quarter, year, day_of_week, week_of_year, is_weekend, is_holiday

- **dim_time**: Time-of-day dimension
  - 1,440 records (1 minute granularity for 24 hours)
  - Includes: time_key, time_value, hour, minute, time_period

**Fact Tables:**
- **trip_fact**: Central fact table for trip analytics
  - Grain: One row per trip
  - Dimensions: trip_id, order_id, eater_id, merchant_id, courier_id, pickup_location_id, dropoff_location_id
  - Timestamps: order_placed_at, order_accepted_at, courier_dispatched_at, pickup_arrived_at, pickup_completed_at, dropoff_arrived_at, delivered_at, cancelled_at
  - Financial: subtotal_amount, delivery_fee, service_fee, tax_amount, tip_amount, total_amount, courier_payout
  - Metrics: distance_miles, preparation_time_minutes, delivery_time_minutes, total_time_minutes, trip_status, is_group_order, promo_code_used, discount_amount
  - Quality: eater_rating, courier_rating, merchant_rating, weather_condition
  - Z-ordered on eater_id, merchant_id, courier_id, order_placed_at

---

## 🚀 Installation & Setup

### Prerequisites

```bash
# Required software
- Python 3.10+
- Databricks CLI
- Google Cloud SDK (gcloud)
- Git

# Python packages (see requirements.txt)
- pyspark >= 3.5.0
- delta-spark >= 3.0.0
- geohash2 == 1.1
- h3 == 3.7.6
```

### Step 1: Clone Repository

```bash
git clone https://github.com/yourusername/uBearDW-databricks-lakehouse.git
cd uBearDW-databricks-lakehouse
```

### Step 2: GCP Setup

```bash
# Authenticate with GCP
gcloud auth login
gcloud config set project gentle-voltage-478517-q0

# Create Cloud SQL instance (if not exists)
gcloud sql instances create ubear-postgres-dev \
  --database-version POSTGRES_15 \
  --region europe-west1

# Initialize database schema
gcloud sql connect ubear-postgres-dev \
  --user foodapp < gcp_infrastructure/init_cloud_sql.sql
```

### Step 3: Debezium Deployment

```bash
# Build Debezium image
cd gcp_infrastructure/debezium-server
docker build -f Dockerfile -t debezium-server-ubear:latest .

# Tag for GCP
docker tag debezium-server-ubear:latest \
  gcr.io/gentle-voltage-478517-q0/debezium-server-ubear:latest

# Push to Container Registry
docker push gcr.io/gentle-voltage-478517-q0/debezium-server-ubear:latest

# Deploy to Cloud Run
gcloud run deploy debezium-server-ubear \
  --image gcr.io/gentle-voltage-478517-q0/debezium-server-ubear:latest \
  --region europe-west1 \
  --platform managed \
  --memory 2Gi \
  --cpu 2 \
  --allow-unauthenticated
```

### Step 4: Databricks Workspace Setup

```bash
# Create catalog
databricks catalogs create --name ubear_catalog --comment "uBear DW Catalog"

# Create schemas
databricks schemas create --catalog-name ubear_catalog \
  --name ubear_bronze --comment "Bronze layer"
databricks schemas create --catalog-name ubear_catalog \
  --name ubear_silver --comment "Silver layer"
databricks schemas create --catalog-name ubear_catalog \
  --name ubear_gold --comment "Gold layer"

# Upload pipeline notebooks
databricks workspace mkdirs /Repos/uBearDW-databricks-lakehouse/pipelines
databricks workspace import --language PYTHON \
  pipelines/bronze_pipeline.py \
  /Repos/uBearDW-databricks-lakehouse/pipelines/bronze_pipeline
# Repeat for silver and gold pipelines
```

### Step 5: Create DLT Pipelines

```bash
# Bronze pipeline
databricks pipelines create --config jobs/bronze_pipeline_config.json

# Silver pipeline
databricks pipelines create --config jobs/silver_pipeline_config.json

# Gold job (batch)
databricks jobs create --json-file jobs/gold_pipeline_config.json
```

---

## ▶️ Pipeline Execution

### Starting the Pipelines

```bash
# Start Bronze DLT pipeline (streaming)
databricks pipelines start-update \
  --pipeline-id <bronze-pipeline-id>

# Start Silver DLT pipeline (streaming)
databricks pipelines start-update \
  --pipeline-id <silver-pipeline-id>

# Trigger Gold batch job
databricks jobs run-now \
  --job-id <gold-job-id>

# Schedule Gold job (Daily 2:00 AM UTC)
# Already configured in jobs/gold_pipeline_config.json
```

### Verifying Data Flow

```sql
-- Check Bronze ingestion
SELECT COUNT(*) as record_count, MAX(kafka_timestamp) as latest_update
FROM ubear_catalog.ubear_bronze.trip_events_bronze;

-- Check Silver transformation
SELECT COUNT(*) as record_count, MAX(silver_load_time) as latest_update
FROM ubear_catalog.ubear_silver.trip_events_silver;

-- Check Gold fact table
SELECT COUNT(*) as trip_count, 
       COUNT(DISTINCT trip_id) as unique_trips,
       MAX(updated_at) as latest_update
FROM ubear_catalog.ubear_gold.trip_fact;

-- Verify dimension tables
SELECT 'eater' as dimension, COUNT(*) as row_count FROM ubear_catalog.ubear_gold.dim_eater
UNION ALL
SELECT 'merchant', COUNT(*) FROM ubear_catalog.ubear_gold.dim_merchant
UNION ALL
SELECT 'courier', COUNT(*) FROM ubear_catalog.ubear_gold.dim_courier
UNION ALL
SELECT 'location', COUNT(*) FROM ubear_catalog.ubear_gold.dim_location;
```

---

## 🔍 Monitoring & Troubleshooting

### CDC Health Checks

```bash
# Verify replication slot
gcloud sql connect ubear-postgres-dev --user foodapp -c \
  "SELECT slot_name, slot_type, restart_lsn FROM pg_replication_slots;"

# Check publication
gcloud sql connect ubear-postgres-dev --user foodapp -c \
  "SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_publication_ubear';"

# View Debezium logs
gcloud run logs read debezium-server-ubear --region europe-west1 --limit 50
```

### Kafka Monitoring

```bash
# List topics
kafka-topics --bootstrap-server pkc-z1o60.europe-west1.gcp.confluent.cloud:9092 \
  --command-config client.properties --list

# Check message count
kafka-run-class kafka.tools.JmxTool \
  --object-name kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec

# Monitor topic lag
kafka-consumer-groups --bootstrap-server pkc-z1o60.europe-west1.gcp.confluent.cloud:9092 \
  --command-config client.properties --group ubear-bronze --describe
```

### Databricks Pipeline Monitoring

```python
# In Databricks notebook - Check pipeline status
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
pipelines = w.pipelines.list_pipelines()
for p in pipelines:
    print(f"{p.name}: {p.state}")
    
# Get recent runs
runs = w.pipelines.list_updates(pipeline_id="<pipeline-id>", max_results=10)
for run in runs:
    print(f"Run {run.update_id}: {run.state}")
```

### Common Issues & Solutions

| Issue | Cause | Solution |
|-------|-------|----------|
| Debezium not connecting | Cloud SQL not accessible | Check Cloud SQL Network → Auth networks, enable Cloud SQL Proxy |
| No messages in Kafka | Debezium crashed or replication slot issue | Restart Debezium: `gcloud run deploy` with same config |
| Bronze pipeline failing | Kafka authentication failed | Verify API Key and SASL configuration in `application.properties` |
| Silver payload parsing error | Missing fields in payload JSONB | Ensure all 18 sample records were inserted into Cloud SQL |
| Gold SCD2 error | Dimension table doesn't exist | First run should create table, subsequent runs do merge. Check logs. |
| Timestamp NULL in fact table | Event type mismatch | Verify event_type values match: 'order_placed', 'delivered', etc. |

---

## 📈 Project Statistics

### Code Metrics
- **Python Code**: ~3,500 lines (Bronze + Silver + Gold pipelines)
- **SQL Scripts**: ~400 lines (Cloud SQL initialization + queries)
- **Configuration**: ~500 lines (Job configs, properties files)
- **Test Data**: 3 complete trips with 18 sample events

### Data Volume (Sample)
| Table | Records | Update Frequency |
|-------|---------|------------------|
| eater | 5 | On demand |
| merchant | 5 | On demand |
| courier | 4 | On demand |
| trip_events | 18 (initial sample) | Real-time |
| trip_fact | 3 (from sample) | Daily batch |
| dim_* | ~7,500 total | Daily/On demand |

### Performance Targets
| Pipeline | Latency | Throughput | Status |
|----------|---------|-----------|--------|
| CDC → Kafka | < 5 seconds | Unlimited | ✅ Verified |
| Kafka → Bronze | 1-2 minutes | Streaming | ✅ Running |
| Bronze → Silver | 2-3 minutes | Streaming | ✅ Running |
| Silver → Gold | Batch daily | End-of-day | ✅ Configured |

### Infrastructure Costs (Monthly Estimate)
- Cloud SQL: ~$30 (db-f1-micro)
- Cloud Run: ~$5 (Debezium server, minimal traffic)
- Confluent Kafka: ~$80 (Starter cluster)
- Databricks: ~$200 (Serverless DBU usage)
- **Total**: ~$315/month

---

## 📚 Additional Resources

### Documentation
- [Databricks Delta Live Tables](https://docs.databricks.com/en/delta-live-tables/)
- [Debezium PostgreSQL Connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Delta Lake Architecture](https://docs.delta.io/latest/index.html)
- [SCD Type 2 Implementation](https://docs.databricks.com/en/lakehouse-architecture/medallion-architecture.html)

### Key Files

```
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
├── gcp_infrastructure/
│   ├── init_cloud_sql.sql            # Database initialization
│   ├── main.tf                       # Terraform infrastructure
│   └── debezium-server/
│       ├── application.properties     # Debezium configuration
│       └── Dockerfile                # Container image
├── jobs/
│   ├── bronze_pipeline_config.json   # DLT Bronze config
│   ├── silver_pipeline_config.json   # DLT Silver config
│   └── gold_pipeline_config.json     # Batch Gold config
└── pipelines/
    ├── bronze_pipeline.py            # CDC ingestion
    ├── silver_pipeline.py            # Data transformation
    └── gold_pipeline.py              # Analytics enrichment
```

---

## 👤 Author
- **Josh** - Data Engineer & Architect



## 🙏 Acknowledgments
- Databricks for Lakehouse platform
- Debezium for reliable CDC
- Google Cloud Platform for infrastructure

---

**Last Updated**: December 6, 2025  
**Status**: ✅ Production Ready (Sample Data)
