# uBear Eats Data Warehouse

[![Platform](https://img.shields.io/badge/Platform-Databricks-FF3621?logo=databricks)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta_Lake-00ADD8)](https://delta.io)
[![GCP](https://img.shields.io/badge/Cloud-Google_Cloud-4285F4?logo=google-cloud)](https://cloud.google.com)
[![CDC](https://img.shields.io/badge/CDC-Debezium-4EA94B)](https://debezium.io)

Modern data warehouse for uBear Eats food delivery platform, built on Databricks Lakehouse with Medallion architecture (Bronze-Silver-Gold) and real-time CDC streaming.

## 🎯 Overview

This project implements an end-to-end data warehouse solution that centralizes and transforms transactional data from uBear Eats for analytics and reporting. It covers the complete order journey from customer placement to courier delivery.

**Key Features:**
- 🔄 Real-time CDC ingestion from PostgreSQL via Debezium
- ☁️ Cloud-native architecture on Google Cloud Platform
- 🏗️ Medallion architecture (Bronze → Silver → Gold)
- 📊 Delta Live Tables for automated data quality
- 🌍 Geospatial enrichment with Geohash and H3
- 📈 SCD Type 2 for dimension tracking

### Data Sources

All source data originates from **Google Cloud SQL PostgreSQL** and flows through a fully managed CDC pipeline:

| Component | Technology | Description |
|-----------|-----------|-------------|
| **Transactional DB** | Cloud SQL PostgreSQL 15 | Source database with 4 tables (eater, merchant, courier, trip_events) |
| **CDC Capture** | Debezium Server 2.5 | Deployed on Cloud Run, captures changes via pgoutput plugin |
| **Event Streaming** | Google Pub/Sub | 5 topics with subscriptions for reliable message delivery |
| **Data Processing** | Databricks DLT | Bronze → Silver → Gold transformations |

**CDC Flow:**
```
Cloud SQL WAL → Debezium (pgoutput) → Pub/Sub Topics → DLT Streaming → Delta Tables
```

## 📐 Architecture

### Data Flow
```
Cloud SQL PostgreSQL → Debezium Server → Pub/Sub → Databricks DLT
                                                        ↓
                                    Bronze (Raw CDC) → Silver (Cleaned) → Gold (Analytics)
```

### Technology Stack
| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Source** | Cloud SQL PostgreSQL | Transactional database |
| **CDC** | Debezium Server 2.5 | Change data capture |
| **Messaging** | Google Pub/Sub | Event streaming |
| **Processing** | Databricks (Delta Live Tables) | ETL pipelines |
| **Storage** | Delta Lake | ACID-compliant data lake |
| **Orchestration** | Databricks Workflows | Job scheduling |
| **IaC** | Terraform | Infrastructure automation |

## 📁 Project Structure

```
├── pipelines/                    # DLT pipeline definitions
│   ├── bronze_pipeline.py        # Raw CDC ingestion from Pub/Sub
│   ├── silver_pipeline.py        # Data cleaning & validation
│   └── gold_pipeline.py          # Business aggregations & dimensions
├── gcp_infrastructure/           # Google Cloud setup
│   ├── main.tf                   # Terraform infrastructure
│   ├── init_cloud_sql.sql        # Database initialization
│   ├── debezium-server/          # CDC configuration
│   │   ├── application.properties
│   │   └── Dockerfile
│   └── deploy_gcp.sh             # Automated deployment
├── utils/                        # Helper modules
│   ├── transformations.py        # Reusable transformations
│   └── optimize_tables.py        # Performance optimization
├── expectations/                 # Data quality rules
│   ├── data_quality.py           # DLT expectations
│   └── data_quality_validation.py # Validation scripts
├── jobs/                         # Pipeline configurations
│   ├── bronze_pipeline_config.json
│   ├── silver_pipeline_config.json
│   └── gold_pipeline_config.json
└── local_stack/                  # Local development
    └── docker-compose.yml        # PostgreSQL + Kafka + Debezium
```

## 🗂️ Data Model

### Bronze Layer (Raw CDC)
Streams raw change events from Pub/Sub subscriptions:
- `eater_bronze` - Customer data
- `merchant_bronze` - Restaurant/merchant data
- `courier_bronze` - Delivery driver data
- `trip_events_bronze` - Order lifecycle events

### Silver Layer (Cleaned & Typed)
Deduplicated and validated data with proper typing:
- `eater_silver` - Deduplicated customers
- `merchant_silver` - Active merchants
- `courier_silver` - Active couriers
- `trip_events_silver` - Valid trip events

### Gold Layer (Analytics-Ready)
Star schema with dimensions and fact tables:

**Dimensions:**
- `dim_eater` - Customer dimension (SCD Type 2)
- `dim_merchant` - Merchant dimension (SCD Type 2)
- `dim_courier` - Courier dimension (SCD Type 2)
- `dim_location` - Geocoded locations (Geohash + H3)
- `dim_date` - Date dimension
- `dim_time` - Time dimension

**Fact:**
- `trip_fact` - Trip metrics with lifecycle aggregations

## 🚀 Quick Start

### Prerequisites
- GCP account with billing enabled
- Databricks workspace (free trial supported)
- Terraform >= 1.12
- gcloud CLI

### 1. Deploy Infrastructure

```bash
cd gcp_infrastructure

# Configure GCP credentials
gcloud auth login
gcloud auth application-default login

# Set variables
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your project details

# Deploy
./deploy_gcp.sh
```

This creates:
- Cloud SQL PostgreSQL instance
- 5 Pub/Sub topics with subscriptions
- Service accounts with IAM permissions
- Debezium Server on Cloud Run

### 2. Configure Databricks

#### Upload Bronze Pipeline
1. Go to Databricks Workspace → Import
2. Upload `pipelines/bronze_pipeline.py`

#### Create DLT Pipeline
```
Name: ubear_bronze_ingestion
Notebook: /Workspace/Users/<email>/bronze_pipeline
Target: ubear_bronze

Configuration:
  gcp.project.id = <your-project-id>
  gcp.credentials.json = <service-account-json>

Spark Config:
  spark.jars.packages = io.github.googleapis:pubsub-spark-sql-streaming_2.12:1.1.0
```

#### Get Service Account Credentials
```bash
cd gcp_infrastructure
./show_credentials_for_databricks.sh
```

### 3. Deploy Pipelines

Create DLT pipelines for Silver and Gold layers using configurations in `jobs/` directory.

### 4. Verify Data Flow

```sql
-- Check Bronze ingestion
SELECT COUNT(*) FROM ubear_bronze.eater_bronze;

-- Check Silver processing  
SELECT COUNT(*) FROM ubear_silver.eater_silver;

-- Check Gold analytics
SELECT * FROM ubear_gold.trip_fact LIMIT 10;
```

## 🧪 Local Development

For local testing with Docker (development environment):

```bash
cd local_stack

# Start PostgreSQL + Kafka + Debezium
docker-compose up -d

# Initialize database
docker exec -i postgres psql -U foodapp -d foodapp < init_tables.sql

# Generate test data
./generate_data.sh

# Register Debezium connector
./register_connector.sh
```

**⚠️ Note:** The local stack uses Kafka for CDC streaming. To connect with Databricks for testing:

**Option 1: Local Spark Processing**
- Build the data warehouse in local PostgreSQL
- Run pipelines using local Spark installation
- Useful for development and unit testing

**Option 2: Databricks Connection via Tunnel**
- Use ngrok or similar to expose local Kafka: `ngrok tcp 29092`
- Update Databricks pipeline config with public endpoint
- Allows testing full Databricks DLT pipelines locally


## 📊 Data Quality

The project implements comprehensive data quality checks:

### DLT Expectations
- Non-null primary keys
- Valid email formats
- Coordinate range validations
- Rating bounds (0-5)
- Referential integrity

### Quality Validation
```bash
# Run validation suite
python expectations/data_quality_validation.py \
  --catalog ubear_catalog \
  --schema ubear_gold
```

## ⚡ Performance Optimization

```bash
# Optimize Gold tables with Z-ordering
python utils/optimize_tables.py \
  --catalog ubear_catalog \
  --schema ubear_gold
```

This runs:
- OPTIMIZE for compaction
- ZORDER BY on frequently filtered columns
- ANALYZE TABLE for statistics

## 🔒 Security

- Service accounts with least-privilege IAM roles
- Credentials managed via Databricks secrets
- Cloud SQL with private IP (optional)
- Pub/Sub message encryption at rest

## 📈 Monitoring

Key metrics to track:
- **Pipeline Health:** DLT pipeline status and failures
- **Data Freshness:** Lag between CDC and Gold tables
- **Data Quality:** Expectation violations
- **Performance:** Pipeline execution time, Pub/Sub lag

## 🤝 Contributing

This is a portfolio/demo project. For production use:
1. Add proper CI/CD pipelines
2. Implement comprehensive testing
3. Add monitoring and alerting
4. Review security configurations
5. Implement disaster recovery

## 📝 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file.

## 🙏 Acknowledgments

- [Databricks](https://databricks.com) for Lakehouse platform
- [Debezium](https://debezium.io) for CDC framework
- [Delta Lake](https://delta.io) for reliable storage

---