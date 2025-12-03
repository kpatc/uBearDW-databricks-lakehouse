/**
 * Infrastructure Terraform pour uBear Data Warehouse sur GCP
 * Architecture: Cloud SQL + Pub/Sub + Cloud Run (Debezium)
 */

terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# =============================================================================
# Variables
# =============================================================================

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "europe-west1"  # Belgique - Proche du Maroc
}

variable "zone" {
  description = "GCP Zone"
  type        = string
  default     = "europe-west1-b"
}

variable "environment" {
  description = "Environment (dev, prod)"
  type        = string
  default     = "dev"
}

variable "db_user" {
  description = "PostgreSQL database user"
  type        = string
  default     = "foodapp"
}

variable "db_password" {
  description = "PostgreSQL database password"
  type        = string
  sensitive   = true
}

variable "db_name" {
  description = "PostgreSQL database name"
  type        = string
  default     = "foodapp"
}

# =============================================================================
# Cloud SQL PostgreSQL Instance
# =============================================================================

resource "google_sql_database_instance" "ubear_postgres" {
  name             = "ubear-postgres-${var.environment}"
  database_version = "POSTGRES_15"
  region           = var.region

  settings {
    # db-f1-micro = Free tier eligible (shared CPU, 614 MB RAM)
    tier = "db-f1-micro"

    # Disk configuration
    disk_type = "PD_HDD"
    disk_size = 10 # GB

    # Backup configuration (désactivé pour économiser - petit projet test)
    backup_configuration {
      enabled                        = false  # Désactivé pour économiser
      point_in_time_recovery_enabled = false
    }

    # IP configuration
    ip_configuration {
      ipv4_enabled    = true
      require_ssl     = false
      authorized_networks {
        name  = "allow-all-for-dev"
        value = "0.0.0.0/0"
      }
    }

    # Database flags pour Debezium CDC (minimisé pour petit projet)
    database_flags {
      name  = "cloudsql.logical_decoding"
      value = "on"
    }
    database_flags {
      name  = "max_replication_slots"
      value = "10"  # Minimum autorisé par GCP
    }
    database_flags {
      name  = "max_wal_senders"
      value = "10"  # Minimum autorisé par GCP
    }

    # Maintenance window
    maintenance_window {
      day          = 7 # Sunday
      hour         = 3
      update_track = "stable"
    }
  }

  deletion_protection = false
}

# Database
resource "google_sql_database" "ubear_db" {
  name     = var.db_name
  instance = google_sql_database_instance.ubear_postgres.name
}

# Database user
resource "google_sql_user" "ubear_user" {
  name     = var.db_user
  instance = google_sql_database_instance.ubear_postgres.name
  password = var.db_password
}

# =============================================================================
# Pub/Sub Topics pour CDC
# =============================================================================

resource "google_pubsub_topic" "eater_cdc" {
  name = "ubear-eater-cdc"

  message_retention_duration = "3600s" # 1 heure (au lieu de 1 jour) - économique
  
  labels = {
    environment = var.environment
    project     = "ubear-dw"
    source      = "debezium"
  }
}

resource "google_pubsub_topic" "merchant_cdc" {
  name = "ubear-merchant-cdc"

  message_retention_duration = "3600s" # 1 heure - économique
  
  labels = {
    environment = var.environment
    project     = "ubear-dw"
    source      = "debezium"
  }
}

resource "google_pubsub_topic" "courier_cdc" {
  name = "ubear-courier-cdc"

  message_retention_duration = "3600s" # 1 heure - économique
  
  labels = {
    environment = var.environment
    project     = "ubear-dw"
    source      = "debezium"
  }
}

resource "google_pubsub_topic" "trip_events_cdc" {
  name = "ubear-trip-events-cdc"

  message_retention_duration = "3600s" # 1 heure - économique
  
  labels = {
    environment = var.environment
    project     = "ubear-dw"
    source      = "debezium"
  }
}

resource "google_pubsub_topic" "schema_changes" {
  name = "ubear-schema-changes"

  message_retention_duration = "3600s" # 1 heure - économique
  
  labels = {
    environment = var.environment
    project     = "ubear-dw"
    source      = "debezium"
  }
}

# =============================================================================
# Pub/Sub Subscriptions pour Databricks
# =============================================================================

resource "google_pubsub_subscription" "eater_sub" {
  name  = "ubear-eater-sub"
  topic = google_pubsub_topic.eater_cdc.name

  ack_deadline_seconds = 60
  message_retention_duration = "3600s" # 1 heure - économique

  expiration_policy {
    ttl = "" # Never expire
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "300s" # Réduit de 600s
  }
}

resource "google_pubsub_subscription" "merchant_sub" {
  name  = "ubear-merchant-sub"
  topic = google_pubsub_topic.merchant_cdc.name

  ack_deadline_seconds = 60
  message_retention_duration = "3600s" # 1 heure - économique

  expiration_policy {
    ttl = ""
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "300s" # Réduit
  }
}

resource "google_pubsub_subscription" "courier_sub" {
  name  = "ubear-courier-sub"
  topic = google_pubsub_topic.courier_cdc.name

  ack_deadline_seconds = 60
  message_retention_duration = "3600s" # 1 heure - économique

  expiration_policy {
    ttl = ""
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "300s" # Réduit
  }
}

resource "google_pubsub_subscription" "trip_events_sub" {
  name  = "ubear-trip-events-sub"
  topic = google_pubsub_topic.trip_events_cdc.name

  ack_deadline_seconds = 60
  message_retention_duration = "3600s" # 1 heure - économique

  expiration_policy {
    ttl = ""
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "300s" # Réduit
  }
}

# =============================================================================
# Service Account pour Debezium
# =============================================================================

resource "google_service_account" "debezium_sa" {
  account_id   = "debezium-connector"
  display_name = "Debezium CDC Connector Service Account"
  description  = "Service account for Debezium to publish CDC events to Pub/Sub"
}

# IAM: Pub/Sub Publisher
resource "google_pubsub_topic_iam_member" "debezium_publisher" {
  for_each = toset([
    google_pubsub_topic.eater_cdc.name,
    google_pubsub_topic.merchant_cdc.name,
    google_pubsub_topic.courier_cdc.name,
    google_pubsub_topic.trip_events_cdc.name,
    google_pubsub_topic.schema_changes.name
  ])

  topic  = each.key
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.debezium_sa.email}"
}

# IAM: Cloud SQL Client
resource "google_project_iam_member" "debezium_sql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.debezium_sa.email}"
}

# =============================================================================
# Service Account pour Databricks
# =============================================================================

resource "google_service_account" "databricks_sa" {
  account_id   = "databricks-pubsub-reader"
  display_name = "Databricks Pub/Sub Reader"
  description  = "Service account for Databricks to read from Pub/Sub subscriptions"
}

# IAM: Pub/Sub Subscriber
resource "google_pubsub_subscription_iam_member" "databricks_subscriber" {
  for_each = toset([
    google_pubsub_subscription.eater_sub.name,
    google_pubsub_subscription.merchant_sub.name,
    google_pubsub_subscription.courier_sub.name,
    google_pubsub_subscription.trip_events_sub.name
  ])

  subscription = each.key
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.databricks_sa.email}"
}

# IAM: Pub/Sub Viewer (pour lister topics)
resource "google_project_iam_member" "databricks_viewer" {
  project = var.project_id
  role    = "roles/pubsub.viewer"
  member  = "serviceAccount:${google_service_account.databricks_sa.email}"
}

# =============================================================================
# Outputs
# =============================================================================

output "cloud_sql_connection_name" {
  description = "Cloud SQL connection name"
  value       = google_sql_database_instance.ubear_postgres.connection_name
}

output "cloud_sql_public_ip" {
  description = "Cloud SQL public IP address"
  value       = google_sql_database_instance.ubear_postgres.public_ip_address
}

output "pubsub_topics" {
  description = "Pub/Sub topics for CDC"
  value = {
    eater       = google_pubsub_topic.eater_cdc.name
    merchant    = google_pubsub_topic.merchant_cdc.name
    courier     = google_pubsub_topic.courier_cdc.name
    trip_events = google_pubsub_topic.trip_events_cdc.name
    schema      = google_pubsub_topic.schema_changes.name
  }
}

output "pubsub_subscriptions" {
  description = "Pub/Sub subscriptions for Databricks"
  value = {
    eater       = google_pubsub_subscription.eater_sub.name
    merchant    = google_pubsub_subscription.merchant_sub.name
    courier     = google_pubsub_subscription.courier_sub.name
    trip_events = google_pubsub_subscription.trip_events_sub.name
  }
}

output "debezium_service_account" {
  description = "Debezium service account email"
  value       = google_service_account.debezium_sa.email
}

output "databricks_service_account" {
  description = "Databricks service account email"
  value       = google_service_account.databricks_sa.email
}

output "connection_string" {
  description = "PostgreSQL connection string"
  value       = "postgresql://${var.db_user}:${var.db_password}@${google_sql_database_instance.ubear_postgres.public_ip_address}:5432/${var.db_name}"
  sensitive   = true
}
