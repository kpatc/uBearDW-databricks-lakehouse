-- Databricks Lakehouse - Création complète des tables Delta (bronze, dimensions SCD2, fact, date, time, location)

CREATE TABLE IF NOT EXISTS workspace.default.trip_events_bronze (
  event STRING,
  payload STRING,
  event_time TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.default.dim_eater (
  eater_id BIGINT,
  eater_uuid STRING,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone_number STRING,
  account_created_date DATE,
  loyalty_tier STRING,
  total_lifetime_orders INT,
  total_lifetime_spend DECIMAL(12,2),
  preferred_cuisine STRING,
  dietary_preferences STRING,
  is_eats_pass_member BOOLEAN,
  default_payment_method STRING,
  average_order_value DECIMAL(12,2),
  customer_segment STRING,
  effective_start_date TIMESTAMP,
  effective_end_date TIMESTAMP,
  is_current BOOLEAN,
  version_number INT
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.default.dim_merchant (
  merchant_id BIGINT,
  merchant_uuid STRING,
  merchant_name STRING,
  business_type STRING,
  cuisine_type STRING,
  cuisine_subtypes STRING,
  price_range STRING,
  average_preparation_minutes INT,
  overall_rating DECIMAL(4,2),
  total_ratings_count INT,
  is_partner_merchant BOOLEAN,
  commission_rate DECIMAL(5,2),
  merchant_onboarding_date DATE,
  accepts_cash BOOLEAN,
  menu_item_count INT,
  average_item_price DECIMAL(8,2),
  operating_hours STRING,
  is_currently_active BOOLEAN,
  merchant_tier STRING,
  city STRING,
  effective_start_date TIMESTAMP,
  effective_end_date TIMESTAMP,
  is_current BOOLEAN,
  version_number INT
) USING DELTA
PARTITIONED BY (city);

CREATE TABLE IF NOT EXISTS workspace.default.dim_courier (
  courier_id BIGINT,
  courier_uuid STRING,
  first_name STRING,
  last_name STRING,
  vehicle_type STRING,
  license_plate STRING,
  onboarding_date DATE,
  primary_delivery_zone STRING,
  overall_rating DECIMAL(4,2),
  total_deliveries_completed INT,
  acceptance_rate DECIMAL(5,2),
  cancellation_rate DECIMAL(5,2),
  average_delivery_time_minutes DECIMAL(5,2),
  on_time_delivery_rate DECIMAL(5,2),
  courier_tier STRING,
  is_active BOOLEAN,
  total_lifetime_earnings DECIMAL(12,2),
  preferred_delivery_hours STRING,
  has_insulated_bag BOOLEAN,
  background_check_date DATE,
  effective_start_date TIMESTAMP,
  effective_end_date TIMESTAMP,
  is_current BOOLEAN,
  version_number INT
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.default.dim_location (
  location_id BIGINT,
  address_line_1 STRING,
  address_line_2 STRING,
  city STRING,
  state_province STRING,
  postal_code STRING,
  country STRING,
  latitude DECIMAL(9,6),
  longitude DECIMAL(9,6),
  geohash STRING,
  h3_index STRING,
  neighborhood STRING,
  region_zone STRING,
  location_type STRING,
  is_high_rise BOOLEAN,
  has_doorman BOOLEAN,
  special_instructions STRING,
  timezone STRING
) USING DELTA
PARTITIONED BY (region_zone);

CREATE TABLE IF NOT EXISTS workspace.default.dim_date (
  date_key INT,
  full_date DATE,
  day_of_week INT,
  day_name STRING,
  is_weekend BOOLEAN,
  is_holiday BOOLEAN,
  holiday_name STRING,
  week_of_year INT,
  month_number INT,
  month_name STRING,
  quarter INT,
  year INT,
  fiscal_year INT,
  fiscal_quarter INT
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.default.dim_time (
  time_key INT,
  time_value STRING,
  hour_24 INT,
  hour_12 INT,
  am_pm STRING,
  minute INT,
  time_period STRING,
  is_peak_hour BOOLEAN
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.default.trip_fact (
  trip_id STRING,
  order_id STRING,
  eater_id BIGINT,
  merchant_id BIGINT,
  courier_id BIGINT,
  pickup_location_id BIGINT,
  dropoff_location_id BIGINT,
  order_placed_at TIMESTAMP,
  order_accepted_at TIMESTAMP,
  courier_dispatched_at TIMESTAMP,
  pickup_arrived_at TIMESTAMP,
  pickup_completed_at TIMESTAMP,
  dropoff_arrived_at TIMESTAMP,
  delivered_at TIMESTAMP,
  subtotal_amount DECIMAL(12,2),
  delivery_fee DECIMAL(12,2),
  service_fee DECIMAL(12,2),
  tax_amount DECIMAL(12,2),
  tip_amount DECIMAL(12,2),
  total_amount DECIMAL(12,2),
  courier_payout DECIMAL(12,2),
  distance_miles DECIMAL(8,3),
  preparation_time_minutes INT,
  delivery_time_minutes INT,
  total_time_minutes INT,
  trip_status STRING,
  cancellation_reason STRING,
  is_scheduled_order BOOLEAN,
  is_group_order BOOLEAN,
  promo_code_used STRING,
  discount_amount DECIMAL(12,2),
  eater_rating INT,
  courier_rating INT,
  merchant_rating INT,
  date_partition DATE,
  region_partition STRING,
  weather_condition STRING
) USING DELTA
PARTITIONED BY (date_partition, region_partition);
CREATE TABLE IF NOT EXISTS workspace.default.trip_events_bronze (
  event STRING,
  payload STRING,
  event_time TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.default.dim_location (
  location_id BIGINT,
  address_line_1 STRING,
  city STRING,
  country STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  geohash STRING,
  h3_index STRING,
  neighborhood STRING,
  region_zone STRING
) USING DELTA
PARTITIONED BY (region_zone);

CREATE TABLE IF NOT EXISTS workspace.default.dim_merchant (
  merchant_id BIGINT,
  merchant_name STRING,
  city STRING,
  cuisine_type STRING,
  average_preparation_minutes INT
) USING DELTA
PARTITIONED BY (city);

CREATE TABLE IF NOT EXISTS workspace.default.dim_eater (
  eater_id BIGINT,
  eater_uuid STRING,
  first_name STRING,
  last_name STRING,
  email STRING,
  account_created_date DATE
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.default.dim_courier (
  courier_id BIGINT,
  courier_uuid STRING,
  first_name STRING,
  last_name STRING,
  vehicle_type STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.default.dim_date (
  date_key INT, full_date DATE, day_of_week INT, month_number INT, year INT
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.default.dim_time (
  time_key INT, time_value STRING, hour_24 INT, minute INT
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.default.trip_fact (
  trip_id STRING,
  order_id STRING,
  eater_id BIGINT,
  merchant_id BIGINT,
  courier_id BIGINT,
  pickup_location_id BIGINT,
  dropoff_location_id BIGINT,
  order_placed_at TIMESTAMP,
  delivered_at TIMESTAMP,
  subtotal_amount DECIMAL(12,2),
  total_amount DECIMAL(12,2),
  distance_miles DOUBLE,
  delivery_time_minutes INT,
  trip_status STRING,
  date_partition DATE,
  region_partition STRING
) USING DELTA
PARTITIONED BY (date_partition, region_partition);
