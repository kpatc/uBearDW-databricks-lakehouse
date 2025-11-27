-- Exemple d'initialisation de la base OLTP Postgres pour le CDC
CREATE TABLE IF NOT EXISTS public.eater (
  eater_id BIGSERIAL PRIMARY KEY,
  eater_uuid VARCHAR(64) NOT NULL,
  first_name VARCHAR(64),
  last_name VARCHAR(64),
  email VARCHAR(128),
  phone_number VARCHAR(32),
  account_created_date DATE,
  loyalty_tier VARCHAR(32),
  total_lifetime_orders INT,
  total_lifetime_spend NUMERIC(12,2),
  preferred_cuisine VARCHAR(64),
  dietary_preferences VARCHAR(64),
  is_eats_pass_member BOOLEAN,
  default_payment_method VARCHAR(32),
  average_order_value NUMERIC(12,2),
  customer_segment VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS public.merchant (
  merchant_id BIGSERIAL PRIMARY KEY,
  merchant_uuid VARCHAR(64) NOT NULL,
  merchant_name VARCHAR(128),
  business_type VARCHAR(32),
  cuisine_type VARCHAR(64),
  cuisine_subtypes VARCHAR(64),
  price_range VARCHAR(8),
  average_preparation_minutes INT,
  overall_rating NUMERIC(4,2),
  total_ratings_count INT,
  is_partner_merchant BOOLEAN,
  commission_rate NUMERIC(5,2),
  merchant_onboarding_date DATE,
  accepts_cash BOOLEAN,
  menu_item_count INT,
  average_item_price NUMERIC(8,2),
  operating_hours VARCHAR(256),
  is_currently_active BOOLEAN,
  merchant_tier VARCHAR(32),
  city VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS public.courier (
  courier_id BIGSERIAL PRIMARY KEY,
  courier_uuid VARCHAR(64) NOT NULL,
  first_name VARCHAR(64),
  last_name VARCHAR(64),
  vehicle_type VARCHAR(32),
  license_plate VARCHAR(32),
  onboarding_date DATE,
  primary_delivery_zone VARCHAR(64),
  overall_rating NUMERIC(4,2),
  total_deliveries_completed INT,
  acceptance_rate NUMERIC(5,2),
  cancellation_rate NUMERIC(5,2),
  average_delivery_time_minutes NUMERIC(5,2),
  on_time_delivery_rate NUMERIC(5,2),
  courier_tier VARCHAR(32),
  is_active BOOLEAN,
  total_lifetime_earnings NUMERIC(12,2),
  preferred_delivery_hours VARCHAR(64),
  has_insulated_bag BOOLEAN,
  background_check_date DATE
);

-- Ajoute d'autres tables si besoin (location, trip, etc.)
