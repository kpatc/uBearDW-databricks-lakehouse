-- Script d'initialisation Cloud SQL PostgreSQL
-- Tables pour uBear Data Warehouse avec support CDC Debezium

-- =============================================================================
-- Enable logical replication (déjà fait via database flags)
-- =============================================================================
-- cloudsql.logical_decoding = on
-- max_replication_slots = 5
-- max_wal_senders = 5

-- =============================================================================
-- Create tables
-- =============================================================================

-- Table eater
CREATE TABLE IF NOT EXISTS eater (
    eater_id SERIAL PRIMARY KEY,
    eater_uuid VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone_number VARCHAR(20),
    address_line_1 VARCHAR(255),
    address_line_2 VARCHAR(255),
    city VARCHAR(100),
    state_province VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    default_payment_method VARCHAR(50),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table merchant
CREATE TABLE IF NOT EXISTS merchant (
    merchant_id SERIAL PRIMARY KEY,
    merchant_uuid VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone_number VARCHAR(20),
    business_type VARCHAR(100),
    cuisine_type VARCHAR(100),
    address_line_1 VARCHAR(255),
    address_line_2 VARCHAR(255),
    city VARCHAR(100),
    state_province VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    operating_hours JSONB,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table courier
CREATE TABLE IF NOT EXISTS courier (
    courier_id SERIAL PRIMARY KEY,
    courier_uuid VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone_number VARCHAR(20),
    vehicle_type VARCHAR(50),
    license_plate VARCHAR(20),
    is_active BOOLEAN DEFAULT true,
    onboarding_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table trip_events (Transactional event log - raw events from app)
CREATE TABLE IF NOT EXISTS trip_events (
    -- Primary & Foreign Keys
    event_id SERIAL PRIMARY KEY,
    trip_id VARCHAR(50) NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    eater_id INTEGER NOT NULL REFERENCES eater(eater_id),
    merchant_id INTEGER NOT NULL REFERENCES merchant(merchant_id),
    courier_id INTEGER REFERENCES courier(courier_id),
    
    -- Event Info (captured by app at each lifecycle stage)
    event_type VARCHAR(50) NOT NULL, -- order_placed, order_accepted, courier_dispatched, pickup_arrived, pickup_completed, dropoff_arrived, delivered, cancelled
    event_time TIMESTAMP NOT NULL,
    
    -- Order Status (updated by app)
    trip_status VARCHAR(50) DEFAULT 'pending', -- pending, accepted, dispatched, picked_up, in_delivery, completed, cancelled
    
    -- Event Payload (JSONB - contains all contextual data for this event)
    payload JSONB,
    
    -- CDC & Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- Create indexes
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_eater_email ON eater(email);
CREATE INDEX IF NOT EXISTS idx_eater_uuid ON eater(eater_uuid);
CREATE INDEX IF NOT EXISTS idx_eater_active ON eater(is_active);

CREATE INDEX IF NOT EXISTS idx_merchant_email ON merchant(email);
CREATE INDEX IF NOT EXISTS idx_merchant_uuid ON merchant(merchant_uuid);
CREATE INDEX IF NOT EXISTS idx_merchant_active ON merchant(is_active);

CREATE INDEX IF NOT EXISTS idx_courier_email ON courier(email);
CREATE INDEX IF NOT EXISTS idx_courier_uuid ON courier(courier_uuid);
CREATE INDEX IF NOT EXISTS idx_courier_active ON courier(is_active);

CREATE INDEX IF NOT EXISTS idx_trip_events_trip_id ON trip_events(trip_id);
CREATE INDEX IF NOT EXISTS idx_trip_events_order_id ON trip_events(order_id);
CREATE INDEX IF NOT EXISTS idx_trip_events_eater_id ON trip_events(eater_id);
CREATE INDEX IF NOT EXISTS idx_trip_events_merchant_id ON trip_events(merchant_id);
CREATE INDEX IF NOT EXISTS idx_trip_events_courier_id ON trip_events(courier_id);
CREATE INDEX IF NOT EXISTS idx_trip_events_status ON trip_events(trip_status);
CREATE INDEX IF NOT EXISTS idx_trip_events_type ON trip_events(event_type);
CREATE INDEX IF NOT EXISTS idx_trip_events_time ON trip_events(event_time);

-- =============================================================================
-- Create Debezium publication (pour pgoutput plugin)
-- =============================================================================

-- Drop if exists
DROP PUBLICATION IF EXISTS dbz_publication_ubear;

-- Create publication for all 4 tables
CREATE PUBLICATION dbz_publication_ubear FOR TABLE 
    eater,
    merchant,
    courier,
    trip_events;

-- =============================================================================
-- Insert sample data
-- =============================================================================

-- Eaters
INSERT INTO eater (eater_uuid, first_name, last_name, email, phone_number, address_line_1, address_line_2, city, state_province, postal_code, country, default_payment_method, is_active)
VALUES
    ('eater-uuid-001', 'Alice', 'Dupont', 'alice.dupont@email.com', '+33612345678', '10 Rue de Rivoli', 'Appartement 5B', 'Paris', 'Ile-de-France', '75001', 'France', 'credit_card', true),
    ('eater-uuid-002', 'Bob', 'Martin', 'bob.martin@email.com', '+33623456789', '45 Avenue Montaigne', NULL, 'Paris', 'Ile-de-France', '75008', 'France', 'paypal', true),
    ('eater-uuid-003', 'Claire', 'Dubois', 'claire.dubois@email.com', '+33634567890', '78 Boulevard Haussmann', 'Étage 3', 'Paris', 'Ile-de-France', '75009', 'France', 'apple_pay', true),
    ('eater-uuid-004', 'David', 'Leroy', 'david.leroy@email.com', '+33645678901', '23 Rue de Lyon', NULL, 'Lyon', 'Auvergne-Rhône-Alpes', '69002', 'France', 'credit_card', true),
    ('eater-uuid-005', 'Emma', 'Petit', 'emma.petit@email.com', '+33656789012', '15 Cours Julien', 'Apt 12', 'Marseille', 'Provence-Alpes-Côte d''Azur', '13006', 'France', 'google_pay', true)
ON CONFLICT (eater_uuid) DO NOTHING;

-- Merchants
INSERT INTO merchant (merchant_uuid, name, email, phone_number, business_type, cuisine_type, address_line_1, address_line_2, city, state_province, postal_code, country, operating_hours, is_active)
VALUES
    ('merchant-uuid-001', 'Le Bistrot Parisien', 'contact@bistrot-parisien.fr', '+33142345678', 'restaurant', 'french', '5 Rue Saint-Honoré', NULL, 'Paris', 'Ile-de-France', '75001', 'France', '{"monday": "11:00-22:00", "tuesday": "11:00-22:00"}'::jsonb, true),
    ('merchant-uuid-002', 'Sushi Tokyo', 'info@sushitokyo.fr', '+33143456789', 'restaurant', 'japanese', '12 Avenue de l''Opéra', 'Local 2', 'Paris', 'Ile-de-France', '75002', 'France', '{"monday": "12:00-23:00", "tuesday": "12:00-23:00"}'::jsonb, true),
    ('merchant-uuid-003', 'Pizza Napoli', 'hello@pizzanapoli.fr', '+33144567890', 'restaurant', 'italian', '34 Rue de Belleville', NULL, 'Paris', 'Ile-de-France', '75020', 'France', '{"monday": "11:30-22:30", "tuesday": "11:30-22:30"}'::jsonb, true),
    ('merchant-uuid-004', 'Bouchon Lyonnais', 'contact@bouchon-lyon.fr', '+33478901234', 'restaurant', 'french', '8 Rue des Marronniers', NULL, 'Lyon', 'Auvergne-Rhône-Alpes', '69002', 'France', '{"monday": "12:00-21:00", "tuesday": "12:00-21:00"}'::jsonb, true),
    ('merchant-uuid-005', 'Bouillabaisse Express', 'info@bouillabaisse-express.fr', '+33491234567', 'restaurant', 'mediterranean', '20 Quai du Port', 'Zone 1', 'Marseille', 'Provence-Alpes-Côte d''Azur', '13002', 'France', '{"monday": "11:00-22:00", "tuesday": "11:00-22:00"}'::jsonb, true)
ON CONFLICT (merchant_uuid) DO NOTHING;

-- Couriers
INSERT INTO courier (courier_uuid, first_name, last_name, email, phone_number, vehicle_type, license_plate, is_active, onboarding_date)
VALUES
    ('courier-uuid-001', 'François', 'Moreau', 'francois.moreau@ubear.com', '+33667890123', 'bicycle', 'N/A', true, '2024-01-15'),
    ('courier-uuid-002', 'Sophie', 'Blanc', 'sophie.blanc@ubear.com', '+33678901234', 'scooter', 'AB-123-CD', true, '2024-02-01'),
    ('courier-uuid-003', 'Lucas', 'Roux', 'lucas.roux@ubear.com', '+33689012345', 'motorcycle', 'EF-456-GH', true, '2024-03-10'),
    ('courier-uuid-004', 'Marie', 'Simon', 'marie.simon@ubear.com', '+33690123456', 'car', 'IJ-789-KL', true, '2024-04-20')
ON CONFLICT (courier_uuid) DO NOTHING;

-- Trip events (raw transactional events - lifecycle tracking)
INSERT INTO trip_events (trip_id, order_id, eater_id, merchant_id, courier_id, event_type, event_time, trip_status, payload)
VALUES
    -- Trip 1: Complete delivery lifecycle
    ('trip-001', 'order-001', 1, 1, NULL, 'order_placed', '2024-12-01 12:00:00', 'pending', 
     '{"subtotal_amount": 35.50, "delivery_fee": 3.50, "service_fee": 2.00, "tax_amount": 3.00, "total_amount": 44.00, "items": [{"name": "Steak Frites", "quantity": 1, "price": 22.00}, {"name": "Salade", "quantity": 1, "price": 8.50}], "payment_method": "credit_card", "pickup_address": "5 Rue Saint-Honoré, Paris", "dropoff_address": "10 Rue de Rivoli, Paris"}'::jsonb),
    
    ('trip-001', 'order-001', 1, 1, NULL, 'order_accepted', '2024-12-01 12:02:00', 'accepted',
     '{"estimated_prep_time_minutes": 15}'::jsonb),
    
    ('trip-001', 'order-001', 1, 1, 1, 'courier_dispatched', '2024-12-01 12:10:00', 'dispatched',
     '{"courier_distance_km": 2.5}'::jsonb),
    
    ('trip-001', 'order-001', 1, 1, 1, 'pickup_arrived', '2024-12-01 12:15:00', 'picked_up',
     '{"arrival_time": "2024-12-01T12:15:00Z"}'::jsonb),
    
    ('trip-001', 'order-001', 1, 1, 1, 'pickup_completed', '2024-12-01 12:18:00', 'picked_up',
     '{"actual_prep_time_minutes": 16}'::jsonb),
    
    ('trip-001', 'order-001', 1, 1, 1, 'dropoff_arrived', '2024-12-01 12:33:00', 'in_delivery',
     '{"dropoff_arrival_time": "2024-12-01T12:33:00Z"}'::jsonb),
    
    ('trip-001', 'order-001', 1, 1, 1, 'delivered', '2024-12-01 12:35:00', 'completed',
     '{"delivery_time_minutes": 17, "eater_rating": 5, "courier_rating": 5, "merchant_rating": 4, "tip_amount": 5.00, "distance_miles": 2.5}'::jsonb),
    
    -- Trip 2: Another complete delivery
    ('trip-002', 'order-002', 2, 2, NULL, 'order_placed', '2024-12-01 18:30:00', 'pending',
     '{"subtotal_amount": 45.00, "delivery_fee": 4.00, "service_fee": 2.50, "tax_amount": 3.50, "total_amount": 55.00, "items": [{"name": "Sushi Platter", "quantity": 1, "price": 45.00}], "payment_method": "paypal", "pickup_address": "12 Avenue de l''Opéra, Paris", "dropoff_address": "45 Avenue Montaigne, Paris"}'::jsonb),
    
    ('trip-002', 'order-002', 2, 2, NULL, 'order_accepted', '2024-12-01 18:32:00', 'accepted',
     '{"estimated_prep_time_minutes": 20}'::jsonb),
    
    ('trip-002', 'order-002', 2, 2, 2, 'courier_dispatched', '2024-12-01 18:45:00', 'dispatched',
     '{"courier_distance_km": 1.8}'::jsonb),
    
    ('trip-002', 'order-002', 2, 2, 2, 'pickup_arrived', '2024-12-01 18:52:00', 'picked_up',
     '{"arrival_time": "2024-12-01T18:52:00Z"}'::jsonb),
    
    ('trip-002', 'order-002', 2, 2, 2, 'pickup_completed', '2024-12-01 18:54:00', 'picked_up',
     '{"actual_prep_time_minutes": 22}'::jsonb),
    
    ('trip-002', 'order-002', 2, 2, 2, 'dropoff_arrived', '2024-12-01 19:06:00', 'in_delivery',
     '{"dropoff_arrival_time": "2024-12-01T19:06:00Z"}'::jsonb),
    
    ('trip-002', 'order-002', 2, 2, 2, 'delivered', '2024-12-01 19:08:00', 'completed',
     '{"delivery_time_minutes": 14, "eater_rating": 5, "courier_rating": 5, "merchant_rating": 5, "tip_amount": 8.00, "distance_miles": 1.8, "weather_condition": "cloudy"}'::jsonb),
    
    -- Trip 3: Cancelled order
    ('trip-003', 'order-003', 4, 4, NULL, 'order_placed', '2024-12-01 17:00:00', 'pending',
     '{"subtotal_amount": 65.00, "delivery_fee": 4.50, "service_fee": 2.50, "tax_amount": 5.00, "total_amount": 77.00, "discount_amount": 10.00, "items": [{"name": "Plat du Jour", "quantity": 2}], "payment_method": "credit_card", "promo_code": "PROMO10"}'::jsonb),
    
    ('trip-003', 'order-003', 4, 4, NULL, 'order_accepted', '2024-12-01 17:02:00', 'accepted',
     '{"estimated_prep_time_minutes": 18}'::jsonb),
    
    ('trip-003', 'order-003', 4, 4, NULL, 'cancelled', '2024-12-01 17:05:00', 'cancelled',
     '{"cancellation_reason": "cancelled_by_eater", "refund_amount": 77.00, "cancellation_fee": 0.00}'::jsonb);

-- =============================================================================
-- Grant permissions
-- =============================================================================

-- Grant replication permission to foodapp user (requis pour Debezium)
ALTER USER foodapp WITH REPLICATION;

-- =============================================================================
-- Verification
-- =============================================================================

-- Show table counts
SELECT 'eater' as table_name, COUNT(*) as count FROM eater
UNION ALL
SELECT 'merchant', COUNT(*) FROM merchant
UNION ALL
SELECT 'courier', COUNT(*) FROM courier
UNION ALL
SELECT 'trip_events', COUNT(*) FROM trip_events;

-- Show publication
SELECT * FROM pg_publication WHERE pubname = 'dbz_publication_ubear';

-- Show publication tables
SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_publication_ubear';
