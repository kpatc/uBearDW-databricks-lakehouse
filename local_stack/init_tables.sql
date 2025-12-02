-- PostgreSQL OLTP Schema - uBear Eats Application Database
-- Modélisation minimaliste comme ferait un software engineer
-- L'enrichissement analytique (geocoding, metrics, etc.) se fera dans le Data Warehouse

-- Suppression des tables si elles existent déjà
DROP TABLE IF EXISTS trip_events CASCADE;
DROP TABLE IF EXISTS courier CASCADE;
DROP TABLE IF EXISTS merchant CASCADE;
DROP TABLE IF EXISTS eater CASCADE;

-- Table EATER (Clients/Consommateurs)
-- Contient uniquement les infos essentielles pour l'application
CREATE TABLE eater (
    eater_id BIGSERIAL PRIMARY KEY,
    eater_uuid TEXT NOT NULL UNIQUE,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    phone_number TEXT,
    address_line_1 TEXT,
    address_line_2 TEXT,
    city TEXT,
    state_province TEXT,
    postal_code TEXT,
    country TEXT DEFAULT 'France',
    default_payment_method TEXT DEFAULT 'credit_card',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table MERCHANT (Restaurants/Commerçants)
-- Infos business essentielles + adresse (pickup location)
CREATE TABLE merchant (
    merchant_id BIGSERIAL PRIMARY KEY,
    merchant_uuid TEXT NOT NULL UNIQUE,
    merchant_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    phone_number TEXT,
    business_type TEXT DEFAULT 'restaurant',
    cuisine_type TEXT,
    address_line_1 TEXT NOT NULL,
    address_line_2 TEXT,
    city TEXT NOT NULL,
    state_province TEXT,
    postal_code TEXT,
    country TEXT DEFAULT 'France',
    operating_hours TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table COURIER (Livreurs)
-- Infos essentielles pour l'application de livraison
CREATE TABLE courier (
    courier_id BIGSERIAL PRIMARY KEY,
    courier_uuid TEXT NOT NULL UNIQUE,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    phone_number TEXT,
    vehicle_type TEXT,
    license_plate TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    onboarding_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table TRIP_EVENTS (Événements de commande - Event Stream)
-- Table principale avec payload JSON contenant tous les détails
-- C'est le "event log" de l'application - format Debezium CDC
CREATE TABLE trip_events (
    event_id BIGSERIAL PRIMARY KEY,
    trip_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    eater_id BIGINT REFERENCES eater(eater_id),
    merchant_id BIGINT REFERENCES merchant(merchant_id),
    courier_id BIGINT REFERENCES courier(courier_id),
    event_type TEXT NOT NULL,
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour optimiser CDC et requêtes application
CREATE INDEX idx_trip_events_trip_id ON trip_events(trip_id);
CREATE INDEX idx_trip_events_order_id ON trip_events(order_id);
CREATE INDEX idx_trip_events_event_time ON trip_events(event_time);
CREATE INDEX idx_trip_events_event_type ON trip_events(event_type);
CREATE INDEX idx_eater_updated_at ON eater(updated_at);
CREATE INDEX idx_merchant_updated_at ON merchant(updated_at);
CREATE INDEX idx_courier_updated_at ON courier(updated_at);
CREATE INDEX idx_eater_email ON eater(email);
CREATE INDEX idx_merchant_city ON merchant(city);

-- Données de test réalistes
-- EATERS (Clients)
INSERT INTO eater (eater_uuid, first_name, last_name, email, phone_number, 
                   address_line_1, address_line_2, city, state_province, postal_code, country,
                   default_payment_method, is_active)
VALUES
    ('eater-uuid-001', 'Alice', 'Dupont', 'alice.dupont@email.com', '+33612345678',
     '10 Rue de Rivoli', 'Appartement 5B', 'Paris', 'Ile-de-France', '75001', 'France',
     'credit_card', TRUE),
    ('eater-uuid-002', 'Bob', 'Martin', 'bob.martin@email.com', '+33623456789',
     '45 Avenue Montaigne', NULL, 'Paris', 'Ile-de-France', '75008', 'France',
     'paypal', TRUE),
    ('eater-uuid-003', 'Charlie', 'Bernard', 'charlie.b@email.com', '+33634567890',
     '78 Rue Lafayette', 'Etage 3', 'Lyon', 'Auvergne-Rhone-Alpes', '69001', 'France',
     'credit_card', TRUE),
    ('eater-uuid-004', 'Diana', 'Petit', 'diana.petit@email.com', '+33645678901',
     '33 Boulevard Haussman', NULL, 'Paris', 'Ile-de-France', '75009', 'France',
     'apple_pay', TRUE),
    ('eater-uuid-005', 'Ethan', 'Roux', 'ethan.roux@email.com', '+33656789012',
     '12 Rue de la Republique', 'Batiment A', 'Marseille', 'Provence-Alpes-Cote', '13001', 'France',
     'credit_card', TRUE);

-- MERCHANTS (Restaurants)
INSERT INTO merchant (merchant_uuid, merchant_name, email, phone_number, business_type, cuisine_type,
                      address_line_1, address_line_2, city, state_province, postal_code, country,
                      operating_hours, is_active)
VALUES
    ('merchant-uuid-001', 'Le Bistrot Parisien', 'contact@bistrotparisien.fr', '+33145678901',
     'restaurant', 'French', '5 Rue Saint-Antoine', NULL, 'Paris', 'Ile-de-France', '75004', 'France',
     '11:00-23:00', TRUE),
    ('merchant-uuid-002', 'Sushi Master Tokyo', 'info@sushimaster.fr', '+33156789012',
     'restaurant', 'Japanese', '88 Rue de Richelieu', 'Rez-de-chaussee', 'Paris', 'Ile-de-France', '75002', 'France',
     '12:00-22:00', TRUE),
    ('merchant-uuid-003', 'Pizza Napoli Express', 'hello@pizzanapoli.fr', '+33167890123',
     'restaurant', 'Italian', '22 Cours Lafayette', NULL, 'Lyon', 'Auvergne-Rhone-Alpes', '69003', 'France',
     '11:30-23:30', TRUE),
    ('merchant-uuid-004', 'Burger House Premium', 'order@burgerhouse.fr', '+33178901234',
     'fast_food', 'American', '18 Boulevard Victor Hugo', NULL, 'Marseille', 'Provence-Alpes-Cote', '13006', 'France',
     '11:00-01:00', TRUE),
    ('merchant-uuid-005', 'Tacos & Burritos', 'contact@tacosburrito.fr', '+33189012345',
     'restaurant', 'Mexican', '67 Rue Paradis', NULL, 'Marseille', 'Provence-Alpes-Cote', '13001', 'France',
     '11:00-22:00', TRUE);

-- COURIERS (Livreurs)
INSERT INTO courier (courier_uuid, first_name, last_name, email, phone_number, 
                     vehicle_type, license_plate, is_active, onboarding_date)
VALUES
    ('courier-uuid-101', 'Jean', 'Courier', 'jean.courier@ubear.com', '+33690123456',
     'scooter', 'SC-123-AB', TRUE, '2022-01-20'),
    ('courier-uuid-102', 'Sophie', 'Rapid', 'sophie.rapid@ubear.com', '+33691234567',
     'bike', 'BK-456-CD', TRUE, '2022-03-15'),
    ('courier-uuid-103', 'Marc', 'Speedy', 'marc.speedy@ubear.com', '+33692345678',
     'car', 'CR-789-EF', TRUE, '2021-12-01'),
    ('courier-uuid-104', 'Julie', 'Fast', 'julie.fast@ubear.com', '+33693456789',
     'scooter', 'SC-321-GH', TRUE, '2022-06-10');

-- TRIP_EVENTS (Événements de commande avec payload JSON)
-- Le payload contient tous les détails: montants, timestamps, ratings, etc.
-- L'enrichissement (geocoding, metrics) sera fait dans le Data Warehouse

INSERT INTO trip_events (trip_id, order_id, eater_id, merchant_id, courier_id, event_type, event_time, payload)
VALUES
    -- Trip 1: Commande complète livrée (cycle complet d'événements)
    ('trip-001', 'order-001', 1, 1, 1, 'order_placed', 
     CURRENT_TIMESTAMP - INTERVAL '2 hours',
     '{"subtotal_amount": 32.50, "delivery_fee": 3.50, "service_fee": 2.00, "tax_amount": 3.85, "tip_amount": 5.00, "total_amount": 46.85, "items": [{"name": "Steak Frites", "price": 18.50, "quantity": 1}, {"name": "Salade Nicoise", "price": 14.00, "quantity": 1}], "promo_code": "FIRST10", "discount_amount": 3.25}'::jsonb),
    
    ('trip-001', 'order-001', 1, 1, 1, 'merchant_confirmed',
     CURRENT_TIMESTAMP - INTERVAL '1 hour 55 minutes',
     '{"estimated_prep_time_minutes": 20}'::jsonb),
    
    ('trip-001', 'order-001', 1, 1, 1, 'courier_assigned',
     CURRENT_TIMESTAMP - INTERVAL '1 hour 45 minutes',
     '{"courier_distance_miles": 0.8}'::jsonb),
    
    ('trip-001', 'order-001', 1, 1, 1, 'pickup_arrived',
     CURRENT_TIMESTAMP - INTERVAL '1 hour 30 minutes',
     '{"actual_prep_time_minutes": 18}'::jsonb),
    
    ('trip-001', 'order-001', 1, 1, 1, 'pickup_completed',
     CURRENT_TIMESTAMP - INTERVAL '1 hour 28 minutes',
     '{}'::jsonb),
    
    ('trip-001', 'order-001', 1, 1, 1, 'delivered',
     CURRENT_TIMESTAMP - INTERVAL '1 hour',
     '{"delivery_time_minutes": 15, "total_distance_miles": 2.5, "eater_rating": 5, "courier_rating": 5, "merchant_rating": 5, "trip_status": "delivered", "weather_condition": "clear"}'::jsonb),
    
    -- Trip 2: Commande en cours de livraison
    ('trip-002', 'order-002', 2, 2, 2, 'order_placed',
     CURRENT_TIMESTAMP - INTERVAL '45 minutes',
     '{"subtotal_amount": 42.00, "delivery_fee": 4.00, "service_fee": 2.50, "tax_amount": 4.85, "tip_amount": 3.00, "total_amount": 56.35, "items": [{"name": "Sushi Set Deluxe", "price": 42.00, "quantity": 1}]}'::jsonb),
    
    ('trip-002', 'order-002', 2, 2, 2, 'merchant_confirmed',
     CURRENT_TIMESTAMP - INTERVAL '40 minutes',
     '{"estimated_prep_time_minutes": 25}'::jsonb),
    
    ('trip-002', 'order-002', 2, 2, 2, 'courier_assigned',
     CURRENT_TIMESTAMP - INTERVAL '35 minutes',
     '{"courier_distance_miles": 1.2}'::jsonb),
    
    ('trip-002', 'order-002', 2, 2, 2, 'pickup_completed',
     CURRENT_TIMESTAMP - INTERVAL '15 minutes',
     '{"actual_prep_time_minutes": 25, "trip_status": "in_transit", "weather_condition": "rainy"}'::jsonb),
    
    -- Trip 3: Nouvelle commande (juste placée)
    ('trip-003', 'order-003', 4, 2, 2, 'order_placed',
     CURRENT_TIMESTAMP - INTERVAL '10 minutes',
     '{"subtotal_amount": 28.50, "delivery_fee": 2.90, "service_fee": 1.80, "tax_amount": 3.15, "tip_amount": 4.50, "total_amount": 40.85, "items": [{"name": "Ramen Bowl", "price": 16.50, "quantity": 1}, {"name": "Gyoza", "price": 12.00, "quantity": 1}], "promo_code": "SAVE5", "discount_amount": 1.50, "trip_status": "pending", "weather_condition": "clear"}'::jsonb);

-- Afficher résumé des données insérées
SELECT 
    'Eaters' as table_name, COUNT(*) as count FROM eater
UNION ALL
SELECT 'Merchants', COUNT(*) FROM merchant
UNION ALL
SELECT 'Couriers', COUNT(*) FROM courier
UNION ALL
SELECT 'Trip Events', COUNT(*) FROM trip_events;

-- Vérifier configuration CDC
SHOW wal_level;

-- Message succès
SELECT 'PostgreSQL OLTP Database initialized successfully!' as status,
       'CDC ready with wal_level=logical' as cdc_status;
