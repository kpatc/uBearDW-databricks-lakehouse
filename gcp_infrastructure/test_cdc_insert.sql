-- Test CDC : Insertion d'un nouvel eater
INSERT INTO eater (eater_uuid, first_name, last_name, email, phone_number, address_line_1, city, state_province, postal_code, country, default_payment_method, is_active)
VALUES
    ('eater-uuid-TEST-001', 'TestUser', 'CDC', 'test.cdc@ubear.com', '+33699999999', '123 Test Street', 'Paris', 'Ile-de-France', '75001', 'France', 'credit_card', true);

-- Vérification
SELECT * FROM eater WHERE eater_uuid = 'eater-uuid-TEST-001';
