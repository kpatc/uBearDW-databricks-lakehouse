-- Generate sample merchants, eaters and many trip_events with detailed payloads for silver/gold
-- Run as psql -U foodapp -d foodapp -f generate_data.sql

-- create more merchants
INSERT INTO merchant (merchant_id, merchant_uuid, merchant_name, address_line_1, address_line_2, city, state_province, postal_code, country, cuisine_type)
SELECT
  gs+2,
  md5(random()::text) as uuid,
  'Merchant ' || (gs+2)::text,
  (gs::text || ' Main St')::text,
  ''::text,
  (ARRAY['Paris','London','New York','Tokyo','Berlin'])[(gs % 5) + 1],
  'State',
  ('PC' || gs::text),
  'Country',
  (ARRAY['French','Italian','Japanese','Mexican','Indian'])[(gs % 5) + 1]
FROM generate_series(1, 50) gs
ON CONFLICT DO NOTHING;

-- create more eaters
INSERT INTO eater (eater_id, eater_uuid, first_name, last_name, email, address_line_1, address_line_2, city, state_province, postal_code, country)
SELECT
  gs+2,
  md5(random()::text),
  'Eater' || (gs+2)::text,
  'Surname' || (gs+2)::text,
  ('eater' || gs::text || '@example.com'),
  (gs::text || ' Elm St'),
  ''::text,
  (ARRAY['Paris','London','New York','Tokyo','Berlin'])[(gs % 5) + 1],
  'State',
  ('PC' || gs::text),
  'Country'
FROM generate_series(1, 200) gs
ON CONFLICT DO NOTHING;

-- Insert many trip_events with payload containing detailed fields
INSERT INTO trip_events (order_id, eater_id, merchant_id, courier_id, event_time, payload)
SELECT
  concat('order-', gs + 1000) as order_id,
  (1 + (random()*200))::int as eater_id,
  (1 + (random()*50))::int as merchant_id,
  (100 + (random()*100))::int as courier_id,
  now() - (interval '1 hour' * (random() * 24)),
  json_build_object(
    'order_id', concat('order-', gs + 1000),
    'eater_id', (1 + (random()*200))::int,
    'merchant_id', (1 + (random()*50))::int,
    'courier_id', (100 + (random()*900))::int,
    'pickup_location_id', (1000 + (random()*999))::int,
    'dropoff_location_id', (2000 + (random()*999))::int,
    'order_placed_at', (now() - (interval '1 day' * (random()*5)))::text,
    'order_accepted_at', (now() - (interval '1 day' * (random()*5) - interval '15 minutes'))::text,
    'courier_dispatched_at', (now() - (interval '1 day' * (random()*5) - interval '5 minutes'))::text,
    'pickup_arrived_at', (now() - (interval '1 day' * (random()*5) - interval '3 minutes'))::text,
    'pickup_completed_at', (now() - (interval '1 day' * (random()*5) + interval '3 minutes'))::text,
    'dropoff_arrived_at', (now() - (interval '1 day' * (random()*5) + interval '7 minutes'))::text,
    'delivered_at', (now() - (interval '1 day' * (random()*5) + interval '20 minutes'))::text,
    'subtotal_amount', round(random()*60 + 5, 2),
    'delivery_fee', round(random()*8 + 1, 2),
    'service_fee', round(random()*4 + 0.5, 2),
    'tax_amount', round(random()*6, 2),
    'tip_amount', round(random()*10, 2),
    'total_amount', round(random()*100 + 5, 2),
    'courier_payout', round(random()*30, 2),
    'distance_miles', round(random()*12, 2),
    'preparation_time_minutes', (5 + random()*35)::int,
    'delivery_time_minutes', (5 + random()*55)::int,
    'total_time_minutes', (10 + random()*90)::int,
    'trip_status', (ARRAY['DELIVERED','CANCELLED','IN_PROGRESS'])[((random()*3)::int)+1],
    'cancellation_reason', (CASE WHEN random() > 0.9 THEN 'restaurant_cancelled' ELSE NULL END),
    'is_scheduled_order', (random() > 0.9),
    'is_group_order', (random() > 0.95),
    'promo_code_used', (CASE WHEN random() > 0.85 THEN concat('PROMO', (random()*1000)::int) ELSE NULL END),
    'discount_amount', CASE WHEN random() > 0.85 THEN round(random()*10, 2) ELSE 0 END,
    'eater_rating', (1 + (random()*4))::int,
    'courier_rating', (1 + (random()*4))::int,
    'merchant_rating', (1 + (random()*4))::int,
    'date_partition', (now() - (interval '1 day' * (random()*7)))::date,
    'region_partition', (ARRAY['europe','north_america','asia'])[((random()*3)::int)+1],
    'weather_condition', (ARRAY['sunny','rain','snow','windy'])[((random()*4)::int)+1]
  )::text
FROM generate_series(1, 500) gs
ON CONFLICT DO NOTHING;

-- create some couriers
INSERT INTO courier (courier_id, courier_uuid, first_name, last_name, vehicle_type, license_plate, onboarding_date, primary_delivery_zone)
SELECT 100 + gs, md5(random()::text), concat('Courier', gs), concat('Surname', gs), (ARRAY['bike','car','scooter'])[(gs % 3) + 1], concat('LP', gs), (now() - (interval '30 days' * (random()*10)))::date, (ARRAY['Paris','Tokyo','New York'])[(gs % 3) + 1]
FROM generate_series(1, 200) gs
ON CONFLICT DO NOTHING;

-- Also create some updates to test CDC
UPDATE trip_events SET payload = jsonb_set(payload::jsonb, '{subtotal_amount}', '100.00')::text WHERE random() > 0.98;

-- Write a few updates for merchant/eater to test SCD2 captures
UPDATE merchant SET merchant_name = concat(merchant_name, ' (Updated)') WHERE merchant_id % 7 = 0;
UPDATE eater SET last_name = concat(last_name, ' (Updated)') WHERE eater_id % 10 = 0;

SELECT count(*) FROM trip_events;
