-- Downstream analytical queries for the unified payment model.
-- 1. Daily payment volume and total amount by payment type
SELECT CAST(event_timestamp AS DATE) AS payment_date, payment_type,
       COUNT(*) AS txn_count, SUM(CAST(amount AS DOUBLE)) AS total_amount, currency
FROM payments_data.payment_events
GROUP BY payment_date, payment_type, currency ORDER BY payment_date, payment_type;

-- 2. Failure rate by source system
SELECT source_system, COUNT(*) AS total,
       SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
       ROUND(SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS failure_rate_pct
FROM payments_data.payment_events GROUP BY source_system ORDER BY failure_rate_pct DESC;

-- 3. Top 10 counterparties by transaction volume
SELECT COALESCE(counterparty_name, counterparty_id, 'Unknown') AS counterparty,
       payment_type, COUNT(*) AS txn_count, SUM(CAST(amount AS DOUBLE)) AS total_amount
FROM payments_data.payment_events
WHERE counterparty_name IS NOT NULL OR counterparty_id IS NOT NULL
GROUP BY counterparty, payment_type ORDER BY total_amount DESC LIMIT 10;

-- 4. Multi-currency breakdown
SELECT currency, COUNT(*) AS txn_count, SUM(CAST(amount AS DOUBLE)) AS total_amount,
       AVG(CAST(amount AS DOUBLE)) AS avg_amount, MIN(CAST(amount AS DOUBLE)) AS min_amount,
       MAX(CAST(amount AS DOUBLE)) AS max_amount
FROM payments_data.payment_events GROUP BY currency ORDER BY total_amount DESC;

-- 5. Cross-product customer activity (customers active across multiple products)
SELECT customer_id, COUNT(DISTINCT payment_type) AS product_count,
       COUNT(*) AS total_txns, SUM(CAST(amount AS DOUBLE)) AS total_spend
FROM payments_data.payment_events WHERE status = 'completed'
GROUP BY customer_id HAVING COUNT(DISTINCT payment_type) > 1 ORDER BY total_spend DESC;
