-- Duplicate transactions
SELECT transaction_id, COUNT(*) AS cnt
FROM silver_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- Orphan transactions
SELECT t.*
FROM silver_transactions t
LEFT JOIN silver_customers c ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
