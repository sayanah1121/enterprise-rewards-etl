-- Check if tables exist
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public';

SELECT 
    customer_id, 
    total_points, 
    total_spend, 
    transaction_count
FROM customer_rewards
ORDER BY total_points DESC
LIMIT 10;





SELECT 
    category, 
    total_points_distributed AS points_given,
    ROUND(CAST(avg_transaction_value AS numeric), 2) AS avg_ticket_size
FROM category_stats
ORDER BY points_given DESC;

SELECT 
    SUM(total_points_distributed) AS total_liability_points
FROM category_stats;