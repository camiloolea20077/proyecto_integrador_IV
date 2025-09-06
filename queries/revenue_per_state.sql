-- Top-10 customer states by revenue (from delivered orders' payments)
WITH order_revenue AS (
  SELECT
    o.order_id,
    o.customer_id,
    SUM(p.payment_value) AS revenue
  FROM olist_orders o
  JOIN olist_order_payments p ON p.order_id = o.order_id
  WHERE o.order_status = 'delivered'
    AND o.order_delivered_customer_date IS NOT NULL
  GROUP BY o.order_id
)
SELECT
  c.customer_state AS customer_state,
  ROUND(SUM(orv.revenue), 2) AS Revenue
FROM order_revenue orv
JOIN olist_customers c ON c.customer_id = orv.customer_id
GROUP BY c.customer_state
ORDER BY Revenue DESC
LIMIT 10;
