-- Average (estimated - delivered) days per customer state (truncated to integer)
SELECT
  c.customer_state AS State,
  CAST(
    AVG(
      julianday(DATE(o.order_estimated_delivery_date))
      - julianday(DATE(o.order_delivered_customer_date))
    ) AS INTEGER
  ) AS Delivery_Difference
FROM olist_orders o
JOIN olist_customers c ON c.customer_id = o.customer_id
WHERE o.order_status = 'delivered'
  AND o.order_delivered_customer_date IS NOT NULL
GROUP BY c.customer_state
ORDER BY Delivery_Difference ASC, State ASC;
