
SELECT
    olist_customers.customer_state AS customer_state,  -- Abreviatura del estado
    SUM(olist_order_payments.payment_value) AS Revenue  -- Ingreso total por estado
FROM
    olist_orders
JOIN
    olist_customers ON olist_orders.customer_id = olist_customers.customer_id
JOIN
    olist_order_payments ON olist_orders.order_id = olist_order_payments.order_id
WHERE
    olist_orders.order_status = 'delivered'
    AND olist_orders.order_delivered_customer_date IS NOT NULL
GROUP BY
    olist_customers.customer_state
ORDER BY
    Revenue DESC
LIMIT 10;