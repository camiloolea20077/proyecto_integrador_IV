
SELECT
    olist_products.product_category_name AS Category,  -- Nombre de la categorÃ­a
    COUNT(DISTINCT olist_orders.order_id) AS Num_order,  -- NÃºmero de pedidos Ãºnicos por categorÃ­a
    SUM(olist_order_payments.payment_value) AS Revenue  -- Ingreso total por categorÃ­a
FROM
    olist_orders
JOIN
    olist_order_items ON olist_orders.order_id = olist_order_items.order_id
JOIN
    olist_products ON olist_order_items.product_id = olist_products.product_id
JOIN
    olist_order_payments ON olist_orders.order_id = olist_order_payments.order_id
WHERE
    olist_orders.order_status = 'delivered'
    AND olist_products.product_category_name IS NOT NULL
    AND olist_orders.order_delivered_customer_date IS NOT NULL
GROUP BY
    olist_products.product_category_name
ORDER BY
    Revenue DESC  -- Ordenar de mayor a menor por ingresos
LIMIT 10;