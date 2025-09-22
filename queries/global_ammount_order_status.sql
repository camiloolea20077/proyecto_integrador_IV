SELECT
    order_status AS order_status,
    COUNT(*) AS Ammount
FROM
    olist_orders
GROUP BY
    order_status
ORDER BY
    Ammount DESC;