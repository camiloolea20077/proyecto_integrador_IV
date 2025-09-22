WITH monthly_revenue AS (
    SELECT
        strftime('%m', order_purchase_timestamp) AS month_no,
        strftime('%Y', order_purchase_timestamp) AS year,
        SUM(payment_value) AS total_revenue
    FROM
        olist_order_payments
    JOIN
        olist_orders ON olist_order_payments.order_id = olist_orders.order_id
    GROUP BY
        month_no, year
)
SELECT
    month_no,
    CASE month_no
        WHEN '01' THEN 'Jan'
        WHEN '02' THEN 'Feb'
        WHEN '03' THEN 'Mar'
        WHEN '04' THEN 'Apr'
        WHEN '05' THEN 'May'
        WHEN '06' THEN 'Jun'
        WHEN '07' THEN 'Jul'
        WHEN '08' THEN 'Aug'
        WHEN '09' THEN 'Sep'
        WHEN '10' THEN 'Oct'
        WHEN '11' THEN 'Nov'
        WHEN '12' THEN 'Dec'
    END AS month,
    COALESCE(SUM(CASE WHEN year = '2016' THEN total_revenue END), 0.00) AS Year2016,
    COALESCE(SUM(CASE WHEN year = '2017' THEN total_revenue END), 0.00) AS Year2017,
    COALESCE(SUM(CASE WHEN year = '2018' THEN total_revenue END), 0.00) AS Year2018
FROM
    monthly_revenue
GROUP BY
    month_no
ORDER BY
    month_no;