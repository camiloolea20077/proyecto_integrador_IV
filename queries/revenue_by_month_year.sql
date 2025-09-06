WITH order_revenue AS (
  SELECT
    o.order_id,
    o.order_purchase_timestamp AS purchase_date,
    SUM(p.payment_value) AS revenue
  FROM olist_orders o
  JOIN olist_order_payments p ON p.order_id = o.order_id
  WHERE o.order_status = 'delivered'
    AND o.order_delivered_customer_date IS NOT NULL
  GROUP BY o.order_id, o.order_purchase_timestamp
),
months(month_no, month) AS (
  SELECT '01','Jan' UNION ALL SELECT '02','Feb' UNION ALL SELECT '03','Mar' UNION ALL SELECT '04','Apr'
  UNION ALL SELECT '05','May' UNION ALL SELECT '06','Jun' UNION ALL SELECT '07','Jul' UNION ALL SELECT '08','Aug'
  UNION ALL SELECT '09','Sep' UNION ALL SELECT '10','Oct' UNION ALL SELECT '11','Nov' UNION ALL SELECT '12','Dec'
)
SELECT
  m.month_no,
  m.month,
  COALESCE(SUM(CASE WHEN CAST(strftime('%Y', r.purchase_date) AS INTEGER) = 2016 THEN r.revenue END), 0.0) AS Year2016,
  COALESCE(SUM(CASE WHEN CAST(strftime('%Y', r.purchase_date) AS INTEGER) = 2017 THEN r.revenue END), 0.0) AS Year2017,
  COALESCE(SUM(CASE WHEN CAST(strftime('%Y', r.purchase_date) AS INTEGER) = 2018 THEN r.revenue END), 0.0) AS Year2018
FROM months m
LEFT JOIN order_revenue r
  ON printf('%02d', CAST(strftime('%m', r.purchase_date) AS INTEGER)) = m.month_no
GROUP BY m.month_no, m.month
ORDER BY CAST(m.month_no AS INTEGER);