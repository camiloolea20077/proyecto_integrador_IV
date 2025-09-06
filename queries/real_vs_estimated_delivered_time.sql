-- Real vs estimated delivery time (days) by month (2016–2018)
WITH delivered AS (
  SELECT
    o.order_id,
    DATE(o.order_purchase_timestamp) AS purchase_date,
    CAST(julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp) AS REAL) AS real_time,
    CAST(julianday(o.order_estimated_delivery_date) - julianday(o.order_purchase_timestamp) AS REAL) AS estimated_time
  FROM olist_orders o
  WHERE o.order_status = 'delivered'
    AND o.order_delivered_customer_date IS NOT NULL
  GROUP BY o.order_id
),
months(month_no, month) AS (
  SELECT '01','Jan' UNION ALL SELECT '02','Feb' UNION ALL SELECT '03','Mar' UNION ALL SELECT '04','Apr'
  UNION ALL SELECT '05','May' UNION ALL SELECT '06','Jun' UNION ALL SELECT '07','Jul' UNION ALL SELECT '08','Aug'
  UNION ALL SELECT '09','Sep' UNION ALL SELECT '10','Oct' UNION ALL SELECT '11','Nov' UNION ALL SELECT '12','Dec'
)
SELECT
  m.month_no,
  m.month,
  AVG(CASE WHEN strftime('%Y', d.purchase_date) = '2016' THEN d.real_time      END) AS Year2016_real_time,
  AVG(CASE WHEN strftime('%Y', d.purchase_date) = '2017' THEN d.real_time      END) AS Year2017_real_time,
  AVG(CASE WHEN strftime('%Y', d.purchase_date) = '2018' THEN d.real_time      END) AS Year2018_real_time,
  AVG(CASE WHEN strftime('%Y', d.purchase_date) = '2016' THEN d.estimated_time END) AS Year2016_estimated_time,
  AVG(CASE WHEN strftime('%Y', d.purchase_date) = '2017' THEN d.estimated_time END) AS Year2017_estimated_time,
  AVG(CASE WHEN strftime('%Y', d.purchase_date) = '2018' THEN d.estimated_time END) AS Year2018_estimated_time
FROM months m
LEFT JOIN delivered d
  ON strftime('%m', d.purchase_date) = m.month_no
GROUP BY m.month_no, m.month
ORDER BY m.month_no;
