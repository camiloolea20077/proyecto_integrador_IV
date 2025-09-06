WITH delivered_orders AS (
  SELECT o.order_id
  FROM olist_orders o
  WHERE o.order_status = 'delivered'
    AND o.order_delivered_customer_date IS NOT NULL
),
items_cat AS (
  SELECT
    oi.order_id,
    t.product_category_name_english AS Category,
    oi.price
  FROM olist_order_items oi
  JOIN olist_products pr ON pr.product_id = oi.product_id
  LEFT JOIN product_category_name_translation t
         ON t.product_category_name = pr.product_category_name
  JOIN delivered_orders d ON d.order_id = oi.order_id
  WHERE t.product_category_name_english IS NOT NULL
)
SELECT
  Category,
  COUNT(DISTINCT order_id) AS Num_order,
  ROUND(SUM(price), 2) AS Revenue
FROM items_cat
GROUP BY Category
ORDER BY Revenue DESC, Category ASC
LIMIT 10;