SELECT
    toStartOfMonth(invoice_date) AS month,
    customer_id,
    round(sum(quantity * unit_price), 2) AS monthly_revenue,
    countDistinct(invoice_no) AS total_orders,
    sum(quantity) AS total_items
FROM {{ source('task4', 'stg_online_retail') }}
WHERE customer_id IS NOT NULL
  AND customer_id != 0
  AND quantity > 0
  AND unit_price > 0
GROUP BY month, customer_id
ORDER BY month, monthly_revenue DESC