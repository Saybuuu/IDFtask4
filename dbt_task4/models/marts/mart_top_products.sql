SELECT
    stock_code,
    any(description) AS description,
    sum(quantity) AS total_quantity,
    round(sum(quantity * unit_price), 2) AS revenue,
    countDistinct(invoice_no) AS total_orders
FROM {{ source('task4', 'stg_online_retail') }}
WHERE quantity > 0
  AND unit_price > 0
  AND stock_code != ''
GROUP BY stock_code
ORDER BY revenue DESC
LIMIT 50