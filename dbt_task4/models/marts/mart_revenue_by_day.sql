SELECT
    toDate(invoice_date) AS day,
    round(sum(quantity * unit_price), 2) AS revenue,
    sum(quantity) AS total_quantity,
    count() AS row_count
FROM {{ source('task4', 'stg_online_retail') }}
WHERE quantity > 0
  AND unit_price > 0
GROUP BY day
ORDER BY day