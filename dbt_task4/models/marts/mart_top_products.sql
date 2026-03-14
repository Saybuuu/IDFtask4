select
    stock_code,
    any(description) as description,
    sum(quantity) as total_quantity,
    round(sum(quantity * unit_price), 2) as revenue,
    countDistinct(invoice_no) as total_orders
from {{ source('task4', 'stg_online_retail') }}
where quantity > 0
  and unit_price > 0
  and stock_code != ''
group by stock_code
order by revenue desc
limit 50