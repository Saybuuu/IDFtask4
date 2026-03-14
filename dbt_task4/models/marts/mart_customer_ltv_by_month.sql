select
    toStartOfMonth(invoice_date) as month,
    customer_id,
    round(sum(quantity * unit_price), 2) as monthly_revenue,
    countDistinct(invoice_no) as total_orders,
    sum(quantity) as total_items
from {{ source('task4', 'stg_online_retail') }}
where customer_id is not null
  and customer_id != 0
  and quantity > 0
  and unit_price > 0
group by month, customer_id
order by month, monthly_revenue desc