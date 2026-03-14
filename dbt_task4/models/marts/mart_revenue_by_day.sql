select
    toDate(invoice_date) as day,
    round(sum(quantity * unit_price), 2) as revenue,
    sum(quantity) as total_quantity,
    count() as row_count
from {{ source('task4', 'stg_online_retail') }}
where quantity > 0
  and unit_price > 0
group by day
order by day