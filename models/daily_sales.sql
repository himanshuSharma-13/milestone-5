-- The business must approve this simple gross-sales definition.
select
    order_date,
    currency,
    count(*) as order_count,
    sum(order_amount) as gross_sales
from {{ ref('orders_silver') }}
where order_status in ('PLACED', 'SHIPPED')
group by order_date, currency
