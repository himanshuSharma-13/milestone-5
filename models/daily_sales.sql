-- This table contains only daily totals, so reporting users do not need
-- customer IDs or individual order rows. The sales owner approves the formula.
select
    order_date,
    currency,
    count(*) as order_count,
    sum(order_amount) as gross_sales
from {{ ref('orders_silver') }}
where order_status in ('PLACED', 'SHIPPED')
group by order_date, currency
