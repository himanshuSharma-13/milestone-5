-- This table contains only daily totals, so reporting users do not need
-- customer IDs or individual order rows. The sales owner approves the formula.
-- Cancelled orders remain in silver for traceability but do not contribute
-- to this demo's sales count or amount. Currency stays in the grouping key
-- so unlike currencies are never added together.
select
    order_date,
    currency,
    count(*) as order_count,
    sum(order_amount) as gross_sales
from {{ ref('orders_silver') }}
where order_status in ('PLACED', 'SHIPPED')
group by order_date, currency
