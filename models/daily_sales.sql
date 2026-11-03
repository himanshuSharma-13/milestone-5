-- Walkthrough note: this model turns clean orders into one row per day and
-- currency. Reporting users see totals without customer or order details.
-- PLACED and SHIPPED count as sales in this demo; CANCELLED stays in silver
-- for history but does not contribute to these totals. A sales owner would
-- approve the real definition before production use.
select
    order_date,
    currency,
    -- Count orders and sum their amounts within each day/currency group.
    count(*) as order_count,
    sum(order_amount) as gross_sales
-- Note: ref() tells dbt that the silver seed is this model's input.
from {{ ref('orders_silver') }}
where order_status in ('PLACED', 'SHIPPED')
-- Keep currencies separate; adding INR and USD would give a meaningless sum.
group by order_date, currency
