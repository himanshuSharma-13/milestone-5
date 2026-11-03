-- Walkthrough note: a dbt test passes when this query returns zero rows.
-- First recalculate daily totals from the silver seed, then compare them
-- with daily_sales. Any row returned below is a group I need to investigate.
with expected as (
    select order_date, currency, count(*) as order_count,
           sum(order_amount) as gross_sales
    from {{ ref('orders_silver') }}
    where order_status in ('PLACED', 'SHIPPED')
    group by order_date, currency
)
-- Note: coalesce shows the date/currency even if one side is missing it.
select coalesce(e.order_date, a.order_date) as order_date,
       coalesce(e.currency, a.currency) as currency
from expected e
-- Full join finds groups that exist on only one side as well as both sides.
full outer join {{ ref('daily_sales') }} a
    on e.order_date = a.order_date and e.currency = a.currency
-- A missing group has a NULL count; matched groups must agree on both values.
where e.order_count is null or a.order_count is null
   or e.order_count <> a.order_count or e.gross_sales <> a.gross_sales
