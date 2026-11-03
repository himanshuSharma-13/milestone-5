-- dbt tests fail when this query returns any rows.
-- Build the expected totals independently from the silver seed, then compare
-- them with the published daily_sales model at the same date/currency grain.
-- This catches a missing group, an extra group, a wrong count, or a wrong sum.
with expected as (
    select order_date, currency, count(*) as order_count,
           sum(order_amount) as gross_sales
    from {{ ref('orders_silver') }}
    where order_status in ('PLACED', 'SHIPPED')
    group by order_date, currency
)
-- coalesce gives a visible key even when a group exists on only one side.
select coalesce(e.order_date, a.order_date) as order_date,
       coalesce(e.currency, a.currency) as currency
from expected e
-- A full join also exposes a date/currency group missing from either side.
full outer join {{ ref('daily_sales') }} a
    on e.order_date = a.order_date and e.currency = a.currency
-- A NULL count identifies a group missing from one side of the join.
where e.order_count is null or a.order_count is null
   -- Compare both the number of orders and their summed value.
   or e.order_count <> a.order_count or e.gross_sales <> a.gross_sales
