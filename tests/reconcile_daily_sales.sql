-- dbt tests fail when this query returns any rows.
with expected as (
    select order_date, currency, count(*) as order_count,
           sum(order_amount) as gross_sales
    from {{ ref('orders_silver') }}
    where order_status in ('PLACED', 'SHIPPED')
    group by order_date, currency
)
select coalesce(e.order_date, a.order_date) as order_date,
       coalesce(e.currency, a.currency) as currency
from expected e
full outer join {{ ref('daily_sales') }} a
    on e.order_date = a.order_date and e.currency = a.currency
where e.order_count is null or a.order_count is null
   or e.order_count <> a.order_count or e.gross_sales <> a.gross_sales
