-- Walkthrough note: dbt expects this query to return no order IDs. A returned
-- ID points to a silver row with a bad quantity, amount, or future date.
-- The separate schema tests cover missing IDs and allowed status/currency.
select order_id
from {{ ref('orders_silver') }}
where quantity <= 0
   or order_amount < 0
   or order_date > current_date()
