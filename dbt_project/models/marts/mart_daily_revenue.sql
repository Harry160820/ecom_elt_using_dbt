-- Recognised revenue = order not cancelled AND payment captured.
select
    order_date,
    count(*) filter (where order_status <> 'cancelled')::integer as orders,
    count(*) filter (where order_status = 'cancelled')::integer as cancelled_orders,
    coalesce(
        sum(order_total) filter (where order_status <> 'cancelled' and payment_status = 'captured'), 0
    )::decimal(14, 2) as recognised_revenue,
    round(
        avg(order_total) filter (where order_status <> 'cancelled' and payment_status = 'captured'), 2
    )::decimal(14, 2) as avg_order_value,
    count(distinct customer_id)::integer as unique_customers
from {{ ref('fct_orders') }}
group by order_date
