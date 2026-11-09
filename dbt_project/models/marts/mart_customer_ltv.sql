select
    c.customer_id,
    c.full_name,
    c.city,
    c.segment,
    min(o.order_date) as first_order_date,
    max(o.order_date) as last_order_date,
    count(o.order_id)::integer as order_count,
    coalesce(sum(o.order_total), 0)::decimal(14, 2) as lifetime_revenue
from {{ ref('dim_customers') }} as c
left join {{ ref('fct_orders') }} as o
    on c.customer_id = o.customer_id
    and o.order_status <> 'cancelled'
    and o.payment_status = 'captured'
where c.is_current
group by c.customer_id, c.full_name, c.city, c.segment
