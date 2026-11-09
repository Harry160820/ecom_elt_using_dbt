-- Revenue by the city the customer lived in *when they ordered* (thanks to SCD2),
-- not where they live today. A Type 1 dimension would give a different (wrong) answer.
select
    c.city,
    c.segment,
    count(*)::integer as orders,
    coalesce(sum(o.order_total), 0)::decimal(14, 2) as recognised_revenue
from {{ ref('fct_orders') }} as o
inner join {{ ref('dim_customers') }} as c on o.customer_key = c.customer_key
where o.order_status <> 'cancelled' and o.payment_status = 'captured'
group by c.city, c.segment
