-- Money reconciliation: a captured payment must equal the order total. Returns offending orders.
select
    o.order_id,
    o.order_total,
    p.amount
from {{ ref('fct_orders') }} as o
inner join {{ ref('stg_payments') }} as p on o.order_id = p.order_id
where p.status = 'captured'
  and abs(o.order_total - p.amount) > 0.005
