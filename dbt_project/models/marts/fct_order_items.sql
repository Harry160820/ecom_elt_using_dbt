select
    i.order_id,
    i.line_no,
    i.product_id,
    p.category,
    o.order_date,
    o.order_status,
    i.quantity,
    i.unit_price,
    i.line_total
from {{ ref('stg_order_items') }} as i
inner join {{ ref('fct_orders') }} as o on i.order_id = o.order_id
left join {{ ref('dim_products') }} as p on i.product_id = p.product_id
where i.is_valid
