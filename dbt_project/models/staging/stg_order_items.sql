-- Flags rows that break business rules instead of silently dropping them.
-- Invalid rows are routed to `quarantined_order_items` so they can be inspected and fixed upstream.
select
    order_id,
    line_no,
    product_id,
    quantity,
    unit_price,
    cast(quantity * unit_price as decimal(14, 2)) as line_total,
    (quantity > 0 and unit_price >= 0) as is_valid,
    _batch_date,
    _loaded_at
from {{ source('raw', 'order_items') }}
qualify row_number() over (partition by order_id, line_no order by _loaded_at desc) = 1
