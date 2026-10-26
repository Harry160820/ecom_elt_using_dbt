-- One row per order *event* (created, shipped, delivered). The source re-sends some rows,
-- so we deduplicate on (order_id, updated_at).
select
    order_id,
    customer_id,
    order_ts,
    status,
    updated_at,
    _batch_date,
    _loaded_at
from {{ source('raw', 'orders') }}
qualify row_number() over (partition by order_id, updated_at order by _loaded_at desc) = 1
