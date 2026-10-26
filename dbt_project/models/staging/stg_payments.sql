select
    payment_id,
    order_id,
    method,
    amount,
    status,
    paid_at,
    _batch_date,
    _loaded_at
from {{ source('raw', 'payments') }}
qualify row_number() over (partition by payment_id order by _loaded_at desc) = 1
