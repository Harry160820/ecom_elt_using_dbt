select
    product_id,
    product_name,
    category,
    unit_price,
    updated_at,
    _batch_date,
    _loaded_at
from {{ source('raw', 'products') }}
qualify row_number() over (partition by product_id, updated_at order by _loaded_at desc) = 1
