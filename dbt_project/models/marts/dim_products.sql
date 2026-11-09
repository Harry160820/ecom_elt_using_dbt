-- Type 1 dimension: only the latest attributes. Prices at purchase time live on the order lines.
select
    product_id,
    product_name,
    category,
    unit_price as current_unit_price
from {{ ref('stg_products') }}
qualify row_number() over (partition by product_id order by updated_at desc) = 1
