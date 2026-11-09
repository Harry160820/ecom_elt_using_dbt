{{ config(materialized='table') }}
-- The "dead-letter" table: rows rejected by business rules, with the reason.
select
    order_id,
    line_no,
    product_id,
    quantity,
    unit_price,
    case
        when quantity <= 0 then 'non_positive_quantity'
        when unit_price < 0 then 'negative_price'
    end as reject_reason,
    _batch_date
from {{ ref('stg_order_items') }}
where not is_valid
