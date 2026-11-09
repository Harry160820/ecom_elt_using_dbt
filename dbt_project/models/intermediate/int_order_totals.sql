-- Order-level totals computed from *valid* lines only.
select
    order_id,
    count(*)::integer as valid_line_count,
    sum(quantity)::integer as total_units,
    sum(line_total)::decimal(14, 2) as order_total
from {{ ref('stg_order_items') }}
where is_valid
group by order_id
