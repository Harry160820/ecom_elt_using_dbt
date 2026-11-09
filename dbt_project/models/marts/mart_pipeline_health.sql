-- One row per batch day: what was loaded, and how much was quarantined. Feeds the HTML report.
with audit as (
    select * from {{ source('raw', '_load_audit') }}
),

quarantined as (
    select _batch_date as batch_date, count(*) as quarantined_items
    from {{ ref('quarantined_order_items') }}
    group by 1
)

select
    a.batch_date,
    coalesce(sum(a.row_count) filter (where a.table_name = 'customers'), 0)::integer as customers_rows,
    coalesce(sum(a.row_count) filter (where a.table_name = 'products'), 0)::integer as products_rows,
    coalesce(sum(a.row_count) filter (where a.table_name = 'orders'), 0)::integer as orders_rows,
    coalesce(sum(a.row_count) filter (where a.table_name = 'order_items'), 0)::integer as order_items_rows,
    coalesce(sum(a.row_count) filter (where a.table_name = 'payments'), 0)::integer as payments_rows,
    count(distinct a.table_name)::integer as tables_loaded,
    coalesce(max(q.quarantined_items), 0)::integer as quarantined_items,
    max(a.loaded_at) as loaded_at
from audit as a
left join quarantined as q on a.batch_date = q.batch_date
group by a.batch_date
