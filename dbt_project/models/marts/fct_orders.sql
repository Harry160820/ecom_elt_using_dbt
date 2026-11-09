{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

-- INCREMENTAL fact: one row per order, always in its latest state.
-- Each run only looks at order events newer than what the table already holds
-- (the "high-water mark"), then replaces those orders (delete+insert on order_id).
-- A shipped/delivered event for an old order therefore updates that order's row.

with changed_events as (

    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}

),

latest_event as (

    select * from changed_events
    qualify row_number() over (partition by order_id order by updated_at desc) = 1

),

payment as (

    select order_id, method, status
    from {{ ref('stg_payments') }}
    qualify row_number() over (partition by order_id order by paid_at desc) = 1

)

select
    e.order_id,
    c.customer_key,
    e.customer_id,
    e.order_ts,
    cast(e.order_ts as date) as order_date,
    e.status as order_status,
    coalesce(t.valid_line_count, 0)::integer as valid_line_count,
    coalesce(t.total_units, 0)::integer as total_units,
    coalesce(t.order_total, 0)::decimal(14, 2) as order_total,
    p.method as payment_method,
    p.status as payment_status,
    e.updated_at
from latest_event as e
-- As-of join: pick the customer version that was valid at the moment of the order.
left join {{ ref('dim_customers') }} as c
    on e.customer_id = c.customer_id
    and e.order_ts >= c.valid_from
    and e.order_ts < coalesce(c.valid_to, timestamp '9999-12-31')
left join {{ ref('int_order_totals') }} as t on e.order_id = t.order_id
left join payment as p on e.order_id = p.order_id
