-- SCD Type 2 dimension built on the snapshot. One row per customer *version*.
-- Facts join to the version that was valid when the order happened.
select
    md5(customer_id || '|' || cast(dbt_valid_from as varchar)) as customer_key,
    customer_id,
    full_name,
    email,
    city,
    segment,
    signup_date,
    cast(dbt_valid_from as timestamp) as valid_from,
    cast(dbt_valid_to as timestamp) as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('customers_snapshot') }}
