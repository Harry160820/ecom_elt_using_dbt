-- Staging: rename, cast, standardise. One row per customer *version* (event-level).
-- Exact duplicates re-sent by the source are removed here, nowhere else.
select
    customer_id,
    full_name,
    lower(email) as email,
    city,
    segment,
    signup_date,
    updated_at,
    _batch_date,
    _loaded_at
from {{ source('raw', 'customers') }}
qualify row_number() over (partition by customer_id, updated_at order by _loaded_at desc) = 1
