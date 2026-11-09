-- Latest known state of each customer. This is what the SCD2 snapshot watches:
-- whenever a customer's `updated_at` moves, the snapshot closes the old version and opens a new one.
select *
from {{ ref('stg_customers') }}
qualify row_number() over (partition by customer_id order by updated_at desc) = 1
