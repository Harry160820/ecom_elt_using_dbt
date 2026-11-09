{{ config(severity='error') }}
-- Alert threshold: a few bad rows are normal, a spike means the source is broken.
select
    q.quarantined,
    t.total,
    q.quarantined * 1.0 / t.total as rate
from (select count(*) as quarantined from {{ ref('quarantined_order_items') }}) as q
cross join (select count(*) as total from {{ ref('stg_order_items') }}) as t
where q.quarantined * 1.0 / t.total > 0.05
