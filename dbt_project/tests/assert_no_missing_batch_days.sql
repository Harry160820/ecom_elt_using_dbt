-- Gap detection: every calendar day between the first and last batch must have been loaded.
with bounds as (
    select min(batch_date) as lo, max(batch_date) as hi from {{ ref('mart_pipeline_health') }}
),

expected as (
    select cast(d as date) as batch_date
    from bounds, generate_series(lo, hi, interval 1 day) as t(d)
)

select e.batch_date as missing_batch_date
from expected as e
left join {{ ref('mart_pipeline_health') }} as h on e.batch_date = h.batch_date
where h.batch_date is null
