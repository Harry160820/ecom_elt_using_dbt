select batch_date, tables_loaded
from {{ ref('mart_pipeline_health') }}
where tables_loaded <> 5
