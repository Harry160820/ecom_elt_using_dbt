select
    cast(d as date) as date_day,
    year(d)::integer as year,
    month(d)::integer as month,
    strftime(d, '%B') as month_name,
    isodow(d)::integer as iso_day_of_week,
    isodow(d) in (6, 7) as is_weekend
from generate_series(date '2024-01-01', date '2026-12-31', interval 1 day) as t(d)
