-- Two different marts must agree on total revenue. If they do not, a join fanned out or dropped rows.
select
    d.total as daily_total,
    c.total as city_total
from (select coalesce(sum(recognised_revenue), 0) as total from {{ ref('mart_daily_revenue') }}) as d
cross join (select coalesce(sum(recognised_revenue), 0) as total from {{ ref('mart_revenue_by_city') }}) as c
where abs(d.total - c.total) > 0.01
