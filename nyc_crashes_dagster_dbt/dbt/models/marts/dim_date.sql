select distinct
    crash_date            as date_day,
    extract(dow from crash_date)::int as day_of_week,
    to_char(crash_date, 'Day') as day_name,
    extract(month from crash_date)::int as month,
    extract(quarter from crash_date)::int as quarter,
    extract(year from crash_date)::int  as year
from {{ ref('crashes') }}
