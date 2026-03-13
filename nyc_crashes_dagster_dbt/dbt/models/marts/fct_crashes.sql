with vehicles as (
    select
        collision_id,
        count(*) as vehicle_count
    from {{ ref('crashed_vehicle') }}
    group by collision_id
),

persons as (
    select
        collision_id,
        count(*) filter (where person_injury = 'injured') as persons_injured,
        count(*) filter (where person_injury = 'killed')  as persons_killed
    from {{ ref('persons') }}
    group by collision_id
)

select
    c.collision_id,
    c.crash_date,
    extract(hour from c.crash_time)::int as crash_hour,
    c.zip_code,
    c.on_street_name,
    c.cross_street_name,
    c.off_street_name,
    coalesce(v.vehicle_count, 0)    as vehicle_count,
    coalesce(p.persons_injured, 0)  as persons_injured,
    coalesce(p.persons_killed, 0)   as persons_killed
from {{ ref('crashes') }} c
left join vehicles v on v.collision_id = c.collision_id::text
left join persons  p on p.collision_id = c.collision_id::text
