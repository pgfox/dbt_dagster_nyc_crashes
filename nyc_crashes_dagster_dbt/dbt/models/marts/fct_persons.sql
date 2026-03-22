with persons as (
    select
        person_record_id,
        collision_id,
        person_type,
        person_injury,
        bodily_injury,
        position_in_vehicle,
        safety_equipment,
        complaint,
        person_sex,
        person_age,
        ejection,
        emotional_status
    from {{ ref('persons') }}
),

crashes as (
    select
        collision_id::text as collision_id,
        crash_date,
        extract(hour from crash_time)::int as crash_hour,
        zip_code
    from {{ ref('crashes') }}
)

select
    p.person_record_id,
    p.collision_id,
    c.crash_date,
    c.crash_hour,
    c.zip_code,
    p.person_type,
    p.person_sex,
    p.person_age,
    p.person_injury,
    p.bodily_injury,
    p.safety_equipment,
    p.position_in_vehicle,
    p.ejection,
    (p.person_injury = 'injured')  as is_injured,
    (p.person_injury = 'killed')   as is_killed
from persons p
left join crashes c on c.collision_id = p.collision_id
