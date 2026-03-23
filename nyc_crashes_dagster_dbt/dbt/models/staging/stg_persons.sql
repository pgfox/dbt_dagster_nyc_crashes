{% set core_persons = adapter.get_relation(
    database=target.database, schema='core', identifier='persons') %}

with source_data as (
    select
        unique_id,
        collision_id,
        {{ dbt.safe_cast('crash_date', 'date') }}           as crash_date,
        {{ dbt.safe_cast('crash_time', 'time') }}           as crash_time,
        nullif(trim(lower(person_id)), '')                  as person_id,
        nullif(trim(lower(person_type)), '')                as person_type,
        nullif(trim(lower(person_injury)), '')              as person_injury,
        nullif(trim(lower(vehicle_id)), '')                 as vehicle_id,
        nullif(trim(lower(bodily_injury)), '')              as bodily_injury,
        nullif(trim(lower(position_in_vehicle)), '')        as position_in_vehicle,
        nullif(trim(lower(safety_equipment)), '')           as safety_equipment,
        nullif(trim(lower(ped_location)), '')               as ped_location,
        nullif(trim(lower(ped_action)), '')                 as ped_action,
        nullif(trim(lower(complaint)), '')                  as complaint,
        nullif(trim(lower(ped_role)), '')                   as ped_role,
        nullif(trim(lower(contributing_factor_1)), '')      as contributing_factor_1,
        nullif(trim(lower(contributing_factor_2)), '')      as contributing_factor_2,
        nullif(trim(lower(person_sex)), '')                 as person_sex,
        case
            when trim(person_age) ~ '^\d+$' then trim(person_age)::int
        end                                                 as person_age,
        nullif(trim(lower(ejection)), '')                   as ejection,
        nullif(trim(lower(emotional_status)), '')           as emotional_status,
        loaded_at,
        _src_file
    from {{ source('raw', 'persons') }}
    {% if core_persons is not none and not flags.FULL_REFRESH %}
    where loaded_at > coalesce(
        (select max(loaded_at) from {{ core_persons }}),
        '1900-01-01'::timestamptz
    )
    {% endif %}
),

deduped as (
    select
        *,
        row_number() over (
            partition by collision_id, person_id
            order by loaded_at desc
        ) as rn
    from source_data
)

select
    unique_id, collision_id, crash_date, crash_time,
    person_id, person_type, person_injury, vehicle_id,
    bodily_injury, position_in_vehicle, safety_equipment,
    ped_location, ped_action, complaint, ped_role,
    contributing_factor_1, contributing_factor_2,
    person_sex, person_age, ejection, emotional_status,
    loaded_at, _src_file
from deduped
where rn = 1
