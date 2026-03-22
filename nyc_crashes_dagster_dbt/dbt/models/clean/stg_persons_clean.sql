select
    unique_id,
    collision_id,
    crash_date,
    crash_time,
    person_id,
    case
        when person_type in ('bicyclist', 'occupant', 'other motorized', 'pedestrian')
        then person_type
    end                                                     as person_type,
    case
        when person_injury in ('injured', 'killed')
        then person_injury
    end                                                     as person_injury,
    vehicle_id,
    bodily_injury,
    position_in_vehicle,
    safety_equipment,
    ped_location,
    ped_action,
    complaint,
    ped_role,
    contributing_factor_1,
    contributing_factor_2,
    person_sex,
    person_age,
    ejection,
    emotional_status,
    loaded_at,
    _src_file
from {{ ref('stg_persons') }}
where
    unique_id    is not null
    and collision_id is not null
    and crash_date   is not null
    and crash_time   is not null
