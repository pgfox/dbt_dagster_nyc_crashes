select
    {{ dbt_utils.generate_surrogate_key(['collision_id', 'person_id']) }} as person_record_id,
    collision_id,
    crash_date,
    crash_time,
    person_id,
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
from {{ ref('stg_persons_clean') }}
