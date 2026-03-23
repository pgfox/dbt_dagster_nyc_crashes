{{ config(materialized='incremental', unique_key='person_record_id') }}

select
    {{ dbt_utils.generate_surrogate_key(['s.collision_id', 's.person_id']) }} as person_record_id,
    s.collision_id,
    s.person_id,
    s.person_type,
    s.person_injury,
    s.bodily_injury,
    s.position_in_vehicle,
    s.safety_equipment,
    s.complaint,
    s.person_sex,
    s.person_age,
    s.ejection,
    s.emotional_status,
    s.row_hash,
    {% if is_incremental() %}
    coalesce(e.loaded_at, s.loaded_at)                              as loaded_at,
    case when e.person_record_id is not null then s.loaded_at end   as updated_at,
    {% else %}
    s.loaded_at,
    null::timestamptz                                               as updated_at,
    {% endif %}
    s._src_file
from {{ ref('stg_persons_clean') }} s
{% if is_incremental() %}
left join {{ this }} e on e.person_record_id =
    {{ dbt_utils.generate_surrogate_key(['s.collision_id', 's.person_id']) }}
where s.row_hash != coalesce(e.row_hash, '')
{% endif %}
