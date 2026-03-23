{{ config(materialized='incremental', unique_key='unique_id') }}

select
    {{ dbt_utils.generate_surrogate_key([
        's.collision_id', 's.vehicle_id', 's.state_registration',
        's.vehicle_type', 's.vehicle_make', 's.vehicle_model'
    ]) }}                           as crashed_vehicle_id,
    s.unique_id,
    s.collision_id,
    s.vehicle_id,
    s.state_registration,
    s.vehicle_type,
    s.vehicle_make,
    s.vehicle_model,
    s.vehicle_year,
    s.travel_direction,
    s.vehicle_occupants,
    s.driver_sex,
    s.driver_license_status,
    s.driver_license_jurisdiction,
    s.pre_crash,
    s.point_of_impact,
    s.public_property_damage,
    s.public_property_damage_type,
    s.row_hash,
    {% if is_incremental() %}
    coalesce(e.loaded_at, s.loaded_at) as loaded_at,
    case when e.unique_id is not null then s.loaded_at end as updated_at,
    {% else %}
    s.loaded_at,
    null::timestamptz as updated_at,
    {% endif %}
    s._src_file
from {{ ref('stg_vehicles_clean') }} s
{% if is_incremental() %}
left join {{ this }} e on e.unique_id = s.unique_id
where s.row_hash != coalesce(e.row_hash, '')
{% endif %}
