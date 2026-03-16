select
    {{ dbt_utils.generate_surrogate_key([
        'collision_id', 'vehicle_id', 'state_registration',
        'vehicle_type', 'vehicle_make', 'vehicle_model'
    ]) }}                           as crashed_vehicle_id,
    collision_id,
    vehicle_id,
    state_registration,
    vehicle_type,
    vehicle_make,
    vehicle_model,
    vehicle_year,
    travel_direction,
    vehicle_occupants,
    driver_sex,
    driver_license_status,
    driver_license_jurisdiction,
    pre_crash,
    point_of_impact,
    public_property_damage,
    public_property_damage_type
from {{ ref('stg_vehicles_clean') }}
