select
    unique_id,
    collision_id,
    crash_date,
    crash_time,
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
    vehicle_damage,
    vehicle_damage_1,
    vehicle_damage_2,
    vehicle_damage_3,
    public_property_damage,
    public_property_damage_type,
    contributing_factor_1,
    contributing_factor_2,
    loaded_at,
    _src_file,
    {{ dbt_utils.generate_surrogate_key([
        'collision_id', 'vehicle_id', 'state_registration',
        'vehicle_type', 'vehicle_make', 'vehicle_model', 'vehicle_year',
        'travel_direction', 'vehicle_occupants',
        'driver_sex', 'driver_license_status', 'driver_license_jurisdiction',
        'pre_crash', 'point_of_impact',
        'vehicle_damage', 'vehicle_damage_1', 'vehicle_damage_2', 'vehicle_damage_3',
        'public_property_damage', 'public_property_damage_type',
        'contributing_factor_1', 'contributing_factor_2'
    ]) }} as row_hash
from {{ ref('stg_vehicles') }}
where
    unique_id    is not null
    and collision_id is not null
    and crash_date   is not null
    and crash_time   is not null
    and (vehicle_year      is null or (vehicle_year      >= 1900 and vehicle_year      <= 2030))
    and (vehicle_occupants is null or (vehicle_occupants >= 0    and vehicle_occupants <= 200))
