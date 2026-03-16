with vehicles as (
    select * from {{ ref('crashed_vehicle') }}
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
    v.crashed_vehicle_id,
    v.collision_id,
    c.crash_date,
    c.crash_hour,
    c.zip_code,
    {{ dbt_utils.generate_surrogate_key(['v.vehicle_type', 'v.vehicle_make', 'v.vehicle_model']) }} as dim_vehicle_id,
    v.vehicle_type,
    v.vehicle_make,
    v.vehicle_model,
    v.vehicle_year,
    v.driver_sex,
    v.vehicle_occupants,
    v.state_registration,
    v.travel_direction,
    v.driver_license_status,
    v.pre_crash,
    v.point_of_impact,
    v.public_property_damage
from vehicles v
left join crashes c on c.collision_id = v.collision_id
