select *
from {{ ref('stg_vehicles') }}
where
    unique_id    is not null
    and collision_id is not null
    and crash_date   is not null
    and crash_time   is not null
    and (vehicle_year      is null or (vehicle_year      >= 1900 and vehicle_year      <= 2030))
    and (vehicle_occupants is null or (vehicle_occupants >= 0    and vehicle_occupants <= 200))
