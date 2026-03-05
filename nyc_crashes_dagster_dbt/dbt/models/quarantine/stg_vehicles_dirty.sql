select *
from {{ ref('stg_vehicles') }}
where
    unique_id    is null
    or collision_id is null
    or crash_date   is null
    or crash_time   is null
    or (vehicle_year      is not null and (vehicle_year      < 1900 or vehicle_year      > 2030))
    or (vehicle_occupants is not null and (vehicle_occupants < 0    or vehicle_occupants > 200))
