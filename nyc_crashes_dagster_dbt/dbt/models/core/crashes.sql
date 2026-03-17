select
    collision_id,
    crash_date,
    crash_time,
    location,
    zip_code,
    latitude,
    longitude,
    on_street_name,
    cross_street_name,
    off_street_name,
    loaded_at,
    _src_file
from {{ ref('stg_crashes_clean') }}
