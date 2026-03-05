select
    collision_id,
    crash_date,
    crash_time
from {{ ref('stg_crashes_clean') }}
