select *
from {{ ref('stg_persons') }}
where
    unique_id    is null
    or collision_id is null
    or crash_date   is null
    or crash_time   is null
