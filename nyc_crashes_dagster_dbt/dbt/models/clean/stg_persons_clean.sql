select *
from {{ ref('stg_persons') }}
where
    unique_id    is not null
    and collision_id is not null
    and crash_date   is not null
    and crash_time   is not null
