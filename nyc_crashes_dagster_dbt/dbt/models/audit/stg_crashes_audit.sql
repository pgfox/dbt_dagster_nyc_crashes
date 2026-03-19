select
    *,
    'zip_code is not 5 numeric digits: ' || zip_code as reason
from {{ ref('stg_crashes') }}
where zip_code is not null
  and zip_code !~ '^[0-9]{5}$'
