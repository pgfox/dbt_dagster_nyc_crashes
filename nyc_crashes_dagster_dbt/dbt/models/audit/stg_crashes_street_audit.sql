select *, 'on_street_name is numeric only: ' || on_street_name as reason
from {{ ref('stg_crashes') }}
where on_street_name ~ '^\d+$'

union all

select *, 'cross_street_name is numeric only: ' || cross_street_name as reason
from {{ ref('stg_crashes') }}
where cross_street_name ~ '^\d+$'

union all

select *, 'off_street_name is numeric only: ' || off_street_name as reason
from {{ ref('stg_crashes') }}
where off_street_name ~ '^\d+$'