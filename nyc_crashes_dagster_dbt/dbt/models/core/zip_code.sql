select
    zip_code,
    max(borough) as borough
from {{ ref('stg_crashes_clean') }}
where zip_code is not null
group by zip_code
