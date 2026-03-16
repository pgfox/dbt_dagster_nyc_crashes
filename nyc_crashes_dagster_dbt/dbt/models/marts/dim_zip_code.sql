select
    zip_code,
    borough
from {{ ref('zip_code') }}
