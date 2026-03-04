-- TODO: add type casts and column selection
select *
from {{ source('raw', 'vehicles') }}
