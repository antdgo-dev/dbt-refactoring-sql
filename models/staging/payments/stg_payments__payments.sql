select
    *
    
from {{ source('dbt_refactoring', 'payments') }}