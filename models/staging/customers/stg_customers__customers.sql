select
    id as customer_id,
    first_name as customer_first_name,
    last_name as customer_last_name

from {{ source('dbt_refactoring', 'customers') }}