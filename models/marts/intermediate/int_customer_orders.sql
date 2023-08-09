with

customers as (

    select * from {{ ref('stg_customers__customers') }}

),

orders as (

    select * from {{ ref('stg_orders__orders') }}

),

customer_orders as (

    select
        customers.customer_id,
        customers.customer_first_name,
        customers.customer_last_name,
        min(orders.order_placed_at) as first_order_date,
        max(orders.order_placed_at) as most_recent_order_date

    from customers

    left join orders on orders.customer_id = customers.customer_id

    group by 1, 2, 3
)

select * from customer_orders