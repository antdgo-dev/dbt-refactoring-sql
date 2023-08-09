WITH

-- sources

customers as (

    select
        id as customer_id,
        first_name as customer_first_name,
        last_name as customer_last_name

    from {{ source('dbt_refactoring', 'customers') }}

),

orders as (

    select
        id as order_id,
        user_id as customer_id,
        order_date as order_placed_at,
        status as order_status
    
    from {{ source('dbt_refactoring', 'orders') }}

),

payments as (

    select * from {{ source('dbt_refactoring', 'payments') }}

),

-- logical queries

finalized_payments as (

    select
        orderid as order_id,
        max(created) as payment_finalized_date,
        sum(amount) / 100.0 as total_amount_paid

    from payments
    
    where status <> 'fail'
    
    group by 1

),

paid_orders as (
    
    select
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        finalized_payments.total_amount_paid,
        finalized_payments.payment_finalized_date

    from orders

    left join finalized_payments on orders.order_id = finalized_payments.order_id

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
),

running_total_clv as (

    select
        order_id,
        sum( total_amount_paid ) over( partition by customer_id order by order_id ) as clv_bad
        
    from paid_orders

    order by order_id

),

-- final

fct as (

    select
        paid_orders.*,
        customer_orders.customer_first_name,
        customer_orders.customer_last_name,
        row_number() over (order by paid_orders.order_id) as transaction_seq,
        row_number() over (partition by customer_orders.customer_id order by paid_orders.order_id) as customer_sales_seq,
        case when customer_orders.first_order_date = paid_orders.order_placed_at then 'new' else 'return' end as nvsr,
        running_total_clv.clv_bad as customer_lifetime_value,
        customer_orders.first_order_date as fdos

    from paid_orders

    left join customer_orders using (customer_id)

    left outer join running_total_clv on running_total_clv.order_id = paid_orders.order_id

    order by paid_orders.order_id

)

-- simple select

select * from fct