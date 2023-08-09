with

payments as (

    select * from {{ ref('stg_payments__payments') }}

),

orders as (

    select * from {{ ref('stg_orders__orders') }}

),

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

)

select * from paid_orders