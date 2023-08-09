with

-- sources

paid_orders as (

    select * from {{ ref('int_paid_orders') }}
    
),

customer_orders as (

    select * from {{ ref('int_customer_orders') }}

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
        sum( paid_orders.total_amount_paid ) over( partition by paid_orders.customer_id order by paid_orders.order_id ) as customer_lifetime_value,
        customer_orders.first_order_date as fdos

    from paid_orders

    left join customer_orders using (customer_id)

    left outer join running_total_clv on running_total_clv.order_id = paid_orders.order_id

    order by paid_orders.order_id

)

-- simple select

select * from fct