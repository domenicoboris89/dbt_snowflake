with customers as (

    select * from {{ ref('stg_customers')}}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

lifetime_value as (

    select 
        customers.*,
        sum(stg_payments.amount) as lifetime_value
    
    from customers
    left join orders
    on customers.customer_id = orders.customer_id
    left join {{ ref('stg_payments') }}
    on orders.order_id = stg_payments.orderid

    group by 1, 2, 3

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

final as (

    select
        lifetime_value.customer_id,
        lifetime_value.first_name,
        lifetime_value.last_name,
        lifetime_value.lifetime_value,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders

    from lifetime_value

    left join customer_orders using (customer_id)

)

select * from final