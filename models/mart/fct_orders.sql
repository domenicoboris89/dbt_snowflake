with cte as (
    select 
        stg_orders.order_id,
        stg_orders.customer_id,
        payment.amount

    from {{ ref('stg_orders') }}
    left join {{ ref('stg_payments') }}
    on stg_orders.order_id = stg_payments.order_id
)

select * from cte