with payments as (

    select * from {{ ref('stg_order_payments') }}

),

summary as (

    select
        order_id,
        count(*) as payment_count,
        sum(payment_value) as total_paid,
        max(payment_type) as main_payment_type  -- simplification : à affiner plus tard si besoin

    from payments
    group by order_id

)

select * from summary