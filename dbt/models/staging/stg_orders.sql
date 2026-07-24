with source as (

    select * from {{ source('raw', 'raw_orders') }}  
),

renamed as (

    select
        order_id,
        customer_id,
        order_status as status,
        order_purchase_timestamp as purchase_ts,
        order_approved_at as approved_at,
        order_delivered_carrier_date as delivered_carrier_date,
        order_delivered_customer_date as delivered_customer_date,
        order_estimated_delivery_date as estimated_delivery_date

    from source
    where order_id is not null

)

select * from renamed