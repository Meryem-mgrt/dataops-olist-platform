with source as (

    select * from {{ source('raw', 'raw_order_items') }}

),

renamed as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value

    from source
    where order_id is not null
      and product_id is not null

)

select * from renamed