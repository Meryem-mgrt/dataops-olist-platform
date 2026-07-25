--La table finale — une ligne par commande, avec tout ce qu'il faut pour analyser les ventes.

with orders_enriched as (

    select * from {{ ref('int_orders_enriched') }}

),

items_summary as (

    select * from {{ ref('int_order_items_summary') }}

),

payments_summary as (

    select * from {{ ref('int_order_payments_summary') }}

),

final as (

    select
        oe.order_id,
        oe.customer_id,
        oe.customer_city,
        oe.customer_state,
        oe.status,
        oe.purchase_ts,
        oe.delivery_days,
        oe.delivery_delay_days,

        items.total_items,
        items.total_items_price,
        items.total_freight_value,

        pay.total_paid,
        pay.payment_count,
        pay.main_payment_type

    from orders_enriched oe
    left join items_summary items on oe.order_id = items.order_id
    left join payments_summary pay on oe.order_id = pay.order_id

)

select * from final