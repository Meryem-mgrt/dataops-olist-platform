--Enrichit les commandes avec les infos client et calcule le délai de livraison réel vs estimé.
with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

enriched as (

    select
        o.order_id,
        o.customer_id,
        o.status,
        o.purchase_ts,
        o.delivered_customer_date,
        o.estimated_delivery_date,
        c.city as customer_city,
        c.state as customer_state,

        -- Délai de livraison réel, en jours
        extract(day from (o.delivered_customer_date - o.purchase_ts)) as delivery_days,

        -- Écart entre livraison réelle et estimée (positif = retard, négatif = en avance)
        extract(day from (o.delivered_customer_date - o.estimated_delivery_date)) as delivery_delay_days

    from orders o
    left join customers c on o.customer_id = c.customer_id

)

select * from enriched