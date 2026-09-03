with source as (

    select * from {{ source('raw', 'raw_exchange_rate') }}

),

-- Comme cette table s'enrichit chaque jour (une nouvelle ligne par exécution),
-- on ne garde que le taux le PLUS RÉCENT, pour avoir une valeur unique
-- et à jour à utiliser dans le reste du pipeline
latest_rate as (

    select
        base_currency,
        target_usd,
        target_eur,
        rate_date,
        fetched_at

    from source
    order by fetched_at desc
    limit 1

)

select * from latest_rate