-- Silver : statuts de stations nettoyés, typés, dédoublonnés.
-- Clé d'unicité métier : (station_id, collected_at), matérialisée en status_key
-- pour le test `unique`. Contrainte CHECK du modèle : num_*_available >= 0.

with source as (

    select * from {{ source('bronze', 'station_status') }}

),

typed as (

    select
        cast(station_id as varchar)                        as station_id,
        cast(num_bikes_available as integer)               as num_bikes_available,
        cast(num_docks_available as integer)               as num_docks_available,
        cast(is_installed as boolean)                      as is_installed,
        cast(is_renting as boolean)                        as is_renting,
        cast(is_returning as boolean)                      as is_returning,
        cast(last_reported as timestamp with time zone)    as last_reported,
        cast(collected_at as timestamp with time zone)     as collected_at
    from source
    where station_id is not null
      and collected_at is not null
      and num_bikes_available >= 0
      and num_docks_available >= 0

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by station_id, collected_at
            order by last_reported desc nulls last
        ) as rang
    from typed

)

select
    station_id || '|' || strftime(collected_at, '%Y-%m-%dT%H:%M:%SZ') as status_key,
    station_id,
    num_bikes_available,
    num_docks_available,
    is_installed,
    is_renting,
    is_returning,
    last_reported,
    collected_at
from deduplicated
where rang = 1
