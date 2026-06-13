-- Gold : profil horaire moyen de disponibilité par station.
-- Heure UTC de collected_at (notre horodatage d'observation), grain
-- station × heure matérialisé en profil_key pour le test `unique`.

with statuts as (

    select * from {{ ref('stg_station_status') }}

),

par_heure as (

    select
        station_id,
        cast(extract(hour from collected_at) as integer) as heure_utc,
        avg(num_bikes_available)                         as moyenne_velos,
        avg(num_docks_available)                         as moyenne_places,
        count(*)                                         as nb_observations
    from statuts
    group by station_id, extract(hour from collected_at)

)

select
    station_id || '|' || lpad(cast(heure_utc as varchar), 2, '0') as profil_key,
    station_id,
    heure_utc,
    round(moyenne_velos, 2)  as moyenne_velos,
    round(moyenne_places, 2) as moyenne_places,
    nb_observations
from par_heure
