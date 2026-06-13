-- Gold : indicateur de tension par station.
-- Une station est « en tension » si elle passe au moins 20 % de ses
-- observations (en période de location active) sans aucun vélo disponible.

with statuts as (

    select * from {{ ref('stg_station_status') }}
    where is_renting

),

par_station as (

    select
        station_id,
        count(*)                                                  as nb_observations,
        count(*) filter (where num_bikes_available = 0)           as nb_obs_sans_velo,
        count(*) filter (where num_docks_available = 0)           as nb_obs_sans_place,
        avg(num_bikes_available)                                  as moyenne_velos_disponibles,
        min(collected_at)                                         as premiere_observation,
        max(collected_at)                                         as derniere_observation
    from statuts
    group by station_id

)

select
    station_id,
    nb_observations,
    nb_obs_sans_velo,
    nb_obs_sans_place,
    round(nb_obs_sans_velo * 100.0 / nb_observations, 2)  as pct_temps_sans_velo,
    round(nb_obs_sans_place * 100.0 / nb_observations, 2) as pct_temps_sans_place,
    round(moyenne_velos_disponibles, 2)                    as moyenne_velos_disponibles,
    nb_obs_sans_velo * 100.0 / nb_observations >= 20.0     as en_tension,
    premiere_observation,
    derniere_observation
from par_station
