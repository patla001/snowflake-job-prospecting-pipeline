{{ config(materialized='table') }}

-- Port of the full dim_station produced by EDW.merge_dim_station_scd2().
-- The SCD2 history comes from the snapshot; the Type 1 attributes (lat/lon,
-- lane_count, length_mi, station_name, etc) are joined from station meta —
-- equivalent to Step 3 in the proc (sql/03_pipeline_scd2_merge.sql:85-114).
--
-- Surrogate-key strategy: this model exposes dbt_scd_id as station_sk
-- (a VARCHAR hash, not an INTEGER like the proc-built dim). Joins from
-- fact_traffic_hour to this dim require an integration step that maps the
-- proc's INTEGER station_sk to the snapshot's hash — out of scope for the
-- per-model port. fact_traffic_hour still joins EDW.dim_station for now.

with snap as (
    select * from {{ ref('dim_station_snapshot') }}
),

meta as (
    select
        station_id,
        any_value(latitude)     as latitude,
        any_value(longitude)    as longitude,
        any_value(length_mi)    as length_mi,
        any_value(lane_count)   as lane_count,
        any_value(station_name) as station_name,
        any_value(state_pm)     as state_pm,
        any_value(abs_pm)       as abs_pm,
        any_value(county_id)    as county_id,
        any_value(city_id)      as city_id
    from {{ source('staging', 'stg_pems_station_meta_raw') }}
    group by station_id
)

select
    snap.dbt_scd_id                              as station_sk,
    snap.station_id                              as station_nk,
    snap.freeway,
    snap.direction_of_travel,
    snap.district,
    snap.station_type,
    coalesce(meta.station_name,
             'Station ' || snap.station_id::varchar) as station_name,
    meta.latitude,
    meta.longitude,
    meta.length_mi,
    meta.lane_count,
    meta.state_pm,
    meta.abs_pm,
    meta.county_id,
    meta.city_id,
    snap.dbt_valid_from                          as effective_from,
    snap.dbt_valid_to                            as effective_to,
    snap.dbt_valid_to is null                    as is_current
from snap
left join meta
    on meta.station_id = snap.station_id
