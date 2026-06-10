{{ config(
    materialized='incremental',
    unique_key=['station_sk', 'posted_date_sk'],
    incremental_strategy='merge',
    cluster_by=['posted_date_sk', 'district_sk'],
    on_schema_change='fail'
) }}

-- Port of TRAFFIC_PEMS_DB.EDW.refresh_agg_traffic_daily(batch_id)
-- (sql/03_pipeline_fact_load.sql:84-140).
--
-- Aggregates fact_traffic_hour to one row per (station, day). On incremental
-- runs, only re-aggregates dates that have new hourly rows since the last
-- build of this model.

with fact as (
    select * from {{ ref('fact_traffic_hour') }}

    {% if is_incremental() %}
    where posted_date_sk in (
        select distinct posted_date_sk
        from {{ ref('fact_traffic_hour') }}
        where posted_date_sk > (
            select coalesce(max(posted_date_sk), 0)
            from {{ this }}
        )
    )
    {% endif %}
)

select
    station_sk,
    freeway_sk,
    district_sk,
    posted_date_sk,
    count(*)                  as hours_observed,
    sum(total_flow_veh)       as total_flow_veh,
    avg(avg_occupancy)        as avg_occupancy,
    avg(avg_speed_mph)        as avg_speed_mph,
    min(avg_speed_mph)        as peak_hour_speed_mph,
    sum(delay_veh_hours)      as total_delay_veh_hours,
    sum(vmt)                  as total_vmt,
    sum(vht)                  as total_vht
from fact
group by 1, 2, 3, 4
