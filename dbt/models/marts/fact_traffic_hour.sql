{{ config(
    materialized='incremental',
    unique_key=['station_sk', 'sample_datetime'],
    incremental_strategy='merge',
    cluster_by=['posted_date_sk', 'district_sk'],
    on_schema_change='fail'
) }}

-- Port of TRAFFIC_PEMS_DB.EDW.load_fact_traffic_hour(batch_id)
-- (sql/03_pipeline_fact_load.sql:20-81).
--
-- Differences from the proc:
--   - No batch_id column. dbt incremental key is (station_sk, sample_datetime)
--     instead of (ingest_batch_id) + NOT EXISTS anti-join.
--   - Subsequent runs only consider staging rows newer than max(sample_datetime)
--     already in this model. First build is a full CTAS.
--
-- Free-flow speed = 65 mph (typical CA mainline posted limit; tunable here).

with staging as (
    select * from {{ source('staging', 'stg_pems_hour_deduped') }}

    {% if is_incremental() %}
    where sample_datetime > (
        select coalesce(max(sample_datetime), '1900-01-01'::timestamp_ntz)
        from {{ this }}
    )
    {% endif %}
)

select
    st.station_sk,
    fwy.freeway_sk,
    coalesce(s.district, 0)                        as district_sk,
    coalesce(d.date_sk, 19000101)                  as posted_date_sk,
    extract(hour from s.sample_datetime)::smallint as hour_sk,
    s.sample_datetime,
    s.samples,
    s.pct_observed,
    s.total_flow_veh,
    s.avg_occupancy,
    s.avg_speed_mph,
    65                                              as free_flow_speed_mph,
    greatest(
        ((1.0 / nullif(s.avg_speed_mph, 0)) - (1.0 / 65))
            * coalesce(s.station_length_mi, 0.5)
            * 60.0,
        0
    )                                               as delay_min_per_veh,
    greatest(
        ((1.0 / nullif(s.avg_speed_mph, 0)) - (1.0 / 65))
            * coalesce(s.station_length_mi, 0.5)
            * coalesce(s.total_flow_veh, 0),
        0
    )                                               as delay_veh_hours,
    coalesce(s.total_flow_veh, 0)
        * coalesce(s.station_length_mi, 0.5)        as vmt,
    coalesce(s.total_flow_veh, 0)
        * coalesce(s.station_length_mi, 0.5)
        / nullif(s.avg_speed_mph, 0)                as vht
from staging s
join {{ source('edw', 'dim_station') }} st
    on st.station_nk = s.station_id and st.is_current
join {{ source('edw', 'dim_freeway') }} fwy
    on fwy.freeway_number = s.freeway
   and fwy.direction_of_travel = s.direction_of_travel
left join {{ source('edw', 'dim_date') }} d
    on d.full_date = date(s.sample_datetime)
