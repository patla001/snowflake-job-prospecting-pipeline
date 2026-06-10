{{ config(materialized='table') }}

-- Port of TRAFFIC_PEMS_DB.EDW.merge_dim_freeway() (sql/03_pipeline_scd2_merge.sql).
-- Side-by-side build into TRAFFIC_PEMS_DB.DBT_MARTS.dim_freeway —
-- the proc-built EDW.dim_freeway is still what Tableau reads. Diff rowcounts
-- to verify correctness before repointing downstream consumers.

select
    freeway              as freeway_number,
    direction_of_travel,
    'SR-' || freeway::varchar || ' ' || direction_of_travel as freeway_label,
    any_value(district)  as district
from {{ source('staging', 'stg_pems_hour_deduped') }}
where freeway is not null
  and direction_of_travel is not null
group by freeway, direction_of_travel
