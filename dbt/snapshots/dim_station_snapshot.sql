{% snapshot dim_station_snapshot %}

-- Port of TRAFFIC_PEMS_DB.EDW.merge_dim_station_scd2() Step 1 + Step 2
-- (sql/03_pipeline_scd2_merge.sql:20-83). The hand-rolled UPDATE+INSERT
-- pattern is replaced with dbt's 'check' snapshot strategy on the four
-- columns the proc treats as SCD2-tracked.
--
-- dbt manages dbt_valid_from / dbt_valid_to / dbt_scd_id automatically,
-- replacing the proc's effective_from / effective_to / station_sk.
--
-- Step 3 (Type 1 meta enrichment) lives in the downstream dim_station
-- model, not here — snapshots should be Type 2 only.

{{
    config(
      target_schema='SNAPSHOTS',
      unique_key='station_id',
      strategy='check',
      check_cols=['freeway', 'direction_of_travel', 'district', 'station_type'],
      invalidate_hard_deletes=True
    )
}}

-- Per-station "latest-known" tuple. The proc was run per-batch and its
-- current row always reflects the most-recent batch's attributes; we
-- mirror that by picking the row with max(sample_datetime) per station,
-- which is deterministic AND keeps the four tracked columns mutually
-- consistent (all from the same source row, not 4 independent ANY_VALUE
-- picks). A naive any_value aggregation produced 70/1676 station
-- mismatches vs the proc on stations with multi-year attribute drift.

select
    station_id,
    freeway,
    direction_of_travel,
    district,
    lane_type as station_type
from {{ source('staging', 'stg_pems_hour_deduped') }}
qualify row_number() over (
    partition by station_id
    order by sample_datetime desc
) = 1

{% endsnapshot %}
