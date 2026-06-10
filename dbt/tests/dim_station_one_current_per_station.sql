-- Invariant: exactly one is_current=TRUE row per station_nk.
-- A passing test returns zero rows; any rows here mean the snapshot has
-- diverged from SCD2 semantics (which shouldn't happen, but we test it).

select
    station_nk,
    count(*) as n_current_rows
from {{ ref('dim_station') }}
where is_current = true
group by station_nk
having count(*) > 1
