-- Can run with:
-- build/release/duckdb ../benchmark_data/tpch/tpch_sf10.duckdb -f scripts/measure/tpch5.sql

-- https://duckdb.org/docs/stable/dev/profiling
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'tpch5.json';
PRAGMA profiling_coverage = 'SELECT';
-- PRAGMA profiling_mode = 'detailed';

SET disable_tiered_hash_cache = 'false';

load tpch;
-- call dbgen(sf = 10);
pragma tpch(5);
