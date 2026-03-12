/* Can run with:
clear; build/release/duckdb ../benchmark_data/tpch/tpch_sf10.duckdb -f scripts/measure/tpch5_analysis.sql
clear; build/release/duckdb ../benchmark_data/tpch/tpch_sf50.duckdb -f scripts/measure/tpch5_analysis.sql
clear; build/release/duckdb ../benchmark_data/tpch/tpch_sf100.duckdb -f scripts/measure/tpch5_analysis.sql
*/

-- https://duckdb.org/docs/stable/dev/profiling
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'results.json';
PRAGMA profiling_coverage = 'SELECT';
-- PRAGMA profiling_mode = 'detailed';

-------- Case #1: Old DuckDB --------------  
-- SET disable_rpt = true;
-- SET disable_tiered_hash_cache = true;
------------------------------------------


-------- Case #2: RPT+ Forward Pass Only -------- 
-- SET rpt_forward_only = true;
-- SET disable_tiered_hash_cache = true;
-------------------------------------------------


-------- Case #3: RPT+ Forward + THC -------- 
SET rpt_forward_only = true;
---------------------------------------------

-- SET threads = 1;
SET threads = 60;
SET pin_threads = 'on';
-- SET thc_size_mib= 4194304;
SET thc_size_mib = 33554432; --30 MiB
SET thc_collect_phase_rows = 100000; 
SET thc_collect_budget_fraction = 0.02; 
SET thc_miss_threshold = 0.05; 
SET thc_activation_threshold = 500000;


load tpch;
-- call dbgen(sf = 10);




/* 

MAIN QUERY

*/


EXPLAIN ANALYZE 

with BULK as (
    SELECT n_name, o_orderkey, c_nationkey, 
FROM
    customer,
    orders,
    nation,
    region

WHERE
    c_custkey = o_custkey
    AND c_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= CAST('1994-01-01' AS date)
    AND o_orderdate < CAST('1995-01-01' AS date)

),

PENULTIMATE AS (

SELECT n_name, c_nationkey, l_suppkey, l_extendedprice, l_discount
FROM 
  BULK,
  lineitem
WHERE
    l_orderkey = o_orderkey -- o_ is build side

)

-- EXPLAIN ANALYZE SELECT
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    PENULTIMATE,
    supplier
WHERE
    c_nationkey = s_nationkey
    AND l_suppkey = s_suppkey
GROUP BY
    n_name
ORDER BY
    revenue DESC;