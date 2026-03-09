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
SET disable_rpt = true;
SET disable_tiered_hash_cache = true;
------------------------------------------

SET threads = 4;
SET pin_threads = 'on';
SET thc_l3_budget = 4194304;
SET thc_collect_phase_rows = 100000; 
SET thc_collect_budget_fraction = 0.02; 
SET thc_miss_threshold = 0.05; 
SET thc_activation_threshold = 500000;

load tpch;
-- call dbgen(sf = 10);


-- -- 1) Number of rows in each table from the existing FROM statement
-- SELECT 'customer' AS table_name, COUNT(*) AS row_count FROM customer
-- UNION ALL
-- SELECT 'orders'   AS table_name, COUNT(*) AS row_count FROM orders
-- UNION ALL
-- SELECT 'lineitem' AS table_name, COUNT(*) AS row_count FROM lineitem
-- UNION ALL
-- SELECT 'supplier' AS table_name, COUNT(*) AS row_count FROM supplier
-- UNION ALL
-- SELECT 'nation'   AS table_name, COUNT(*) AS row_count FROM nation
-- UNION ALL
-- SELECT 'region'   AS table_name, COUNT(*) AS row_count FROM region
-- ORDER BY table_name;

-- -- 2) Number of unique o_orderkey values
-- SELECT
--     COUNT(DISTINCT o_orderkey) AS unique_o_orderkey_count
-- FROM orders;

-- -- 3) 25th/50th/75th percentile of per-order lineitem multiplicity
-- WITH per_order_lineitem_fanout AS (
--     SELECT
--         o.o_orderkey,
--         COUNT(l.l_orderkey)::DOUBLE AS lineitems_for_this_order
--     FROM orders o
--     LEFT JOIN lineitem l
--         ON l.l_orderkey = o.o_orderkey
--     GROUP BY o.o_orderkey
-- )
-- SELECT
--     quantile_cont(lineitems_for_this_order, 0.25) AS p25_lineitems_per_order,
--     quantile_cont(lineitems_for_this_order, 0.50) AS p50_lineitems_per_order,
--     quantile_cont(lineitems_for_this_order, 0.75) AS p75_lineitems_per_order
-- FROM per_order_lineitem_fanout;



-- 1st big join: everything w/ lineitem on l_orderkey = o_orderkey
-- 2nd big join: everything w/ supplier on c_nationkey = s_nationkey, l_suppkey = s_suppkey


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

)


-- EXPLAIN ANALYZE SELECT
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    BULK,
    lineitem,
    supplier
WHERE
    l_orderkey = o_orderkey -- o_ is build side
    AND c_nationkey = s_nationkey
    AND l_suppkey = s_suppkey
GROUP BY
    n_name
ORDER BY
    revenue DESC;