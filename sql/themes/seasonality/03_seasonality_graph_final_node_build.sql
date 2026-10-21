-- =====================================================================================
-- SEASONALITY GRAPH: MODEL PERIODS AND EDGE-BUILD PARAMETERS
-- Spark SQL / Databricks / Kyuubi compatible (Spark 3.2+)
--
-- Purpose
--   1) Create one editable parameter row for the final node/edge build.
--   2) Create a 12-period calendar used by node and edge vectors.
--
-- IMPORTANT
--   The current default is CALENDAR MONTHS (P01 = January ... P12 = December), because
--   the official Tesco commercial-period calendar has not been supplied. Replace only
--   the calendar table with the official date-to-period mapping when it becomes available;
--   the downstream node and edge scripts do not need to change.
-- =====================================================================================

DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_final_params;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_final_params
USING PARQUET AS
SELECT
    DATE '2023-01-01' AS analysis_start_date,
    DATE '2027-01-01' AS analysis_end_date_exclusive,
    'CALENDAR_MONTH_12_PROVISIONAL' AS model_period_scheme,
    CAST(1 AS INT) AS provisional_period_calendar_flag,

    -- Pair support. These are graph sparsification thresholds, not seasonality filters.
    CAST(2 AS BIGINT) AS min_tpnb_pair_baskets_per_period,
    CAST(10 AS BIGINT) AS min_tpnb_pair_baskets_all_years,
    CAST(5 AS BIGINT) AS min_theme_pair_baskets_per_period,
    CAST(20 AS BIGINT) AS min_theme_pair_baskets_all_years,

    -- Numerical safety and display tolerances.
    CAST(0.000001 AS DOUBLE) AS percentage_tolerance,
    CAST(0.000001 AS DOUBLE) AS weight_tolerance,

    -- Initial commercial Theme classes. Candidate and low-information nodes remain in
    -- the product graph, but are not initially presented as confirmed commercial Themes.
    ARRAY('SEASONAL_LED', 'SEASONALITY_PLUS_GENERAL', 'GENERAL_ONLY')
        AS commercial_theme_behavior_array,

    CURRENT_DATE() AS build_date;


DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_model_period_calendar;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_model_period_calendar
USING PARQUET AS
WITH p AS (
    SELECT *
    FROM lab_customer_value_analytics.seasonality_graph_final_params
),
coverage AS (
    SELECT MAX(cal_date) AS max_data_date
    FROM lab_customer_value_analytics.seasonality_v2_calendar_days
),
dates AS (
    SELECT
        c.cal_date,
        YEAR(c.cal_date) AS period_year,
        MONTH(c.cal_date) AS period_number,
        CONCAT(
            CAST(YEAR(c.cal_date) AS STRING),
            '-P',
            LPAD(CAST(MONTH(c.cal_date) AS STRING), 2, '0')
        ) AS period_id,
        TRUNC(c.cal_date, 'MM') AS period_start_date,
        LAST_DAY(c.cal_date) AS period_end_date
    FROM lab_customer_value_analytics.seasonality_v2_calendar_days c
    CROSS JOIN p
    WHERE c.cal_date >= p.analysis_start_date
      AND c.cal_date <  p.analysis_end_date_exclusive
)
SELECT
    d.cal_date,
    d.period_year,
    d.period_number,
    d.period_id,
    d.period_start_date,
    d.period_end_date,
    DATEDIFF(d.period_end_date, d.period_start_date) + 1 AS calendar_days_in_period,
    CASE WHEN d.period_end_date <= c.max_data_date THEN 1 ELSE 0 END AS period_is_complete_flag,
    CASE WHEN d.cal_date <= c.max_data_date THEN 1 ELSE 0 END AS date_is_observed_flag,
    'CALENDAR_MONTH_12_PROVISIONAL' AS period_scheme
FROM dates d
CROSS JOIN coverage c;


-- Period summary: useful for validation and easy replacement with an official calendar.
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_model_period_summary;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_model_period_summary
USING PARQUET AS
SELECT
    period_year,
    period_number,
    period_id,
    MIN(period_start_date) AS period_start_date,
    MAX(period_end_date) AS period_end_date,
    MAX(calendar_days_in_period) AS calendar_days_in_period,
    SUM(date_is_observed_flag) AS observed_days_in_period,
    MAX(period_is_complete_flag) AS period_is_complete_flag,
    MAX(period_scheme) AS period_scheme
FROM lab_customer_value_analytics.seasonality_graph_model_period_calendar
GROUP BY period_year, period_number, period_id;
-- =====================================================================================
-- SEASONALITY GRAPH: FINAL TPNB NODE TABLES
-- Spark SQL / Databricks / Kyuubi compatible (Spark 3.2+)
--
-- Prerequisites
--   00_graph_model_periods_and_params.sql
--   seasonality_graph_features_v2_1_period_vectors_full_build.sql
--      OR the final Graph v2.1 overlay has already completed.
--
-- Outputs
--   seasonality_graph_tpnb_model_period_features  TPNB x year x P01-P12
--   seasonality_graph_tpnb_model_period_vectors   TPNB x year, 12-element arrays
--   seasonality_graph_tpnb_nodes                  FINAL one row per TPNB
--   seasonality_graph_theme_nodes_final           subclass x behaviour
--   seasonality_graph_theme_node_period_features  Theme x year x period
--   seasonality_graph_theme_node_period_vectors   Theme x year, 12-element arrays
--
-- Design notes
--   * Every TPNB remains a graph node.
--   * The commercial Theme key is corrected to never be NULL:
--       COALESCE(subcategory_code, 'UNKNOWN_SUBCLASS') | graph_behavior_class
--   * Raw share values are retained. Consumer-facing shares are clamped to 0-100, and
--     share_out_of_bounds_flag shows whether the source value was outside tolerance.
--   * Model-period features are normalised within the same TPNB and year.
-- =====================================================================================


-- =====================================================================================
-- 1. TPNB x MODEL PERIOD FEATURES
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_tpnb_model_period_features;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_tpnb_model_period_features
USING PARQUET AS
WITH node_scope AS (
    SELECT
        f.tpnb,
        f.graph_behavior_class,
        f.graph_evidence_tier,
        f.seasonal_pattern_flag,
        f.measured_seasonal_uplift_flag,
        f.repeated_time_of_year_pattern_flag,
        f.repeated_peak_season,
        COALESCE(NULLIF(TRIM(f.subcategory_code), ''), 'UNKNOWN_SUBCLASS') AS subcategory_code,
        COALESCE(NULLIF(TRIM(f.subcategory_name), ''), 'Unknown subclass') AS subcategory_name,
        CONCAT(
            COALESCE(NULLIF(TRIM(f.subcategory_code), ''), 'UNKNOWN_SUBCLASS'),
            '|',
            f.graph_behavior_class
        ) AS theme_node_key
    FROM lab_customer_value_analytics.seasonality_graph_v2_tpnb_features f
    WHERE f.graph_node_inclusion_flag = 1
),
market_period AS (
    SELECT
        c.period_year,
        c.period_number,
        c.period_id,
        MIN(c.period_start_date) AS period_start_date,
        MAX(c.period_end_date) AS period_end_date,
        MAX(c.calendar_days_in_period) AS calendar_days_in_period,
        SUM(c.date_is_observed_flag) AS observed_days_in_period,
        MAX(c.period_is_complete_flag) AS period_is_complete_flag,
        SUM(COALESCE(d.baskets, 0)) AS market_baskets,
        SUM(COALESCE(d.items, 0)) AS market_item_slots,
        SUM(COALESCE(d.sales, CAST(0 AS DOUBLE))) AS market_sales
    FROM lab_customer_value_analytics.seasonality_graph_model_period_calendar c
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_daily_fact d
      ON c.cal_date = d.sale_date
     AND d.entity_type = 'ALL'
    GROUP BY c.period_year, c.period_number, c.period_id
),
market_year AS (
    SELECT
        period_year,
        SUM(market_baskets) AS year_market_baskets,
        SUM(market_item_slots) AS year_market_item_slots,
        SUM(market_sales) AS year_market_sales
    FROM market_period
    GROUP BY period_year
),
tpnb_period_actual AS (
    SELECT
        d.entity_id AS tpnb,
        c.period_year,
        c.period_number,
        c.period_id,
        SUM(d.baskets) AS actual_baskets,
        SUM(d.sales) AS actual_sales,
        SUM(d.units) AS actual_units,
        COUNT(DISTINCT d.sale_date) AS active_sale_days
    FROM lab_customer_value_analytics.seasonality_v2_entity_daily d
    JOIN node_scope n
      ON d.entity_id = n.tpnb
    JOIN lab_customer_value_analytics.seasonality_graph_model_period_calendar c
      ON d.sale_date = c.cal_date
    WHERE d.entity_level = 'TPNB'
    GROUP BY d.entity_id, c.period_year, c.period_number, c.period_id
),
-- Exact accepted named-season incremental demand, mapped to model periods.
-- Multiple accepted occasions can overlap on a date; MAX is used per TPNB x date so the
-- same daily demand is not counted more than once.
named_daily_dedup AS (
    SELECT
        t.tpnb,
        t.role_date AS sale_date,
        MAX(GREATEST(t.excess_baskets, CAST(0 AS DOUBLE))) AS named_positive_incremental_baskets,
        MAX(GREATEST(t.incremental_sales, CAST(0 AS DOUBLE))) AS named_positive_incremental_sales,
        MAX(CASE WHEN t.excess_baskets > 0 OR t.incremental_sales > 0 THEN 1 ELSE 0 END)
            AS named_seasonal_day_flag
    FROM lab_customer_value_analytics.seasonality_v2_6_timing_daily t
    GROUP BY t.tpnb, t.role_date
),
named_period AS (
    SELECT
        n.tpnb,
        c.period_year,
        c.period_number,
        SUM(n.named_positive_incremental_baskets) AS named_positive_incremental_baskets,
        SUM(n.named_positive_incremental_sales) AS named_positive_incremental_sales,
        SUM(n.named_seasonal_day_flag) AS named_seasonal_days
    FROM named_daily_dedup n
    JOIN lab_customer_value_analytics.seasonality_graph_model_period_calendar c
      ON n.sale_date = c.cal_date
    GROUP BY n.tpnb, c.period_year, c.period_number
),
joined AS (
    SELECT
        a.tpnb,
        n.subcategory_code,
        n.subcategory_name,
        n.theme_node_key,
        n.graph_behavior_class,
        n.graph_evidence_tier,
        n.seasonal_pattern_flag,
        n.measured_seasonal_uplift_flag,
        n.repeated_time_of_year_pattern_flag,
        n.repeated_peak_season,
        a.period_year,
        a.period_number,
        a.period_id,
        m.period_start_date,
        m.period_end_date,
        m.calendar_days_in_period,
        m.observed_days_in_period,
        m.period_is_complete_flag,
        m.market_baskets,
        m.market_item_slots,
        m.market_sales,
        y.year_market_baskets,
        y.year_market_item_slots,
        y.year_market_sales,
        a.actual_baskets,
        a.actual_sales,
        a.actual_units,
        a.active_sale_days,
        COALESCE(p.named_positive_incremental_baskets, CAST(0 AS DOUBLE))
            AS named_positive_incremental_baskets,
        COALESCE(p.named_positive_incremental_sales, CAST(0 AS DOUBLE))
            AS named_positive_incremental_sales,
        COALESCE(p.named_seasonal_days, 0) AS named_seasonal_days
    FROM tpnb_period_actual a
    JOIN node_scope n
      ON a.tpnb = n.tpnb
    JOIN market_period m
      ON a.period_year = m.period_year
     AND a.period_number = m.period_number
    JOIN market_year y
      ON a.period_year = y.period_year
    LEFT JOIN named_period p
      ON a.tpnb = p.tpnb
     AND a.period_year = p.period_year
     AND a.period_number = p.period_number
),
year_totals AS (
    SELECT
        j.*,
        SUM(j.actual_baskets) OVER (PARTITION BY j.tpnb, j.period_year) AS year_actual_baskets,
        SUM(j.actual_sales) OVER (PARTITION BY j.tpnb, j.period_year) AS year_actual_sales,
        SUM(j.actual_units) OVER (PARTITION BY j.tpnb, j.period_year) AS year_actual_units,
        SUM(j.active_sale_days) OVER (PARTITION BY j.tpnb, j.period_year) AS year_active_sale_days,
        SUM(j.named_positive_incremental_baskets) OVER (PARTITION BY j.tpnb, j.period_year)
            AS year_named_positive_incremental_baskets,
        SUM(j.named_positive_incremental_sales) OVER (PARTITION BY j.tpnb, j.period_year)
            AS year_named_positive_incremental_sales,
        AVG(j.actual_baskets) OVER (PARTITION BY j.tpnb, j.period_year) AS mean_period_baskets,
        STDDEV_POP(j.actual_baskets) OVER (PARTITION BY j.tpnb, j.period_year) AS sd_period_baskets,
        AVG(j.actual_sales) OVER (PARTITION BY j.tpnb, j.period_year) AS mean_period_sales,
        STDDEV_POP(j.actual_sales) OVER (PARTITION BY j.tpnb, j.period_year) AS sd_period_sales
    FROM joined j
),
expected AS (
    SELECT
        y.*,
        CASE
            WHEN y.year_active_sale_days > 0
                THEN y.year_actual_baskets * y.active_sale_days / y.year_active_sale_days
            ELSE CAST(0 AS DOUBLE)
        END AS active_day_expected_baskets,
        CASE
            WHEN y.year_active_sale_days > 0
                THEN y.year_actual_sales * y.active_sale_days / y.year_active_sale_days
            ELSE CAST(0 AS DOUBLE)
        END AS active_day_expected_sales,
        CASE
            WHEN y.market_baskets > 0 THEN y.actual_baskets / y.market_baskets
        END AS period_basket_penetration,
        CASE
            WHEN y.year_market_baskets > 0
                THEN y.year_actual_baskets / y.year_market_baskets
        END AS year_basket_penetration
    FROM year_totals y
),
excess AS (
    SELECT
        e.*,
        GREATEST(e.actual_baskets - e.active_day_expected_baskets, CAST(0 AS DOUBLE))
            AS positive_excess_baskets,
        GREATEST(e.actual_sales - e.active_day_expected_sales, CAST(0 AS DOUBLE))
            AS positive_excess_sales
    FROM expected e
),
scored AS (
    SELECT
        e.*,
        SUM(e.positive_excess_baskets) OVER (PARTITION BY e.tpnb, e.period_year)
            AS year_positive_excess_baskets,
        SUM(e.positive_excess_sales) OVER (PARTITION BY e.tpnb, e.period_year)
            AS year_positive_excess_sales
    FROM excess e
)
SELECT
    tpnb,
    subcategory_code,
    subcategory_name,
    theme_node_key,
    graph_behavior_class,
    graph_evidence_tier,
    period_year,
    period_number,
    period_id,
    period_start_date,
    period_end_date,
    calendar_days_in_period,
    observed_days_in_period,
    period_is_complete_flag,

    CASE WHEN actual_baskets > 0 THEN 1 ELSE 0 END AS sold_flag,
    active_sale_days,
    actual_baskets,
    actual_sales,
    actual_units,
    market_baskets,
    market_item_slots,
    market_sales,

    actual_baskets / NULLIF(year_actual_baskets, 0) AS actual_basket_weight_within_tpnb_year,
    actual_sales / NULLIF(year_actual_sales, 0) AS actual_sales_weight_within_tpnb_year,
    actual_units / NULLIF(year_actual_units, 0) AS actual_units_weight_within_tpnb_year,

    active_day_expected_baskets,
    active_day_expected_sales,
    actual_baskets / NULLIF(active_day_expected_baskets, 0) AS active_day_basket_rate_index,
    actual_sales / NULLIF(active_day_expected_sales, 0) AS active_day_sales_rate_index,
    positive_excess_baskets,
    positive_excess_sales,
    positive_excess_baskets / NULLIF(year_positive_excess_baskets, 0)
        AS positive_excess_basket_weight_within_tpnb_year,
    positive_excess_sales / NULLIF(year_positive_excess_sales, 0)
        AS positive_excess_sales_weight_within_tpnb_year,

    period_basket_penetration,
    year_basket_penetration,
    period_basket_penetration / NULLIF(year_basket_penetration, 0)
        AS period_basket_penetration_index,
    (actual_baskets - mean_period_baskets) / NULLIF(sd_period_baskets, 0) AS period_baskets_zscore,
    (actual_sales - mean_period_sales) / NULLIF(sd_period_sales, 0) AS period_sales_zscore,

    named_positive_incremental_baskets,
    named_positive_incremental_sales,
    named_seasonal_days,
    named_positive_incremental_baskets / NULLIF(year_named_positive_incremental_baskets, 0)
        AS named_seasonality_basket_weight_within_tpnb_year,
    named_positive_incremental_sales / NULLIF(year_named_positive_incremental_sales, 0)
        AS named_seasonality_sales_weight_within_tpnb_year,

    CASE
        WHEN year_named_positive_incremental_sales > 0
            THEN named_positive_incremental_sales / year_named_positive_incremental_sales
        WHEN repeated_time_of_year_pattern_flag = 1 AND year_positive_excess_sales > 0
            THEN positive_excess_sales / year_positive_excess_sales
        ELSE CAST(0 AS DOUBLE)
    END AS period_seasonality_sales_weight,

    CASE
        WHEN year_named_positive_incremental_baskets > 0
            THEN named_positive_incremental_baskets / year_named_positive_incremental_baskets
        WHEN repeated_time_of_year_pattern_flag = 1 AND year_positive_excess_baskets > 0
            THEN positive_excess_baskets / year_positive_excess_baskets
        ELSE CAST(0 AS DOUBLE)
    END AS period_seasonality_basket_weight,

    CASE
        WHEN year_named_positive_incremental_sales > 0 THEN 'ACCEPTED_NAMED_OCCASION_INCREMENTAL'
        WHEN repeated_time_of_year_pattern_flag = 1 THEN 'REPEATED_TIME_OF_YEAR_EXCESS'
        ELSE 'NO_SEASONAL_PERIOD_WEIGHT'
    END AS period_seasonality_weight_source,

    CASE
        WHEN seasonal_pattern_flag = 1
         AND (
                named_positive_incremental_sales > 0
             OR (repeated_time_of_year_pattern_flag = 1 AND positive_excess_sales > 0)
         )
            THEN 1 ELSE 0
    END AS period_seasonal_flag
FROM scored;


-- =====================================================================================
-- 2. TPNB x YEAR: ZERO-FILLED 12-PERIOD VECTORS
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_tpnb_model_period_vectors;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_tpnb_model_period_vectors
USING PARQUET AS
WITH maps AS (
    SELECT
        tpnb,
        subcategory_code,
        subcategory_name,
        theme_node_key,
        graph_behavior_class,
        graph_evidence_tier,
        period_year,
        MAX(period_is_complete_flag) AS year_has_complete_period_flag,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', sold_flag))) AS sold_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', actual_baskets))) AS baskets_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', actual_sales))) AS sales_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', actual_basket_weight_within_tpnb_year))) AS basket_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', actual_sales_weight_within_tpnb_year))) AS sales_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', active_day_basket_rate_index))) AS basket_rate_index_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', active_day_sales_rate_index))) AS sales_rate_index_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', period_basket_penetration_index))) AS penetration_index_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', positive_excess_baskets))) AS excess_baskets_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', positive_excess_sales))) AS excess_sales_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', period_seasonality_basket_weight))) AS seasonality_basket_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', period_seasonality_sales_weight))) AS seasonality_sales_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', period_seasonal_flag))) AS seasonal_flag_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', period_sales_zscore))) AS sales_zscore_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', period_baskets_zscore))) AS baskets_zscore_map
    FROM lab_customer_value_analytics.seasonality_graph_tpnb_model_period_features
    GROUP BY
        tpnb,
        subcategory_code,
        subcategory_name,
        theme_node_key,
        graph_behavior_class,
        graph_evidence_tier,
        period_year
)
SELECT
    tpnb,
    subcategory_code,
    subcategory_name,
    theme_node_key,
    graph_behavior_class,
    graph_evidence_tier,
    period_year,
    year_has_complete_period_flag,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(sold_map, p), 0))
        AS period_sold_flag_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(baskets_map, p), CAST(0 AS DOUBLE)))
        AS period_actual_baskets_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(sales_map, p), CAST(0 AS DOUBLE)))
        AS period_actual_sales_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(basket_weight_map, p), CAST(0 AS DOUBLE)))
        AS period_actual_basket_weight_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(sales_weight_map, p), CAST(0 AS DOUBLE)))
        AS period_actual_sales_weight_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(basket_rate_index_map, p), CAST(0 AS DOUBLE)))
        AS period_basket_rate_index_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(sales_rate_index_map, p), CAST(0 AS DOUBLE)))
        AS period_sales_rate_index_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(penetration_index_map, p), CAST(0 AS DOUBLE)))
        AS period_basket_penetration_index_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(excess_baskets_map, p), CAST(0 AS DOUBLE)))
        AS period_positive_excess_baskets_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(excess_sales_map, p), CAST(0 AS DOUBLE)))
        AS period_positive_excess_sales_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(seasonality_basket_weight_map, p), CAST(0 AS DOUBLE)))
        AS period_seasonality_basket_weight_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(seasonality_sales_weight_map, p), CAST(0 AS DOUBLE)))
        AS period_seasonality_sales_weight_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(seasonal_flag_map, p), 0))
        AS period_seasonal_flag_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(sales_zscore_map, p), CAST(0 AS DOUBLE)))
        AS period_sales_zscore_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(baskets_zscore_map, p), CAST(0 AS DOUBLE)))
        AS period_baskets_zscore_array,
    TRANSFORM(
        SEQUENCE(1, 12),
        p -> NAMED_STRUCT(
            'period_number', p,
            'sold_flag', COALESCE(ELEMENT_AT(sold_map, p), 0),
            'actual_baskets', COALESCE(ELEMENT_AT(baskets_map, p), CAST(0 AS DOUBLE)),
            'actual_sales', COALESCE(ELEMENT_AT(sales_map, p), CAST(0 AS DOUBLE)),
            'actual_basket_weight', COALESCE(ELEMENT_AT(basket_weight_map, p), CAST(0 AS DOUBLE)),
            'actual_sales_weight', COALESCE(ELEMENT_AT(sales_weight_map, p), CAST(0 AS DOUBLE)),
            'basket_rate_index', COALESCE(ELEMENT_AT(basket_rate_index_map, p), CAST(0 AS DOUBLE)),
            'sales_rate_index', COALESCE(ELEMENT_AT(sales_rate_index_map, p), CAST(0 AS DOUBLE)),
            'basket_penetration_index', COALESCE(ELEMENT_AT(penetration_index_map, p), CAST(0 AS DOUBLE)),
            'positive_excess_baskets', COALESCE(ELEMENT_AT(excess_baskets_map, p), CAST(0 AS DOUBLE)),
            'positive_excess_sales', COALESCE(ELEMENT_AT(excess_sales_map, p), CAST(0 AS DOUBLE)),
            'seasonality_basket_weight', COALESCE(ELEMENT_AT(seasonality_basket_weight_map, p), CAST(0 AS DOUBLE)),
            'seasonality_sales_weight', COALESCE(ELEMENT_AT(seasonality_sales_weight_map, p), CAST(0 AS DOUBLE)),
            'seasonal_flag', COALESCE(ELEMENT_AT(seasonal_flag_map, p), 0),
            'sales_zscore', COALESCE(ELEMENT_AT(sales_zscore_map, p), CAST(0 AS DOUBLE)),
            'baskets_zscore', COALESCE(ELEMENT_AT(baskets_zscore_map, p), CAST(0 AS DOUBLE))
        )
    ) AS model_period_feature_array
FROM maps;


-- =====================================================================================
-- 3. FINAL ONE-ROW-PER-TPNB NODE TABLE
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_tpnb_nodes;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_tpnb_nodes
USING PARQUET AS
WITH p AS (
    SELECT *
    FROM lab_customer_value_analytics.seasonality_graph_final_params
),
period_years AS (
    SELECT
        tpnb,
        ARRAY_SORT(COLLECT_LIST(NAMED_STRUCT(
            'period_year', period_year,
            'period_sold_flag_array', period_sold_flag_array,
            'period_actual_baskets_array', period_actual_baskets_array,
            'period_actual_sales_array', period_actual_sales_array,
            'period_actual_basket_weight_array', period_actual_basket_weight_array,
            'period_actual_sales_weight_array', period_actual_sales_weight_array,
            'period_basket_rate_index_array', period_basket_rate_index_array,
            'period_sales_rate_index_array', period_sales_rate_index_array,
            'period_basket_penetration_index_array', period_basket_penetration_index_array,
            'period_positive_excess_baskets_array', period_positive_excess_baskets_array,
            'period_positive_excess_sales_array', period_positive_excess_sales_array,
            'period_seasonality_basket_weight_array', period_seasonality_basket_weight_array,
            'period_seasonality_sales_weight_array', period_seasonality_sales_weight_array,
            'period_seasonal_flag_array', period_seasonal_flag_array,
            'period_sales_zscore_array', period_sales_zscore_array,
            'period_baskets_zscore_array', period_baskets_zscore_array,
            'model_period_feature_array', model_period_feature_array
        ))) AS model_period_year_feature_array
    FROM lab_customer_value_analytics.seasonality_graph_tpnb_model_period_vectors
    GROUP BY tpnb
),
base AS (
    SELECT
        f.*,
        COALESCE(NULLIF(TRIM(f.subcategory_code), ''), 'UNKNOWN_SUBCLASS')
            AS corrected_subcategory_code,
        COALESCE(NULLIF(TRIM(f.subcategory_name), ''), 'Unknown subclass')
            AS corrected_subcategory_name,
        CASE
            WHEN NULLIF(TRIM(f.subcategory_code), '') IS NULL THEN 1 ELSE 0
        END AS missing_subcategory_flag,
        CASE
            WHEN f.seasonal_basket_share_pct < -p.percentage_tolerance
              OR f.seasonal_basket_share_pct > 100.0 + p.percentage_tolerance
              OR f.general_basket_share_pct < -p.percentage_tolerance
              OR f.general_basket_share_pct > 100.0 + p.percentage_tolerance
              OR f.seasonal_sales_share_pct < -p.percentage_tolerance
              OR f.seasonal_sales_share_pct > 100.0 + p.percentage_tolerance
              OR f.general_sales_share_pct < -p.percentage_tolerance
              OR f.general_sales_share_pct > 100.0 + p.percentage_tolerance
                THEN 1 ELSE 0
        END AS share_out_of_bounds_flag,
        y.model_period_year_feature_array
    FROM lab_customer_value_analytics.seasonality_graph_v2_tpnb_features f
    LEFT JOIN period_years y
      ON f.tpnb = y.tpnb
    CROSS JOIN p
)
SELECT
    tpnb,
    product_description,
    hierarchy_department_name,
    hierarchy_section_name,
    hierarchy_class_name,
    corrected_subcategory_code AS subcategory_code,
    corrected_subcategory_name AS subcategory_name,
    CONCAT(corrected_subcategory_code, '|', graph_behavior_class) AS theme_node_key,
    CONCAT(
        corrected_subcategory_name,
        ' - ',
        CASE graph_behavior_class
            WHEN 'SEASONAL_LED' THEN 'Seasonal-led'
            WHEN 'SEASONALITY_PLUS_GENERAL' THEN 'Seasonality plus general'
            WHEN 'GENERAL_ONLY' THEN 'General only'
            WHEN 'SEASONAL_PATTERN_CANDIDATE' THEN 'Seasonal pattern candidate'
            WHEN 'GENERAL_PATTERN_CANDIDATE' THEN 'General pattern candidate'
            WHEN 'LOW_INFORMATION_NODE' THEN 'Low-information node'
            ELSE graph_behavior_class
        END
    ) AS theme_node_display_name,
    missing_subcategory_flag,
    share_out_of_bounds_flag,
    as_of_date,

    graph_behavior_class,
    graph_behavior_reason,
    graph_evidence_tier,
    graph_data_state,
    graph_training_label_flag,
    graph_training_label_weight,
    graph_node_inclusion_flag,
    theme_behavior_flag,
    confirmed_commercial_theme_flag,

    seasonality_flag,
    seasonal_pattern_flag,
    measured_seasonal_uplift_flag,
    seasonal_led_flag,
    general_sales_flag,
    seasonality_plus_general_flag,
    general_only_flag,
    occasion_suppressed_flag,

    season_before_flag,
    buildup_flag,
    pre_event_flag,
    season_event_flag,
    season_after_flag,
    post_event_clearance_flag,
    seasonal_period_only_flag,
    seasonal_timing_profile,

    source_product_seasonality,
    source_seasonality_evidence,
    source_availability_pattern,
    source_insufficient_evidence_flag,
    source_review_required_flag,
    source_held_occasion_count,
    source_held_occasion_list,
    graph_v1_assignment_status,

    first_sale_date,
    last_sale_date,
    observable_history_days,
    observed_sale_span_days,
    days_since_last_sale,

    explicit_seasonal_identity_flag,
    hard_seasonal_identity_flag,
    explicit_identity_count,
    explicit_identity_occasion_list,
    explicit_identity_family_list,
    explicit_identity_season_list,
    matched_identity_patterns,
    identity_timing_alignment,
    occasion_attribution_status,
    occasion_identity_conflict_flag,

    accepted_occasion_count,
    accepted_occasion_list,
    candidate_held_occasion_count,
    candidate_held_occasion_list,
    primary_occasion_key,
    primary_occasion,
    secondary_occasion,
    primary_occasion_weight_pct,
    secondary_occasion_weight_pct,
    named_seasonal_peak_count,
    multi_seasonal_peak_flag,

    usable_complete_years,
    max_years_same_peak_season,
    strongest_observed_peak_season,
    repeated_peak_season,
    strongest_peak_median_basket_share,
    repeated_peak_median_basket_share,
    strongest_peak_median_sales_share,
    repeated_peak_median_sales_share,
    observed_peak_season_list,
    maximum_single_year_peak_share,
    repeated_time_of_year_pattern_flag,
    repeated_time_of_year_seasonal_led_flag,

    shopping_start_days_before,
    shopping_peak_days_before,
    shopping_end_days_before,
    shopping_window_days,
    peak_season,
    time_of_year_pattern,
    best_8_week_share_pct,

    total_baskets,
    total_sales,
    avg_annual_sales,
    active_week_positions,
    active_quarters,

    seasonal_basket_share_pct AS seasonal_basket_share_pct_raw,
    general_basket_share_pct AS general_basket_share_pct_raw,
    seasonal_sales_share_pct AS seasonal_sales_share_pct_raw,
    general_sales_share_pct AS general_sales_share_pct_raw,
    LEAST(100.0, GREATEST(0.0, seasonal_basket_share_pct)) AS seasonal_basket_share_pct,
    LEAST(100.0, GREATEST(0.0, general_basket_share_pct)) AS general_basket_share_pct,
    LEAST(100.0, GREATEST(0.0, seasonal_sales_share_pct)) AS seasonal_sales_share_pct,
    LEAST(100.0, GREATEST(0.0, general_sales_share_pct)) AS general_sales_share_pct,
    seasonal_share_measure_type,

    general_baskets,
    general_sales,
    general_active_week_positions,
    general_active_quarters,
    general_active_days,
    seasonal_incremental_baskets,
    seasonal_incremental_sales,
    source_occasion_dependency_pct,
    max_seasonal_basket_share_index,
    max_seasonal_z_score,

    periodic_high_response_flag,
    periodic_medium_evidence_flag,
    periodic_review_evidence_flag,
    periodic_high_event_count,
    periodic_medium_event_count,
    periodic_high_event_list,
    periodic_medium_event_list,
    periodic_review_event_list,
    periodic_high_incremental_baskets,
    periodic_high_incremental_sales,

    -- Existing occasion-phase vectors from Graph v2.1.
    seasonality_period_id_array AS occasion_phase_period_id_array,
    seasonality_period_occasion_key_array AS occasion_phase_occasion_key_array,
    seasonality_period_year_array AS occasion_phase_year_array,
    seasonality_period_phase_array AS occasion_phase_array,
    seasonality_period_actual_sales_array AS occasion_phase_actual_sales_array,
    seasonality_period_actual_sales_weight_array AS occasion_phase_actual_sales_weight_array,
    seasonality_period_incremental_sales_array AS occasion_phase_incremental_sales_array,
    seasonality_period_incremental_sales_weight_array AS occasion_phase_incremental_sales_weight_array,
    seasonality_period_material_sales_flag_array AS occasion_phase_material_sales_flag_array,
    seasonality_period_measured_uplift_flag_array AS occasion_phase_measured_uplift_flag_array,
    seasonality_period_feature_array AS occasion_phase_feature_array,

    -- Fixed 12-period vectors, one struct per year.
    model_period_year_feature_array,
    'P01_TO_P12_WITHIN_EACH_YEAR' AS model_period_vector_order,
    'CALENDAR_MONTH_12_PROVISIONAL' AS model_period_scheme
FROM base;


-- =====================================================================================
-- 4. FINAL SUBCLASS x BEHAVIOUR THEME NODES
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_theme_nodes_final;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_theme_nodes_final
USING PARQUET AS
SELECT
    theme_node_key,
    MAX(theme_node_display_name) AS theme_node_display_name,
    MAX(subcategory_code) AS subcategory_code,
    MAX(subcategory_name) AS subcategory_name,
    graph_behavior_class,
    MAX(theme_behavior_flag) AS theme_behavior_node_flag,
    CASE
        WHEN graph_behavior_class IN ('SEASONAL_LED', 'SEASONALITY_PLUS_GENERAL', 'GENERAL_ONLY')
         AND SUM(CASE WHEN graph_evidence_tier = 'CONFIRMED' THEN 1 ELSE 0 END) > 0
            THEN 1 ELSE 0
    END AS confirmed_commercial_theme_node_flag,
    COUNT(*) AS tpnb_count,
    SUM(CASE WHEN graph_evidence_tier = 'CONFIRMED' THEN 1 ELSE 0 END) AS confirmed_tpnb_count,
    SUM(CASE WHEN graph_evidence_tier = 'SUPPORTED' THEN 1 ELSE 0 END) AS supported_tpnb_count,
    SUM(CASE WHEN graph_evidence_tier = 'PROVISIONAL' THEN 1 ELSE 0 END) AS provisional_tpnb_count,
    SUM(CASE WHEN graph_evidence_tier = 'LOW_INFORMATION' THEN 1 ELSE 0 END) AS low_information_tpnb_count,
    SUM(graph_training_label_flag) AS training_label_tpnb_count,
    SUM(total_baskets) AS total_baskets,
    SUM(total_sales) AS total_sales,
    SUM(total_baskets * seasonal_basket_share_pct) / NULLIF(SUM(total_baskets), 0)
        AS basket_weighted_seasonal_share_pct,
    SUM(total_baskets * general_basket_share_pct) / NULLIF(SUM(total_baskets), 0)
        AS basket_weighted_general_share_pct,
    AVG(repeated_peak_median_basket_share) AS avg_repeated_peak_median_share,
    AVG(max_seasonal_basket_share_index) AS avg_max_seasonal_basket_share_index,
    AVG(max_seasonal_z_score) AS avg_max_seasonal_z_score,
    100.0 * AVG(CAST(season_before_flag AS DOUBLE)) AS products_with_before_season_pct,
    100.0 * AVG(CAST(season_event_flag AS DOUBLE)) AS products_with_event_season_pct,
    100.0 * AVG(CAST(season_after_flag AS DOUBLE)) AS products_with_after_season_pct,
    100.0 * AVG(CAST(measured_seasonal_uplift_flag AS DOUBLE)) AS products_with_measured_uplift_pct,
    100.0 * AVG(CAST(seasonal_led_flag AS DOUBLE)) AS seasonal_led_product_pct,
    100.0 * AVG(CAST(general_sales_flag AS DOUBLE)) AS general_sales_product_pct,
    100.0 * AVG(CAST(periodic_high_response_flag AS DOUBLE)) AS periodic_high_product_pct,
    MAX(as_of_date) AS as_of_date
FROM lab_customer_value_analytics.seasonality_graph_tpnb_nodes
GROUP BY theme_node_key, graph_behavior_class;


-- =====================================================================================
-- 5. THEME NODE x MODEL PERIOD FEATURES AND VECTORS
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_theme_node_period_features;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_theme_node_period_features
USING PARQUET AS
WITH base AS (
    SELECT
        n.theme_node_key,
        n.subcategory_code,
        n.subcategory_name,
        n.graph_behavior_class,
        p.period_year,
        p.period_number,
        MAX(p.period_id) AS period_id,
        COUNT(DISTINCT p.tpnb) AS sold_tpnb_count,
        SUM(p.actual_baskets) AS theme_actual_baskets,
        SUM(p.actual_sales) AS theme_actual_sales,
        SUM(p.positive_excess_baskets) AS theme_positive_excess_baskets,
        SUM(p.positive_excess_sales) AS theme_positive_excess_sales,
        SUM(p.named_positive_incremental_baskets) AS theme_named_incremental_baskets,
        SUM(p.named_positive_incremental_sales) AS theme_named_incremental_sales,
        SUM(p.actual_sales * p.period_seasonality_sales_weight)
            / NULLIF(SUM(p.actual_sales), 0) AS sales_weighted_product_seasonality_weight,
        AVG(CAST(p.period_seasonal_flag AS DOUBLE)) AS sold_product_seasonal_share,
        MAX(p.period_is_complete_flag) AS period_is_complete_flag
    FROM lab_customer_value_analytics.seasonality_graph_tpnb_nodes n
    JOIN lab_customer_value_analytics.seasonality_graph_tpnb_model_period_features p
      ON n.tpnb = p.tpnb
    GROUP BY
        n.theme_node_key,
        n.subcategory_code,
        n.subcategory_name,
        n.graph_behavior_class,
        p.period_year,
        p.period_number
),
scored AS (
    SELECT
        b.*,
        b.theme_actual_baskets / NULLIF(SUM(b.theme_actual_baskets) OVER (
            PARTITION BY b.theme_node_key, b.period_year), 0)
            AS theme_period_basket_weight,
        b.theme_actual_sales / NULLIF(SUM(b.theme_actual_sales) OVER (
            PARTITION BY b.theme_node_key, b.period_year), 0)
            AS theme_period_sales_weight,
        b.theme_positive_excess_sales / NULLIF(SUM(b.theme_positive_excess_sales) OVER (
            PARTITION BY b.theme_node_key, b.period_year), 0)
            AS theme_period_positive_excess_sales_weight,
        b.theme_named_incremental_sales / NULLIF(SUM(b.theme_named_incremental_sales) OVER (
            PARTITION BY b.theme_node_key, b.period_year), 0)
            AS theme_period_named_incremental_sales_weight
    FROM base b
)
SELECT * FROM scored;


DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_theme_node_period_vectors;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_theme_node_period_vectors
USING PARQUET AS
WITH maps AS (
    SELECT
        theme_node_key,
        MAX(subcategory_code) AS subcategory_code,
        MAX(subcategory_name) AS subcategory_name,
        MAX(graph_behavior_class) AS graph_behavior_class,
        period_year,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', sold_tpnb_count))) AS sold_count_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', theme_actual_baskets))) AS baskets_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', theme_actual_sales))) AS sales_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', theme_period_basket_weight))) AS basket_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', theme_period_sales_weight))) AS sales_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', sales_weighted_product_seasonality_weight))) AS seasonality_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', sold_product_seasonal_share))) AS seasonal_share_map
    FROM lab_customer_value_analytics.seasonality_graph_theme_node_period_features
    GROUP BY theme_node_key, period_year
)
SELECT
    theme_node_key,
    subcategory_code,
    subcategory_name,
    graph_behavior_class,
    period_year,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(sold_count_map, p), 0))
        AS period_sold_tpnb_count_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(baskets_map, p), CAST(0 AS DOUBLE)))
        AS period_theme_baskets_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(sales_map, p), CAST(0 AS DOUBLE)))
        AS period_theme_sales_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(basket_weight_map, p), CAST(0 AS DOUBLE)))
        AS period_theme_basket_weight_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(sales_weight_map, p), CAST(0 AS DOUBLE)))
        AS period_theme_sales_weight_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(seasonality_weight_map, p), CAST(0 AS DOUBLE)))
        AS period_theme_seasonality_weight_array,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(seasonal_share_map, p), CAST(0 AS DOUBLE)))
        AS period_theme_seasonal_product_share_array
FROM maps;
-- =====================================================================================
-- SEASONALITY GRAPH: FINAL TPNB NODES, DATA-QUALITY FIXES AND 12-PERIOD PROFILES
-- Spark SQL / Databricks / Kyuubi compatible (Spark 3.2+)
--
-- Prerequisites
--   10_graph_model_periods_and_params.sql
--   11_graph_tpnb_period_base.sql
--
-- What this fixes
--   1) Missing subclass values never produce a blank Theme key. Products are retained and
--      grouped using the nearest available hierarchy level (class, section, department,
--      or UNKNOWN) with an explicit fallback flag.
--   2) Raw seasonal/general shares are preserved for audit. Consumer-facing shares are
--      clamped to 0-100 and quality flags identify material out-of-range values.
--   3) Adds one stable P01-P12 profile per TPNB, normalised across the full analysis window,
--      as well as the existing year-specific P01-P12 vectors.
--
-- Final outputs
--   seasonality_graph_tpnb_period_profile
--   seasonality_graph_tpnb_period_profile_vectors
--   seasonality_graph_tpnb_share_quality
--   seasonality_graph_tpnb_nodes_final               one row per TPNB
--   seasonality_graph_theme_nodes_final              hierarchy group x behaviour
--   seasonality_graph_theme_node_period_features
--   seasonality_graph_theme_node_period_vectors
-- =====================================================================================


-- =====================================================================================
-- 1. ALL-YEARS P01-P12 PROFILE AT TPNB LEVEL
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_tpnb_period_profile;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_tpnb_period_profile
USING PARQUET AS
WITH aggregated AS (
    SELECT
        tpnb,
        MAX(subcategory_code) AS source_subcategory_code,
        MAX(subcategory_name) AS source_subcategory_name,
        MAX(graph_behavior_class) AS graph_behavior_class,
        MAX(graph_evidence_tier) AS graph_evidence_tier,
        period_number,
        COUNT(DISTINCT period_year) AS observed_year_count,
        SUM(CASE WHEN actual_baskets > 0 THEN 1 ELSE 0 END) AS sold_period_year_count,
        SUM(actual_baskets) AS actual_baskets,
        SUM(actual_sales) AS actual_sales,
        SUM(actual_units) AS actual_units,
        SUM(active_sale_days) AS active_sale_days,
        SUM(market_baskets) AS market_baskets,
        SUM(market_item_slots) AS market_item_slots,
        SUM(market_sales) AS market_sales,
        SUM(positive_excess_baskets) AS positive_excess_baskets,
        SUM(positive_excess_sales) AS positive_excess_sales,
        SUM(named_positive_incremental_baskets) AS named_positive_incremental_baskets,
        SUM(named_positive_incremental_sales) AS named_positive_incremental_sales,
        SUM(named_seasonal_days) AS named_seasonal_days,
        AVG(actual_basket_weight_within_tpnb_year) AS mean_within_year_basket_weight,
        AVG(actual_sales_weight_within_tpnb_year) AS mean_within_year_sales_weight,
        AVG(active_day_basket_rate_index) AS mean_active_day_basket_rate_index,
        AVG(active_day_sales_rate_index) AS mean_active_day_sales_rate_index,
        AVG(period_basket_penetration_index) AS mean_basket_penetration_index,
        AVG(period_baskets_zscore) AS mean_period_baskets_zscore,
        AVG(period_sales_zscore) AS mean_period_sales_zscore,
        MAX(repeated_time_of_year_pattern_flag) AS repeated_time_of_year_pattern_flag
    FROM lab_customer_value_analytics.seasonality_graph_tpnb_model_period_features
    GROUP BY tpnb, period_number
),
totals AS (
    SELECT
        a.*,
        SUM(actual_baskets) OVER (PARTITION BY tpnb) AS all_period_actual_baskets,
        SUM(actual_sales) OVER (PARTITION BY tpnb) AS all_period_actual_sales,
        SUM(actual_units) OVER (PARTITION BY tpnb) AS all_period_actual_units,
        SUM(positive_excess_baskets) OVER (PARTITION BY tpnb) AS all_period_positive_excess_baskets,
        SUM(positive_excess_sales) OVER (PARTITION BY tpnb) AS all_period_positive_excess_sales,
        SUM(named_positive_incremental_baskets) OVER (PARTITION BY tpnb)
            AS all_period_named_incremental_baskets,
        SUM(named_positive_incremental_sales) OVER (PARTITION BY tpnb)
            AS all_period_named_incremental_sales
    FROM aggregated a
)
SELECT
    tpnb,
    source_subcategory_code,
    source_subcategory_name,
    graph_behavior_class,
    graph_evidence_tier,
    period_number,
    observed_year_count,
    sold_period_year_count,
    CASE WHEN actual_baskets > 0 THEN 1 ELSE 0 END AS sold_flag,
    actual_baskets,
    actual_sales,
    actual_units,
    active_sale_days,
    market_baskets,
    market_item_slots,
    market_sales,
    actual_baskets / NULLIF(all_period_actual_baskets, 0) AS actual_basket_weight_within_tpnb,
    actual_sales / NULLIF(all_period_actual_sales, 0) AS actual_sales_weight_within_tpnb,
    actual_units / NULLIF(all_period_actual_units, 0) AS actual_units_weight_within_tpnb,
    positive_excess_baskets,
    positive_excess_sales,
    positive_excess_baskets / NULLIF(all_period_positive_excess_baskets, 0)
        AS positive_excess_basket_weight_within_tpnb,
    positive_excess_sales / NULLIF(all_period_positive_excess_sales, 0)
        AS positive_excess_sales_weight_within_tpnb,
    named_positive_incremental_baskets,
    named_positive_incremental_sales,
    named_seasonal_days,
    named_positive_incremental_baskets / NULLIF(all_period_named_incremental_baskets, 0)
        AS named_incremental_basket_weight_within_tpnb,
    named_positive_incremental_sales / NULLIF(all_period_named_incremental_sales, 0)
        AS named_incremental_sales_weight_within_tpnb,
    CASE
        WHEN all_period_named_incremental_sales > 0
            THEN named_positive_incremental_sales / all_period_named_incremental_sales
        WHEN repeated_time_of_year_pattern_flag = 1 AND all_period_positive_excess_sales > 0
            THEN positive_excess_sales / all_period_positive_excess_sales
        ELSE CAST(0 AS DOUBLE)
    END AS period_seasonality_sales_weight,
    CASE
        WHEN all_period_named_incremental_baskets > 0
            THEN named_positive_incremental_baskets / all_period_named_incremental_baskets
        WHEN repeated_time_of_year_pattern_flag = 1 AND all_period_positive_excess_baskets > 0
            THEN positive_excess_baskets / all_period_positive_excess_baskets
        ELSE CAST(0 AS DOUBLE)
    END AS period_seasonality_basket_weight,
    CASE
        WHEN all_period_named_incremental_sales > 0 THEN 'ACCEPTED_NAMED_OCCASION_INCREMENTAL'
        WHEN repeated_time_of_year_pattern_flag = 1 THEN 'REPEATED_TIME_OF_YEAR_EXCESS'
        ELSE 'NO_SEASONAL_PERIOD_WEIGHT'
    END AS period_seasonality_weight_source,
    CASE
        WHEN all_period_named_incremental_sales > 0 AND named_positive_incremental_sales > 0 THEN 1
        WHEN repeated_time_of_year_pattern_flag = 1 AND positive_excess_sales > 0 THEN 1
        ELSE 0
    END AS period_seasonal_flag,
    mean_within_year_basket_weight,
    mean_within_year_sales_weight,
    mean_active_day_basket_rate_index,
    mean_active_day_sales_rate_index,
    mean_basket_penetration_index,
    mean_period_baskets_zscore,
    mean_period_sales_zscore
FROM totals;


-- =====================================================================================
-- 2. ZERO-FILLED ALL-YEARS 12-PERIOD VECTOR PER TPNB
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_tpnb_period_profile_vectors;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_tpnb_period_profile_vectors
USING PARQUET AS
WITH maps AS (
    SELECT
        tpnb,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', sold_flag))) AS sold_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', observed_year_count))) AS observed_year_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', sold_period_year_count))) AS sold_year_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', actual_baskets))) AS baskets_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', actual_sales))) AS sales_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', actual_basket_weight_within_tpnb))) AS basket_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', actual_sales_weight_within_tpnb))) AS sales_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', positive_excess_baskets))) AS excess_baskets_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', positive_excess_sales))) AS excess_sales_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', positive_excess_basket_weight_within_tpnb))) AS excess_basket_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', positive_excess_sales_weight_within_tpnb))) AS excess_sales_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', named_positive_incremental_baskets))) AS named_baskets_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', named_positive_incremental_sales))) AS named_sales_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', named_incremental_basket_weight_within_tpnb))) AS named_basket_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', named_incremental_sales_weight_within_tpnb))) AS named_sales_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', period_seasonality_basket_weight))) AS seasonality_basket_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', period_seasonality_sales_weight))) AS seasonality_sales_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', period_seasonal_flag))) AS seasonal_flag_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', mean_active_day_basket_rate_index))) AS basket_rate_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', mean_active_day_sales_rate_index))) AS sales_rate_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', mean_basket_penetration_index))) AS penetration_map
    FROM lab_customer_value_analytics.seasonality_graph_tpnb_period_profile
    GROUP BY tpnb
)
SELECT
    tpnb,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(sold_map, p), 0)) AS period_sold_flag_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(observed_year_map, p), 0)) AS period_observed_year_count_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(sold_year_map, p), 0)) AS period_sold_year_count_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(baskets_map, p), CAST(0 AS DOUBLE))) AS period_actual_baskets_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(sales_map, p), CAST(0 AS DOUBLE))) AS period_actual_sales_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(basket_weight_map, p), CAST(0 AS DOUBLE))) AS period_actual_basket_weight_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(sales_weight_map, p), CAST(0 AS DOUBLE))) AS period_actual_sales_weight_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(excess_baskets_map, p), CAST(0 AS DOUBLE))) AS period_positive_excess_baskets_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(excess_sales_map, p), CAST(0 AS DOUBLE))) AS period_positive_excess_sales_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(excess_basket_weight_map, p), CAST(0 AS DOUBLE))) AS period_positive_excess_basket_weight_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(excess_sales_weight_map, p), CAST(0 AS DOUBLE))) AS period_positive_excess_sales_weight_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(named_baskets_map, p), CAST(0 AS DOUBLE))) AS period_named_incremental_baskets_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(named_sales_map, p), CAST(0 AS DOUBLE))) AS period_named_incremental_sales_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(named_basket_weight_map, p), CAST(0 AS DOUBLE))) AS period_named_incremental_basket_weight_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(named_sales_weight_map, p), CAST(0 AS DOUBLE))) AS period_named_incremental_sales_weight_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(seasonality_basket_weight_map, p), CAST(0 AS DOUBLE))) AS period_seasonality_basket_weight_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(seasonality_sales_weight_map, p), CAST(0 AS DOUBLE))) AS period_seasonality_sales_weight_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(seasonal_flag_map, p), 0)) AS period_seasonal_flag_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(basket_rate_map, p), CAST(0 AS DOUBLE))) AS period_basket_rate_index_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(sales_rate_map, p), CAST(0 AS DOUBLE))) AS period_sales_rate_index_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(penetration_map, p), CAST(0 AS DOUBLE))) AS period_basket_penetration_index_array_12,
    TRANSFORM(
        SEQUENCE(1, 12),
        p -> NAMED_STRUCT(
            'period_number', p,
            'sold_flag', COALESCE(ELEMENT_AT(sold_map, p), 0),
            'observed_year_count', COALESCE(ELEMENT_AT(observed_year_map, p), 0),
            'sold_year_count', COALESCE(ELEMENT_AT(sold_year_map, p), 0),
            'actual_baskets', COALESCE(ELEMENT_AT(baskets_map, p), CAST(0 AS DOUBLE)),
            'actual_sales', COALESCE(ELEMENT_AT(sales_map, p), CAST(0 AS DOUBLE)),
            'actual_basket_weight', COALESCE(ELEMENT_AT(basket_weight_map, p), CAST(0 AS DOUBLE)),
            'actual_sales_weight', COALESCE(ELEMENT_AT(sales_weight_map, p), CAST(0 AS DOUBLE)),
            'positive_excess_baskets', COALESCE(ELEMENT_AT(excess_baskets_map, p), CAST(0 AS DOUBLE)),
            'positive_excess_sales', COALESCE(ELEMENT_AT(excess_sales_map, p), CAST(0 AS DOUBLE)),
            'positive_excess_basket_weight', COALESCE(ELEMENT_AT(excess_basket_weight_map, p), CAST(0 AS DOUBLE)),
            'positive_excess_sales_weight', COALESCE(ELEMENT_AT(excess_sales_weight_map, p), CAST(0 AS DOUBLE)),
            'named_incremental_baskets', COALESCE(ELEMENT_AT(named_baskets_map, p), CAST(0 AS DOUBLE)),
            'named_incremental_sales', COALESCE(ELEMENT_AT(named_sales_map, p), CAST(0 AS DOUBLE)),
            'named_incremental_basket_weight', COALESCE(ELEMENT_AT(named_basket_weight_map, p), CAST(0 AS DOUBLE)),
            'named_incremental_sales_weight', COALESCE(ELEMENT_AT(named_sales_weight_map, p), CAST(0 AS DOUBLE)),
            'seasonality_basket_weight', COALESCE(ELEMENT_AT(seasonality_basket_weight_map, p), CAST(0 AS DOUBLE)),
            'seasonality_sales_weight', COALESCE(ELEMENT_AT(seasonality_sales_weight_map, p), CAST(0 AS DOUBLE)),
            'seasonal_flag', COALESCE(ELEMENT_AT(seasonal_flag_map, p), 0),
            'basket_rate_index', COALESCE(ELEMENT_AT(basket_rate_map, p), CAST(0 AS DOUBLE)),
            'sales_rate_index', COALESCE(ELEMENT_AT(sales_rate_map, p), CAST(0 AS DOUBLE)),
            'basket_penetration_index', COALESCE(ELEMENT_AT(penetration_map, p), CAST(0 AS DOUBLE))
        )
    ) AS model_period_feature_array_12
FROM maps;


-- =====================================================================================
-- 3. SHARE-QUALITY AUDIT
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_tpnb_share_quality;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_tpnb_share_quality
USING PARQUET AS
WITH p AS (
    SELECT *
    FROM lab_customer_value_analytics.seasonality_graph_final_params
)
SELECT
    n.tpnb,
    n.product_description,
    n.subcategory_code AS source_subcategory_code,
    n.subcategory_name AS source_subcategory_name,
    n.graph_behavior_class,
    n.graph_evidence_tier,
    n.seasonal_basket_share_pct_raw,
    n.general_basket_share_pct_raw,
    n.seasonal_sales_share_pct_raw,
    n.general_sales_share_pct_raw,
    CASE
        WHEN n.seasonal_basket_share_pct_raw < -p.percentage_tolerance
          OR n.seasonal_basket_share_pct_raw > 100.0 + p.percentage_tolerance
          OR n.general_basket_share_pct_raw < -p.percentage_tolerance
          OR n.general_basket_share_pct_raw > 100.0 + p.percentage_tolerance
          OR n.seasonal_sales_share_pct_raw < -p.percentage_tolerance
          OR n.seasonal_sales_share_pct_raw > 100.0 + p.percentage_tolerance
          OR n.general_sales_share_pct_raw < -p.percentage_tolerance
          OR n.general_sales_share_pct_raw > 100.0 + p.percentage_tolerance
            THEN 1 ELSE 0
    END AS material_share_out_of_bounds_flag,
    CASE
        WHEN n.seasonal_basket_share_pct_raw < 0 OR n.seasonal_basket_share_pct_raw > 100
          OR n.general_basket_share_pct_raw < 0 OR n.general_basket_share_pct_raw > 100
          OR n.seasonal_sales_share_pct_raw < 0 OR n.seasonal_sales_share_pct_raw > 100
          OR n.general_sales_share_pct_raw < 0 OR n.general_sales_share_pct_raw > 100
            THEN 1 ELSE 0
    END AS any_share_out_of_bounds_flag
FROM lab_customer_value_analytics.seasonality_graph_tpnb_nodes n
CROSS JOIN p
WHERE n.seasonal_basket_share_pct_raw < 0 OR n.seasonal_basket_share_pct_raw > 100
   OR n.general_basket_share_pct_raw < 0 OR n.general_basket_share_pct_raw > 100
   OR n.seasonal_sales_share_pct_raw < 0 OR n.seasonal_sales_share_pct_raw > 100
   OR n.general_sales_share_pct_raw < 0 OR n.general_sales_share_pct_raw > 100;


-- =====================================================================================
-- 4. FINAL ONE-ROW-PER-TPNB NODE TABLE
--
-- The original Graph v2.1 columns are retained with o.*. New columns beginning with
-- final_ or effective_ are the fields to use for the graph and Theme membership.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_tpnb_nodes_final;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_tpnb_nodes_final
USING PARQUET AS
WITH hierarchy AS (
    SELECT
        o.*,
        CASE
            WHEN COALESCE(o.missing_subcategory_flag, 0) = 0
             AND NULLIF(TRIM(o.subcategory_code), '') IS NOT NULL
                THEN TRIM(o.subcategory_code)
            WHEN NULLIF(TRIM(o.hierarchy_class_name), '') IS NOT NULL THEN CONCAT(
                'CLASS_FALLBACK:',
                REGEXP_REPLACE(UPPER(TRIM(o.hierarchy_class_name)), '[^A-Z0-9]+', '_')
            )
            WHEN NULLIF(TRIM(o.hierarchy_section_name), '') IS NOT NULL THEN CONCAT(
                'SECTION_FALLBACK:',
                REGEXP_REPLACE(UPPER(TRIM(o.hierarchy_section_name)), '[^A-Z0-9]+', '_')
            )
            WHEN NULLIF(TRIM(o.hierarchy_department_name), '') IS NOT NULL THEN CONCAT(
                'DEPARTMENT_FALLBACK:',
                REGEXP_REPLACE(UPPER(TRIM(o.hierarchy_department_name)), '[^A-Z0-9]+', '_')
            )
            ELSE 'UNKNOWN_SUBCLASS'
        END AS effective_subcategory_code,
        CASE
            WHEN COALESCE(o.missing_subcategory_flag, 0) = 0
             AND NULLIF(TRIM(o.subcategory_name), '') IS NOT NULL
                THEN TRIM(o.subcategory_name)
            WHEN NULLIF(TRIM(o.hierarchy_class_name), '') IS NOT NULL THEN CONCAT('[Class fallback] ', TRIM(o.hierarchy_class_name))
            WHEN NULLIF(TRIM(o.hierarchy_section_name), '') IS NOT NULL THEN CONCAT('[Section fallback] ', TRIM(o.hierarchy_section_name))
            WHEN NULLIF(TRIM(o.hierarchy_department_name), '') IS NOT NULL THEN CONCAT('[Department fallback] ', TRIM(o.hierarchy_department_name))
            ELSE 'Unknown subclass'
        END AS effective_subcategory_name,
        CASE
            WHEN COALESCE(o.missing_subcategory_flag, 0) = 0
             AND NULLIF(TRIM(o.subcategory_code), '') IS NOT NULL
                THEN 'SUBCLASS'
            WHEN NULLIF(TRIM(o.hierarchy_class_name), '') IS NOT NULL THEN 'CLASS_FALLBACK'
            WHEN NULLIF(TRIM(o.hierarchy_section_name), '') IS NOT NULL THEN 'SECTION_FALLBACK'
            WHEN NULLIF(TRIM(o.hierarchy_department_name), '') IS NOT NULL THEN 'DEPARTMENT_FALLBACK'
            ELSE 'UNKNOWN'
        END AS theme_hierarchy_level,
        CAST(COALESCE(o.missing_subcategory_flag, 0) AS INT) AS missing_subclass_flag
    FROM lab_customer_value_analytics.seasonality_graph_tpnb_nodes o
),
finalised AS (
    SELECT
        h.*,
        CONCAT(h.effective_subcategory_code, '|', h.graph_behavior_class) AS final_theme_node_key,
        CONCAT(
            h.effective_subcategory_name,
            ' - ',
            CASE h.graph_behavior_class
                WHEN 'SEASONAL_LED' THEN 'Seasonal-led'
                WHEN 'SEASONALITY_PLUS_GENERAL' THEN 'Seasonality plus general'
                WHEN 'GENERAL_ONLY' THEN 'General only'
                WHEN 'SEASONAL_PATTERN_CANDIDATE' THEN 'Seasonal pattern candidate'
                WHEN 'GENERAL_PATTERN_CANDIDATE' THEN 'General pattern candidate'
                WHEN 'LOW_INFORMATION_NODE' THEN 'Low-information node'
                ELSE h.graph_behavior_class
            END
        ) AS final_theme_node_display_name,
        CASE
            WHEN h.theme_hierarchy_level = 'SUBCLASS'
             AND h.graph_behavior_class IN ('SEASONAL_LED', 'SEASONALITY_PLUS_GENERAL', 'GENERAL_ONLY')
                THEN 1 ELSE 0
        END AS commercial_theme_eligible_flag,
        CASE
            WHEN h.theme_hierarchy_level = 'SUBCLASS'
             AND h.confirmed_commercial_theme_flag = 1
                THEN 1 ELSE 0
        END AS confirmed_commercial_theme_eligible_flag,
        CASE
            WHEN h.seasonal_basket_share_pct_raw < 0 OR h.seasonal_basket_share_pct_raw > 100
              OR h.general_basket_share_pct_raw < 0 OR h.general_basket_share_pct_raw > 100
              OR h.seasonal_sales_share_pct_raw < 0 OR h.seasonal_sales_share_pct_raw > 100
              OR h.general_sales_share_pct_raw < 0 OR h.general_sales_share_pct_raw > 100
                THEN 1 ELSE 0
        END AS share_clamped_flag,
        LEAST(100.0, GREATEST(0.0, h.seasonal_basket_share_pct_raw))
            AS final_seasonal_basket_share_pct,
        LEAST(100.0, GREATEST(0.0, h.general_basket_share_pct_raw))
            AS final_general_basket_share_pct,
        LEAST(100.0, GREATEST(0.0, h.seasonal_sales_share_pct_raw))
            AS final_seasonal_sales_share_pct,
        LEAST(100.0, GREATEST(0.0, h.general_sales_share_pct_raw))
            AS final_general_sales_share_pct
    FROM hierarchy h
)
SELECT
    f.*,
    v.period_sold_flag_array_12,
    v.period_observed_year_count_array_12,
    v.period_sold_year_count_array_12,
    v.period_actual_baskets_array_12,
    v.period_actual_sales_array_12,
    v.period_actual_basket_weight_array_12,
    v.period_actual_sales_weight_array_12,
    v.period_positive_excess_baskets_array_12,
    v.period_positive_excess_sales_array_12,
    v.period_positive_excess_basket_weight_array_12,
    v.period_positive_excess_sales_weight_array_12,
    v.period_named_incremental_baskets_array_12,
    v.period_named_incremental_sales_array_12,
    v.period_named_incremental_basket_weight_array_12,
    v.period_named_incremental_sales_weight_array_12,
    v.period_seasonality_basket_weight_array_12,
    v.period_seasonality_sales_weight_array_12,
    v.period_seasonal_flag_array_12,
    v.period_basket_rate_index_array_12,
    v.period_sales_rate_index_array_12,
    v.period_basket_penetration_index_array_12,
    v.model_period_feature_array_12,
    'P01_TO_P12_ALL_OBSERVED_YEARS' AS final_period_vector_order,
    'WITHIN_TPNB_FULL_ANALYSIS_WINDOW' AS final_period_normalisation_scope
FROM finalised f
LEFT JOIN lab_customer_value_analytics.seasonality_graph_tpnb_period_profile_vectors v
  ON f.tpnb = v.tpnb;


-- =====================================================================================
-- 5. FINAL THEME NODES
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_theme_nodes_final;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_theme_nodes_final
USING PARQUET AS
SELECT
    final_theme_node_key AS theme_node_key,
    MAX(final_theme_node_display_name) AS theme_node_display_name,
    MAX(effective_subcategory_code) AS subcategory_code,
    MAX(effective_subcategory_name) AS subcategory_name,
    MAX(theme_hierarchy_level) AS theme_hierarchy_level,
    MAX(missing_subclass_flag) AS missing_subclass_flag,
    graph_behavior_class,
    MAX(theme_behavior_flag) AS theme_behavior_node_flag,
    MAX(commercial_theme_eligible_flag) AS commercial_theme_eligible_flag,
    MAX(confirmed_commercial_theme_eligible_flag) AS confirmed_commercial_theme_node_flag,
    COUNT(*) AS tpnb_count,
    SUM(CASE WHEN graph_evidence_tier = 'CONFIRMED' THEN 1 ELSE 0 END) AS confirmed_tpnb_count,
    SUM(CASE WHEN graph_evidence_tier = 'SUPPORTED' THEN 1 ELSE 0 END) AS supported_tpnb_count,
    SUM(CASE WHEN graph_evidence_tier = 'PROVISIONAL' THEN 1 ELSE 0 END) AS provisional_tpnb_count,
    SUM(CASE WHEN graph_evidence_tier = 'LOW_INFORMATION' THEN 1 ELSE 0 END) AS low_information_tpnb_count,
    SUM(graph_training_label_flag) AS training_label_tpnb_count,
    100.0 * SUM(CASE WHEN graph_evidence_tier = 'CONFIRMED' THEN 1 ELSE 0 END) / COUNT(*)
        AS confirmed_tpnb_share_pct,
    100.0 * SUM(CASE WHEN graph_evidence_tier = 'SUPPORTED' THEN 1 ELSE 0 END) / COUNT(*)
        AS supported_tpnb_share_pct,
    SUM(total_baskets) AS total_baskets,
    SUM(total_sales) AS total_sales,
    SUM(total_baskets * final_seasonal_basket_share_pct) / NULLIF(SUM(total_baskets), 0)
        AS basket_weighted_seasonal_share_pct,
    SUM(total_baskets * final_general_basket_share_pct) / NULLIF(SUM(total_baskets), 0)
        AS basket_weighted_general_share_pct,
    AVG(repeated_peak_median_basket_share) AS avg_repeated_peak_median_share,
    AVG(max_seasonal_basket_share_index) AS avg_max_seasonal_basket_share_index,
    AVG(max_seasonal_z_score) AS avg_max_seasonal_z_score,
    100.0 * AVG(CAST(season_before_flag AS DOUBLE)) AS products_with_before_season_pct,
    100.0 * AVG(CAST(season_event_flag AS DOUBLE)) AS products_with_event_season_pct,
    100.0 * AVG(CAST(season_after_flag AS DOUBLE)) AS products_with_after_season_pct,
    100.0 * AVG(CAST(measured_seasonal_uplift_flag AS DOUBLE)) AS products_with_measured_uplift_pct,
    100.0 * AVG(CAST(seasonal_led_flag AS DOUBLE)) AS seasonal_led_product_pct,
    100.0 * AVG(CAST(general_sales_flag AS DOUBLE)) AS general_sales_product_pct,
    100.0 * AVG(CAST(periodic_high_response_flag AS DOUBLE)) AS periodic_high_product_pct,
    MAX(as_of_date) AS as_of_date
FROM lab_customer_value_analytics.seasonality_graph_tpnb_nodes_final
GROUP BY final_theme_node_key, graph_behavior_class;


-- =====================================================================================
-- 6. THEME NODE x P01-P12 FEATURES AND VECTORS (ALL YEARS)
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_theme_node_period_features;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_theme_node_period_features
USING PARQUET AS
WITH base AS (
    SELECT
        n.final_theme_node_key AS theme_node_key,
        MAX(n.effective_subcategory_code) AS subcategory_code,
        MAX(n.effective_subcategory_name) AS subcategory_name,
        MAX(n.theme_hierarchy_level) AS theme_hierarchy_level,
        MAX(n.graph_behavior_class) AS graph_behavior_class,
        p.period_number,
        COUNT(DISTINCT p.tpnb) AS sold_tpnb_count,
        SUM(p.actual_baskets) AS theme_actual_baskets,
        SUM(p.actual_sales) AS theme_actual_sales,
        SUM(p.positive_excess_baskets) AS theme_positive_excess_baskets,
        SUM(p.positive_excess_sales) AS theme_positive_excess_sales,
        SUM(p.named_positive_incremental_baskets) AS theme_named_incremental_baskets,
        SUM(p.named_positive_incremental_sales) AS theme_named_incremental_sales,
        SUM(p.actual_sales * p.period_seasonality_sales_weight) / NULLIF(SUM(p.actual_sales), 0)
            AS sales_weighted_product_seasonality_weight,
        AVG(CAST(p.period_seasonal_flag AS DOUBLE)) AS sold_product_seasonal_share
    FROM lab_customer_value_analytics.seasonality_graph_tpnb_nodes_final n
    JOIN lab_customer_value_analytics.seasonality_graph_tpnb_period_profile p
      ON n.tpnb = p.tpnb
    GROUP BY n.final_theme_node_key, p.period_number
),
scored AS (
    SELECT
        b.*,
        b.theme_actual_baskets / NULLIF(SUM(b.theme_actual_baskets) OVER (
            PARTITION BY b.theme_node_key), 0) AS theme_period_basket_weight,
        b.theme_actual_sales / NULLIF(SUM(b.theme_actual_sales) OVER (
            PARTITION BY b.theme_node_key), 0) AS theme_period_sales_weight,
        b.theme_positive_excess_sales / NULLIF(SUM(b.theme_positive_excess_sales) OVER (
            PARTITION BY b.theme_node_key), 0) AS theme_period_positive_excess_sales_weight,
        b.theme_named_incremental_sales / NULLIF(SUM(b.theme_named_incremental_sales) OVER (
            PARTITION BY b.theme_node_key), 0) AS theme_period_named_incremental_sales_weight
    FROM base b
)
SELECT * FROM scored;


DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_theme_node_period_vectors;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_theme_node_period_vectors
USING PARQUET AS
WITH maps AS (
    SELECT
        theme_node_key,
        MAX(subcategory_code) AS subcategory_code,
        MAX(subcategory_name) AS subcategory_name,
        MAX(theme_hierarchy_level) AS theme_hierarchy_level,
        MAX(graph_behavior_class) AS graph_behavior_class,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', sold_tpnb_count))) AS sold_count_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', theme_actual_baskets))) AS baskets_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', theme_actual_sales))) AS sales_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', theme_period_basket_weight))) AS basket_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', theme_period_sales_weight))) AS sales_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', theme_period_positive_excess_sales_weight))) AS excess_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', theme_period_named_incremental_sales_weight))) AS named_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', sales_weighted_product_seasonality_weight))) AS seasonality_weight_map,
        MAP_FROM_ENTRIES(COLLECT_LIST(NAMED_STRUCT('key', period_number, 'value', sold_product_seasonal_share))) AS seasonal_share_map
    FROM lab_customer_value_analytics.seasonality_graph_theme_node_period_features
    GROUP BY theme_node_key
)
SELECT
    theme_node_key,
    subcategory_code,
    subcategory_name,
    theme_hierarchy_level,
    graph_behavior_class,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(sold_count_map, p), 0)) AS period_sold_tpnb_count_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(baskets_map, p), CAST(0 AS DOUBLE))) AS period_theme_baskets_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(sales_map, p), CAST(0 AS DOUBLE))) AS period_theme_sales_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(basket_weight_map, p), CAST(0 AS DOUBLE))) AS period_theme_basket_weight_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(sales_weight_map, p), CAST(0 AS DOUBLE))) AS period_theme_sales_weight_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(excess_weight_map, p), CAST(0 AS DOUBLE))) AS period_theme_positive_excess_sales_weight_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(named_weight_map, p), CAST(0 AS DOUBLE))) AS period_theme_named_incremental_sales_weight_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(seasonality_weight_map, p), CAST(0 AS DOUBLE))) AS period_theme_seasonality_weight_array_12,
    TRANSFORM(SEQUENCE(1, 12), p -> COALESCE(ELEMENT_AT(seasonal_share_map, p), CAST(0 AS DOUBLE))) AS period_theme_seasonal_product_share_array_12
FROM maps;
