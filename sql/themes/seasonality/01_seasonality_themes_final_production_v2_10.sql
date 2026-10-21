-- =====================================================================================
-- SEASONALITY THEMES - FINAL PRODUCTION BUILD
-- Annual policy: v2.6 final annual attribution and timing/theme output
-- Periodic policy: v2.10 final observed occurrence-response confidence
-- Spark SQL / Databricks / Kyuubi compatible
--
-- FINAL CANONICAL OUTPUTS
--   Annual one-row-per-TPNB themes:
--     lab_customer_value_analytics.seasonality_v2_6_theme_output
--   Annual product x occasion evidence:
--     lab_customer_value_analytics.seasonality_v2_6_product_occasion_profile
--   Periodic product x event x occurrence confidence:
--     lab_customer_value_analytics.seasonality_v2_10_periodic_confidence
--   Periodic event summary:
--     lab_customer_value_analytics.seasonality_v2_10_periodic_event_summary
--
-- IMPORTANT INTERPRETATION
--   * Annual output is recurring seasonality.
--   * Periodic output is an observed, matched-calendar and trend-adjusted occurrence
--     response. It is not recurring seasonality and does not prove causality.
--   * Promotion dates are inferred from visible average-price reductions, not the
--     certified Tesco promotion/campaign calendar.
--   * Range start is inferred from observed sales spells, not a certified ranging table.
--
-- RUNNING THE FILE
--   Run all statements in order. The build is restartable because each output table is
--   dropped and recreated. The only raw invoice-line read is in the base daily-fact step.
-- =====================================================================================

-- =====================================================================================
-- PART A - BASE ANALYTICAL PIPELINE THROUGH VALIDATION LABELS (v2.1, real-data patched)
-- =====================================================================================
-- =====================================================================================
-- SEASONALITY THEMES v2.1
-- Spark SQL for Databricks / Kyuubi (Spark 3.2+). Uses no QUALIFY, no SELECT * EXCEPT and
-- no TRY_ELEMENT_AT. Tested on Spark 3.5 and 4.0, with ANSI mode on and off.
--
-- In one line: for each product (TPNB) and subclass, and for each occasion, measure whether
-- the product takes a bigger share of basket items around the occasion than in comparable
-- weeks, consistently across years, and roll that up into theme flags. v2.1 adds
-- data-driven build-up windows, when customers actually shop for each occasion against
-- when we land ranges and start promotions, occasion weights, and a final theme output.
--
-- Method and formulas: doc "Seasonality Themes v2 - Method and Query Design".
--
-- Run the statements in order. Each creates one table in lab_customer_value_analytics.
--    0  seasonality_v2_params               tunable thresholds (edit here)
--    1  seasonality_v2_daily_fact           the ONLY read of sales.invoice_line
--    2  seasonality_v2_product_dim          one product master row per sold TPNB
--    3  seasonality_v2_occasion_config      family, windows, build-up search, recurrence (edit here)
--    4  seasonality_v2_event_calendar       one dated row per occasion x year
--    5  seasonality_v2_calendar_days        every day in the data, with a low-traffic flag
--    5b seasonality_v2_buildup_calibration  when shopping for each occasion really starts
--    6  seasonality_v2_occurrences          window and baseline dates per occasion x year,
--                                           plus a build-up window where 5b finds one
--    7  seasonality_v2_date_roles           the role each date plays for each occurrence
--    8  seasonality_v2_occasion_windows     calendar-side totals per occurrence
--    9  seasonality_v2_entity_daily         day x entity (entity = TPNB or subclass)
--   10  seasonality_v2_availability_spells  runs of days each entity was on sale
--   11  seasonality_v2_occasion_metrics     entity x occasion x year: tests and verdicts
--   12  seasonality_v2_weekly_profile       entity x week of year
--   13  seasonality_v2_entity_profile       entity: time of year, day of week, availability
--   14  seasonality_v2_tpnb_summary         one row per TPNB with every flag
--   15  seasonality_v2_validation_labels    seed list of known seasonal and everyday products
--   16  scoring queries (read-only)
--   17  seasonality_v2_promo_periods        promotion periods inferred from price
--   18  seasonality_v2_timing_daily         product x occasion x year x day: demand vs baseline
--   19  seasonality_v2_product_timing       product x occasion x year: shopping window, range
--                                           landing, promotion start
--   20  seasonality_v2_occasion_daily_curve occasion x year x day: demand and promotion curves
--   21  seasonality_v2_occasion_timing      occasion x year: customers' timing vs ours
--   22  seasonality_v2_occasion_weights     one row per occasion: importance weight and tier
--   23  seasonality_v2_product_occasion_profile  product x occasion: verdicts, timing, weights
--   24  seasonality_v2_theme_output         FINAL: one row per TPNB for theme building
--
-- Performance design:
--   * sales.invoice_line is read once (statement 1) with one large shuffle. Lines are
--     distributed by day and basket bucket, so basket, item and day aggregations run
--     inside their partitions. Only small partial aggregates move in a second shuffle.
--   * Statement 11 shuffles the entity-day table once, by entity. Everything after that
--     (role join, aggregation, annual totals, verdict window) runs in the same partitions,
--     with the small calendar, spell and product tables broadcast.
--   * Statement 5b works on subclass x day rows. Statements 17-24 work only on products
--     flagged seasonal somewhere. 18 is the largest (one join and one set of windows).
-- =====================================================================================
-- REAL-DATA PATCHES INCLUDED
--   * Product-master GB launch/deactivation dates are retained as diagnostics but no
--     longer hard-exclude event-years; observed availability spells determine eligibility.
--   * Incomplete years cannot pass the annual specifically-seasonal concentration test.
--     They can still contribute to local uplift evidence.
-- =====================================================================================


-- =====================================================================================
-- 0  PARAMETERS  (one row, read by every later statement)
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_params;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_params
USING PARQUET AS
SELECT
    CAST(0.5 AS DOUBLE)       AS low_traffic_ratio,             -- day below 50% of its weekday median = low traffic
    28                        AS spell_max_gap_days,            -- longer run without a sale = not on sale
    14                        AS min_baseline_days,             -- fewer usable baseline days = INSUFFICIENT_BASELINE
    CAST(20.0 AS DOUBLE)      AS min_expected_baskets,          -- volume gate for the core window (pre + event)
    CAST(10.0 AS DOUBLE)      AS min_expected_baskets_window,   -- volume gate for a single pre, event or post window
    CAST(1.5 AS DOUBLE)       AS uplift_index_min,              -- basket-share index for an uplift (1 / 1.5 for a decline)
    CAST(3.0 AS DOUBLE)       AS z_min,                         -- significance threshold
    CAST(5.0 AS DOUBLE)       AS specific_ratio_min,            -- core share vs rest-of-year share for "specific"
    CAST(20.0 AS DOUBLE)      AS min_specific_baskets,          -- minimum core baskets for the specific test
    CAST(-10.0 AS DOUBLE)     AS promo_price_change_max_pct,    -- price change at or below this = price drop
    2                         AS min_years,                     -- years needed before any verdict
    CAST(2.0 / 3.0 AS DOUBLE) AS min_year_share,                -- share of years that must agree
    120                       AS limited_edition_max_days,      -- longest on-sale run for a limited edition
    CAST(1.15 AS DOUBLE)      AS dow_index_min,                 -- day-of-week skew threshold
    84                        AS baseline_max_scan_days,        -- how far past the gap a baseline may reach for usable days
    CAST(2.0 AS DOUBLE)       AS nested_occasion_min_ratio,     -- an occasion inside a build-up must reach this x the build-up's share
    -- shopping windows, promotions and occasion weights (statements 5b and 17-24)
    CAST(0.10 AS DOUBLE)      AS timing_start_pct,              -- shopping has started once this share of the extra demand has happened
    CAST(0.50 AS DOUBLE)      AS timing_mid_pct,                -- midpoint of the extra demand
    CAST(0.90 AS DOUBLE)      AS timing_end_pct,                -- shopping is done once this share has happened
    7                         AS min_buildup_days,              -- a build-up shorter than this does not become its own window
    14                        AS default_search_days,           -- days searched before the pre-event window when an occasion sets none
    CAST(50.0 AS DOUBLE)      AS min_calibration_excess,        -- extra item slots needed to calibrate one occasion year
    3                         AS promo_min_days,                -- a promotion period needs at least this many promoted days
    3                         AS promo_max_gap_days,            -- promoted days at most this far apart belong to one period
    7                         AS timing_align_days,             -- promotion and demand starts this close count as aligned
    182                       AS range_landing_max_days,        -- an on-sale run starting this close before the event = seasonal range
    -- last: baskets needed for weekly and day-of-week profiles
    104                       AS min_profile_baskets;


-- =====================================================================================
-- 1  DAILY FACT  (the only read of sales.invoice_line)
--    One row per day x entity. entity_type:
--      TPNB    a product          baskets = baskets containing it
--      ALL     every product      baskets = all baskets, items = basket x TPNB slots
--      OUTLET  one outlet         for coverage and closure checks
--      SOURCE  one channel        shows which transaction_source values the table holds
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_daily_fact;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_daily_fact
USING PARQUET AS
WITH lines_raw AS (
    SELECT
        TO_DATE(il.transaction_date)                   AS sale_date,
        COALESCE(CAST(il.tpnb AS STRING), 'NULL_TPNB') AS entity_id,
        UPPER(TRIM(CAST(il.country_code AS STRING)))   AS sale_country_code,
        il.transaction_source,
        il.retail_outlet_number,
        il.checkout_no,
        il.transaction_start_datetime,
        il.transaction_number,
        COALESCE(
            NULLIF(TRIM(il.customer_friendly_description), ''),
            NULLIF(TRIM(il.description), ''),
            CAST(il.tpnb AS STRING),
            'NULL TPNB'
        )                                              AS invoice_product_description,
        CAST(il.amount AS DOUBLE)                      AS sales_value,
        CAST(il.quantity AS DOUBLE)                    AS units
    FROM sales.invoice_line il
    WHERE il.transaction_date >= DATE '2023-01-01'
      AND il.transaction_date <  DATE '2027-01-01'
      AND TO_DATE(il.transaction_date) <= CURRENT_DATE()
),

-- The one large shuffle. Every line of a basket lands in the same partition, and each
-- day is spread over 32 buckets so no single task holds a whole day.
lines AS (
    SELECT
        r.*,
        PMOD(
            HASH(r.sale_country_code, r.transaction_source, r.retail_outlet_number,
                 r.checkout_no, r.transaction_start_datetime, r.transaction_number),
            32
        ) AS basket_bucket
    FROM lines_raw r
    DISTRIBUTE BY sale_date, basket_bucket
),

-- One row per basket x TPNB (repeated lines of a TPNB in one basket are summed).
basket_item AS (
    SELECT
        sale_date,
        basket_bucket,
        sale_country_code,
        transaction_source,
        retail_outlet_number,
        checkout_no,
        transaction_start_datetime,
        transaction_number,
        entity_id,
        MAX(invoice_product_description) AS invoice_product_description,
        COALESCE(SUM(sales_value), 0.0)  AS item_sales,
        COALESCE(SUM(units), 0.0)        AS item_units
    FROM lines
    GROUP BY
        sale_date,
        basket_bucket,
        sale_country_code,
        transaction_source,
        retail_outlet_number,
        checkout_no,
        transaction_start_datetime,
        transaction_number,
        entity_id
),

-- Basket totals on every row of the basket. The window stays inside the partition.
basket_item_ctx AS (
    SELECT
        b.*,
        MIN(b.entity_id)  OVER bw AS first_entity_in_basket,
        COUNT(*)          OVER bw AS basket_items,
        SUM(b.item_sales) OVER bw AS basket_sales,
        SUM(b.item_units) OVER bw AS basket_units
    FROM basket_item b
    WINDOW bw AS (
        PARTITION BY
            b.sale_date,
            b.basket_bucket,
            b.sale_country_code,
            b.transaction_source,
            b.retail_outlet_number,
            b.checkout_no,
            b.transaction_start_datetime,
            b.transaction_number
    )
),

-- Each basket x TPNB row emits its TPNB row. The basket's first item also emits one ALL,
-- one OUTLET and one SOURCE row, so each basket is counted exactly once in those.
item_rows AS (
    SELECT
        c.sale_date,
        x.row_type,
        x.row_id,
        x.row_description,
        x.row_baskets,
        x.row_items,
        x.row_sales,
        x.row_units
    FROM basket_item_ctx c
    LATERAL VIEW INLINE(
        CASE
            WHEN c.entity_id = c.first_entity_in_basket THEN ARRAY(
                NAMED_STRUCT('t', 'TPNB', 'i', c.entity_id, 'd', c.invoice_product_description,
                             'b', CAST(1 AS BIGINT), 'n', CAST(1 AS BIGINT), 's', c.item_sales, 'u', c.item_units),
                NAMED_STRUCT('t', 'ALL', 'i', 'ALL', 'd', CAST(NULL AS STRING),
                             'b', CAST(1 AS BIGINT), 'n', c.basket_items, 's', c.basket_sales, 'u', c.basket_units),
                NAMED_STRUCT('t', 'OUTLET', 'i', COALESCE(CAST(c.retail_outlet_number AS STRING), 'NULL'), 'd', CAST(NULL AS STRING),
                             'b', CAST(1 AS BIGINT), 'n', c.basket_items, 's', c.basket_sales, 'u', c.basket_units),
                NAMED_STRUCT('t', 'SOURCE', 'i', COALESCE(CAST(c.transaction_source AS STRING), 'NULL'), 'd', CAST(NULL AS STRING),
                             'b', CAST(1 AS BIGINT), 'n', c.basket_items, 's', c.basket_sales, 'u', c.basket_units)
            )
            ELSE ARRAY(
                NAMED_STRUCT('t', 'TPNB', 'i', c.entity_id, 'd', c.invoice_product_description,
                             'b', CAST(1 AS BIGINT), 'n', CAST(1 AS BIGINT), 's', c.item_sales, 'u', c.item_units)
            )
        END
    ) x AS row_type, row_id, row_description, row_baskets, row_items, row_sales, row_units
)

SELECT
    sale_date,
    YEAR(sale_date)      AS sale_year,
    DAYOFWEEK(sale_date) AS sale_day_of_week,
    row_type             AS entity_type,
    row_id               AS entity_id,
    MAX(row_description) AS invoice_product_description,
    SUM(row_baskets)     AS baskets,
    SUM(row_items)       AS items,
    SUM(row_sales)       AS sales,
    SUM(row_units)       AS units
FROM item_rows
GROUP BY sale_date, row_type, row_id;


-- =====================================================================================
-- 2  PRODUCT DIMENSION  (one UKROI product master row per sold TPNB)
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_product_dim;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_product_dim
USING PARQUET AS
WITH sold_tpnb AS (
    SELECT DISTINCT TRY_CAST(entity_id AS BIGINT) AS tpnb
    FROM lab_customer_value_analytics.seasonality_v2_daily_fact
    WHERE entity_type = 'TPNB'
      AND TRY_CAST(entity_id AS BIGINT) IS NOT NULL
),

product_filtered AS (
    SELECT
        CAST(p.tpnb AS BIGINT) AS tpnb,
        CAST(p.tpna AS STRING) AS product_master_tpna,
        COALESCE(
            NULLIF(TRIM(p.customer_friendly_description), ''),
            NULLIF(TRIM(p.description), ''),
            NULLIF(TRIM(p.tpna_description), ''),
            NULLIF(TRIM(p.till_description), ''),
            CAST(p.tpnb AS STRING)
        ) AS product_description,
        p.commercial_hierarchy_division_code   AS hierarchy_division_code,
        p.commercial_hierarchy_division_name   AS hierarchy_division_name,
        p.commercial_hierarchy_department_code AS hierarchy_department_code,
        p.commercial_hierarchy_department_name AS hierarchy_department_name,
        p.commercial_hierarchy_section_code    AS hierarchy_section_code,
        p.commercial_hierarchy_section_name    AS hierarchy_section_name,
        p.commercial_hierarchy_class_code      AS hierarchy_class_code,
        p.commercial_hierarchy_class_name      AS hierarchy_class_name,
        p.commercial_hierarchy_subclass_code   AS hierarchy_subclass_code,
        p.commercial_hierarchy_subclass_name   AS hierarchy_subclass_name,
        -- Guarded so a product with no GB entry gives NULL instead of failing under ANSI.
        CASE
            WHEN SIZE(FILTER(p.country, x -> x.code = 'GB')) > 0
                THEN ELEMENT_AT(FILTER(p.country, x -> x.code = 'GB'), 1)
        END AS gb_country_record,
        p.load_date_time,
        p.load_date,
        p.tap_load_date,
        p.unique_product_key,
        p.description AS raw_description
    FROM product.product p
    LEFT SEMI JOIN sold_tpnb s
      ON CAST(p.tpnb AS BIGINT) = s.tpnb
    WHERE p.country_code = 'UKROI'
      AND p.tpnb IS NOT NULL
),

ranked AS (
    SELECT
        pf.*,
        COUNT(*) OVER (PARTITION BY pf.tpnb) AS product_master_rows_for_tpnb,
        ROW_NUMBER() OVER (
            PARTITION BY pf.tpnb
            ORDER BY
                CASE WHEN pf.gb_country_record IS NOT NULL THEN 0 ELSE 1 END,
                pf.load_date_time DESC NULLS LAST,
                pf.load_date DESC NULLS LAST,
                pf.tap_load_date DESC NULLS LAST,
                COALESCE(pf.product_master_tpna, '') DESC,
                COALESCE(pf.unique_product_key, '') DESC,
                SHA2(
                    CONCAT_WS(
                        '||',
                        COALESCE(pf.hierarchy_division_code, ''),
                        COALESCE(pf.hierarchy_department_code, ''),
                        COALESCE(pf.hierarchy_section_code, ''),
                        COALESCE(pf.hierarchy_class_code, ''),
                        COALESCE(pf.hierarchy_subclass_code, ''),
                        COALESCE(pf.raw_description, '')
                    ),
                    256
                ) DESC
        ) AS product_rn
    FROM product_filtered pf
)

SELECT
    tpnb,
    product_master_tpna,
    'UKROI' AS product_market_code,
    CASE WHEN gb_country_record IS NOT NULL THEN 'Y' ELSE 'N' END AS has_gb_country_record,
    TRY_CAST(gb_country_record.launch_date AS DATE)   AS gb_launch_date,
    TRY_CAST(gb_country_record.deactive_date AS DATE) AS gb_deactive_date,
    product_description,
    hierarchy_division_code,
    hierarchy_division_name,
    hierarchy_department_code,
    hierarchy_department_name,
    hierarchy_section_code,
    hierarchy_section_name,
    hierarchy_class_code,
    hierarchy_class_name,
    hierarchy_subclass_code,
    hierarchy_subclass_name,
    CASE WHEN hierarchy_subclass_code IS NOT NULL
         THEN CONCAT('SC:', hierarchy_subclass_code) END AS subclass_entity_id,
    product_master_rows_for_tpnb,
    CASE WHEN product_master_rows_for_tpnb > 1 THEN 'Y' ELSE 'N' END AS product_master_dedup_applied_flag
FROM ranked
WHERE product_rn = 1;


-- =====================================================================================
-- 3  OCCASION CONFIG  (edit to tune windows. Occasions not listed get the defaults:
--    family OTHER, lead 2 days, lag 0, gap 7, 4 weeks each side, not major)
--    lead_days          days before the event counted as the pre-event window
--    lag_days           days after the event counted as the post-event window
--    baseline_gap_days  days left out between the windows and the baseline
--    baseline_weeks     weeks of baseline on each side
--    baseline_side      BOTH, BEFORE or AFTER
--    is_major           Y = its windows are removed from every other occasion's baseline
--    buildup_search_days  how far before the event statement 5b looks for the start of
--                         shopping. NULL = no build-up window for this occasion
--    recurrence_per_year  how often the occasion happens (1 = yearly, 0.25 = every 4 years),
--                         used to weight occasions in statement 22
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_occasion_config;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_occasion_config
USING PARQUET AS
SELECT
    occasion_key,
    occasion_family,
    is_major,
    CAST(lead_days AS INT)         AS lead_days,
    CAST(lag_days AS INT)          AS lag_days,
    CAST(baseline_gap_days AS INT) AS baseline_gap_days,
    CAST(baseline_weeks AS INT)    AS baseline_weeks,
    baseline_side,
    CAST(buildup_search_days AS INT)    AS buildup_search_days,
    CAST(recurrence_per_year AS DOUBLE) AS recurrence_per_year,
    config_note
FROM VALUES
    -- Christmas and New Year: baselines end in late November, before the build-up
    ('CHRISTMAS_DAY',              'CHRISTMAS_NEW_YEAR',       'Y', 7, 0, 21, 4, 'BEFORE',   70,  1.0, 'Shops likely closed on the day, so the pre-event week carries the effect'),
    ('BOXING_DAY',                 'CHRISTMAS_NEW_YEAR',       'Y', 0, 2, 29, 4, 'BEFORE', NULL,  1.0, 'No pre window: it would be Christmas itself'),
    ('NEW_YEARS_EVE',              'CHRISTMAS_NEW_YEAR',       'Y', 1, 0, 33, 4, 'BEFORE', NULL,  1.0, 'Added in v2: missing from the source calendar'),
    ('NEW_YEARS_DAY',              'CHRISTMAS_NEW_YEAR',       'Y', 0, 7, 35, 4, 'BEFORE', NULL,  1.0, 'Post window = first week of January'),
    -- Easter
    ('EASTER_SUNDAY',              'EASTER',                   'Y', 3, 1, 7, 4, 'BOTH',   49,  1.0,   'Pre = Thursday to Saturday, post = Easter Monday'),
    -- Gifting and celebrations
    ('MOTHERS_DAY',                'GIFTING_CELEBRATIONS',     'Y', 3, 0, 7, 4, 'BOTH',   21,  1.0,   NULL),
    ('FATHERS_DAY',                'GIFTING_CELEBRATIONS',     'Y', 3, 0, 7, 4, 'BOTH',   21,  1.0,   NULL),
    ('VALENTINES_GALENTINES',      'GIFTING_CELEBRATIONS',     'Y', 2, 0, 7, 4, 'BOTH',   21,  1.0,   NULL),
    ('INTERNATIONAL_WOMENS_DAY',   'GIFTING_CELEBRATIONS',     'N', 1, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('INTERNATIONAL_MENS_DAY',     'GIFTING_CELEBRATIONS',     'N', 1, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    -- Halloween and Bonfire Night
    ('HALLOWEEN',                  'HALLOWEEN_BONFIRE',        'Y', 5, 0, 7, 4, 'BOTH',   35,  1.0,   NULL),
    ('BONFIRE_NIGHT',              'HALLOWEEN_BONFIRE',        'Y', 3, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    -- Bank holidays: pre = Friday to Sunday before the Monday
    ('EARLY_MAY_BANK_HOLIDAY',     'BANK_HOLIDAYS',            'Y', 3, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('SPRING_BANK_HOLIDAY',        'BANK_HOLIDAYS',            'Y', 3, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('SUMMER_BANK_HOLIDAY',        'BANK_HOLIDAYS',            'Y', 3, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    -- Food moments
    ('PANCAKE_DAY',                'FOOD_MOMENTS',             'Y', 3, 0, 7, 4, 'BOTH',   14,  1.0,   NULL),
    ('BRITISH_FOOD_FORTNIGHT',     'FOOD_MOMENTS',             'N', 0, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('TASTE_OF_LONDON',            'FOOD_MOMENTS',             'N', 0, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('WORLD_VEGETARIAN_DAY',       'FOOD_MOMENTS',             'N', 1, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('FAIRTRADE_FORTNIGHT',        'FOOD_MOMENTS',             'N', 0, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    -- Retail events
    ('BLACK_FRIDAY',               'RETAIL_EVENTS',            'Y', 0, 3, 7, 4, 'BOTH',   21,  1.0,   'Post window runs to Cyber Monday'),
    ('BACK_TO_SCHOOL_UNIVERSITY',  'RETAIL_EVENTS',            'N', 7, 0, 7, 4, 'BOTH',   42,  1.0,   NULL),
    -- National days
    ('BURNS_NIGHT',                'NATIONAL_DAYS',            'N', 3, 0, 7, 4, 'BOTH',   21,  1.0,   NULL),
    ('ST_ANDREWS_DAY',             'NATIONAL_DAYS',            'N', 2, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('ST_DAVIDS_DAY',              'NATIONAL_DAYS',            'N', 2, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('ST_GEORGES_DAY',             'NATIONAL_DAYS',            'N', 2, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('ST_PATRICKS_DAY',            'NATIONAL_DAYS',            'N', 2, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    -- World religious and cultural moments
    ('LUNAR_NEW_YEAR',             'WORLD_RELIGIOUS_CULTURAL', 'N', 3, 1, 7, 4, 'BOTH',   21,  1.0,   NULL),
    ('DIWALI',                     'WORLD_RELIGIOUS_CULTURAL', 'N', 3, 0, 7, 4, 'BOTH',   21,  1.0,   NULL),
    ('RAMADAN',                    'WORLD_RELIGIOUS_CULTURAL', 'N', 3, 0, 7, 4, 'BOTH',   14,  1.0,   'A month-long period, not a single day'),
    ('EID_AL_FITR',                'WORLD_RELIGIOUS_CULTURAL', 'N', 3, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('HOLI',                       'WORLD_RELIGIOUS_CULTURAL', 'N', 2, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('PASSOVER',                   'WORLD_RELIGIOUS_CULTURAL', 'N', 3, 0, 7, 4, 'BOTH',   14,  1.0,   NULL),
    ('HANUKKAH',                   'WORLD_RELIGIOUS_CULTURAL', 'N', 2, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    -- Sport: tournaments are periods (no lead), single-day events get a short lead-in
    ('RUGBY_SIX_NATIONS',          'SPORT',                    'N', 0, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('WIMBLEDON',                  'SPORT',                    'N', 0, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('THE_ASHES',                  'SPORT',                    'N', 0, 0, 7, 4, 'BOTH', NULL,  0.5,   NULL),
    ('FIFA_CLUB_WORLD_CUP',        'SPORT',                    'N', 0, 0, 7, 4, 'BOTH', NULL, 0.25,   NULL),
    ('FIFA_WORLD_CUP',             'SPORT',                    'N', 0, 0, 7, 4, 'BOTH', NULL, 0.25,   NULL),
    ('UEFA_WOMENS_EURO',           'SPORT',                    'N', 0, 0, 7, 4, 'BOTH', NULL, 0.25,   NULL),
    ('WOMENS_RUGBY_WORLD_CUP',     'SPORT',                    'N', 0, 0, 7, 4, 'BOTH', NULL, 0.25,   NULL),
    ('TOUR_DE_FRANCE',             'SPORT',                    'N', 0, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('COMMONWEALTH_GAMES',         'SPORT',                    'N', 0, 0, 7, 4, 'BOTH', NULL, 0.25,   NULL),
    ('WINTER_OLYMPICS',            'SPORT',                    'N', 0, 0, 7, 4, 'BOTH', NULL, 0.25,   NULL),
    ('FA_CUP_FINAL',               'SPORT',                    'N', 2, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('GRAND_NATIONAL',             'SPORT',                    'N', 2, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('THE_BOAT_RACE',              'SPORT',                    'N', 2, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('ROYAL_ASCOT',                'SPORT',                    'N', 1, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('THE_OPEN_CHAMPIONSHIP',      'SPORT',                    'N', 1, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('BRITISH_GRAND_PRIX',         'SPORT',                    'N', 1, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    -- Entertainment and festivals
    ('BAFTAS',                     'ENTERTAINMENT_FESTIVALS',  'N', 1, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('OSCARS',                     'ENTERTAINMENT_FESTIVALS',  'N', 1, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('EUROVISION_FINAL',           'ENTERTAINMENT_FESTIVALS',  'N', 1, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('GLASTONBURY_FESTIVAL',       'ENTERTAINMENT_FESTIVALS',  'N', 2, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('ISLE_OF_WIGHT_FESTIVAL',     'ENTERTAINMENT_FESTIVALS',  'N', 2, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('EDINBURGH_FESTIVAL_FRINGE',  'ENTERTAINMENT_FESTIVALS',  'N', 0, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('BBC_PROMS',                  'ENTERTAINMENT_FESTIVALS',  'N', 0, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('RHS_CHELSEA_FLOWER_SHOW',    'ENTERTAINMENT_FESTIVALS',  'N', 0, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('RED_NOSE_DAY',               'ENTERTAINMENT_FESTIVALS',  'N', 3, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    -- Awareness days
    ('WORLD_HEALTH_DAY',           'AWARENESS_DAYS',           'N', 1, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('RANDOM_ACTS_OF_KINDNESS_DAY','AWARENESS_DAYS',           'N', 1, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('WORLD_CHILDRENS_DAY',        'AWARENESS_DAYS',           'N', 1, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('WORLD_BOOK_DAY_UK_IRELAND',  'AWARENESS_DAYS',           'N', 1, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('MENS_HEALTH_WEEK',           'AWARENESS_DAYS',           'N', 0, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('WOMENS_HEALTH_WEEK',         'AWARENESS_DAYS',           'N', 0, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('JUNETEENTH',                 'AWARENESS_DAYS',           'N', 1, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL),
    ('REMEMBRANCE_SUNDAY',         'AWARENESS_DAYS',           'N', 2, 0, 7, 4, 'BOTH', NULL,  1.0,   NULL)
AS c(occasion_key, occasion_family, is_major, lead_days, lag_days, baseline_gap_days,
     baseline_weeks, baseline_side, buildup_search_days, recurrence_per_year, config_note);


-- =====================================================================================
-- 4  EVENT CALENDAR  (one dated row per occasion x year, 2023-2026)
--    Three sources, in priority order where they overlap:
--      a. the team's source calendar (unchanged)
--      b. RULE_COMPUTED_V2: dates computed from fixed rules (Easter by the Gregorian
--         algorithm, Mothering Sunday, Pancake Day, Father's Day, bank holidays, Black
--         Friday, Remembrance Sunday, World Book Day, back to school, New Year's Eve)
--      c. SUPPLEMENT_V2_VERIFY: looked-up dates for moving religious and sporting events
--         in the years the source calendar lacks. Please confirm these with the team.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_event_calendar;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_event_calendar
USING PARQUET AS
WITH source_calendar AS (
    SELECT
        event_occurrence_id,
        occasion_key,
        occasion_name,
        occasion_group,
        event_year,
        event_start_date,
        event_end_date,
        date_origin,
        recurrence_handling
    FROM VALUES
        ('NEW_YEARS_DAY__20230101__20230101', 'NEW_YEARS_DAY', 'New Year''s Day', 'Big Moments', 2023, DATE '2023-01-01', DATE '2023-01-01', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('BURNS_NIGHT__20230125__20230125', 'BURNS_NIGHT', 'Burns Night', 'Religious & Cultural Moments', 2023, DATE '2023-01-25', DATE '2023-01-25', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('VALENTINES_GALENTINES__20230213__20230214', 'VALENTINES_GALENTINES', 'Valentine''s Day & Galentine''s Day', 'Big Moments', 2023, DATE '2023-02-13', DATE '2023-02-14', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('RANDOM_ACTS_OF_KINDNESS_DAY__20230217__20230217', 'RANDOM_ACTS_OF_KINDNESS_DAY', 'Random Acts of Kindness Day', 'Religious & Cultural Moments', 2023, DATE '2023-02-17', DATE '2023-02-17', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('ST_DAVIDS_DAY__20230301__20230301', 'ST_DAVIDS_DAY', 'St David''s Day', 'Religious & Cultural Moments', 2023, DATE '2023-03-01', DATE '2023-03-01', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('INTERNATIONAL_WOMENS_DAY__20230308__20230308', 'INTERNATIONAL_WOMENS_DAY', 'International Women''s Day', 'Mini Moments', 2023, DATE '2023-03-08', DATE '2023-03-08', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('ST_PATRICKS_DAY__20230317__20230317', 'ST_PATRICKS_DAY', 'St Patrick''s Day', 'Religious & Cultural Moments', 2023, DATE '2023-03-17', DATE '2023-03-17', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('WORLD_HEALTH_DAY__20230407__20230407', 'WORLD_HEALTH_DAY', 'World Health Day', 'Religious & Cultural Moments', 2023, DATE '2023-04-07', DATE '2023-04-07', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('ST_GEORGES_DAY__20230423__20230423', 'ST_GEORGES_DAY', 'St George''s Day', 'Religious & Cultural Moments', 2023, DATE '2023-04-23', DATE '2023-04-23', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('JUNETEENTH__20230619__20230619', 'JUNETEENTH', 'Juneteenth', 'Religious & Cultural Moments', 2023, DATE '2023-06-19', DATE '2023-06-19', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('WORLD_VEGETARIAN_DAY__20231001__20231001', 'WORLD_VEGETARIAN_DAY', 'World Vegetarian Day', 'Religious & Cultural Moments', 2023, DATE '2023-10-01', DATE '2023-10-01', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('HALLOWEEN__20231031__20231031', 'HALLOWEEN', 'Halloween', 'Big Moments', 2023, DATE '2023-10-31', DATE '2023-10-31', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('BONFIRE_NIGHT__20231105__20231105', 'BONFIRE_NIGHT', 'Bonfire Night (Guy Fawkes)', 'Big Moments', 2023, DATE '2023-11-05', DATE '2023-11-05', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('INTERNATIONAL_MENS_DAY__20231119__20231119', 'INTERNATIONAL_MENS_DAY', 'International Men''s Day', 'Mini Moments', 2023, DATE '2023-11-19', DATE '2023-11-19', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('WORLD_CHILDRENS_DAY__20231120__20231120', 'WORLD_CHILDRENS_DAY', 'World Children''s Day', 'Mini Moments', 2023, DATE '2023-11-20', DATE '2023-11-20', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('ST_ANDREWS_DAY__20231130__20231130', 'ST_ANDREWS_DAY', 'St Andrew''s Day', 'Religious & Cultural Moments', 2023, DATE '2023-11-30', DATE '2023-11-30', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('CHRISTMAS_DAY__20231225__20231225', 'CHRISTMAS_DAY', 'Christmas Day', 'Big Moments', 2023, DATE '2023-12-25', DATE '2023-12-25', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('BOXING_DAY__20231226__20231226', 'BOXING_DAY', 'Boxing Day', 'Big Moments', 2023, DATE '2023-12-26', DATE '2023-12-26', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('NEW_YEARS_DAY__20240101__20240101', 'NEW_YEARS_DAY', 'New Year''s Day', 'Big Moments', 2024, DATE '2024-01-01', DATE '2024-01-01', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('BURNS_NIGHT__20240125__20240125', 'BURNS_NIGHT', 'Burns Night', 'Religious & Cultural Moments', 2024, DATE '2024-01-25', DATE '2024-01-25', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('VALENTINES_GALENTINES__20240213__20240214', 'VALENTINES_GALENTINES', 'Valentine''s Day & Galentine''s Day', 'Big Moments', 2024, DATE '2024-02-13', DATE '2024-02-14', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('RANDOM_ACTS_OF_KINDNESS_DAY__20240217__20240217', 'RANDOM_ACTS_OF_KINDNESS_DAY', 'Random Acts of Kindness Day', 'Religious & Cultural Moments', 2024, DATE '2024-02-17', DATE '2024-02-17', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('ST_DAVIDS_DAY__20240301__20240301', 'ST_DAVIDS_DAY', 'St David''s Day', 'Religious & Cultural Moments', 2024, DATE '2024-03-01', DATE '2024-03-01', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('INTERNATIONAL_WOMENS_DAY__20240308__20240308', 'INTERNATIONAL_WOMENS_DAY', 'International Women''s Day', 'Mini Moments', 2024, DATE '2024-03-08', DATE '2024-03-08', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('ST_PATRICKS_DAY__20240317__20240317', 'ST_PATRICKS_DAY', 'St Patrick''s Day', 'Religious & Cultural Moments', 2024, DATE '2024-03-17', DATE '2024-03-17', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('WORLD_HEALTH_DAY__20240407__20240407', 'WORLD_HEALTH_DAY', 'World Health Day', 'Religious & Cultural Moments', 2024, DATE '2024-04-07', DATE '2024-04-07', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('ST_GEORGES_DAY__20240423__20240423', 'ST_GEORGES_DAY', 'St George''s Day', 'Religious & Cultural Moments', 2024, DATE '2024-04-23', DATE '2024-04-23', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('JUNETEENTH__20240619__20240619', 'JUNETEENTH', 'Juneteenth', 'Religious & Cultural Moments', 2024, DATE '2024-06-19', DATE '2024-06-19', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('WORLD_VEGETARIAN_DAY__20241001__20241001', 'WORLD_VEGETARIAN_DAY', 'World Vegetarian Day', 'Religious & Cultural Moments', 2024, DATE '2024-10-01', DATE '2024-10-01', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('HALLOWEEN__20241031__20241031', 'HALLOWEEN', 'Halloween', 'Big Moments', 2024, DATE '2024-10-31', DATE '2024-10-31', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('BONFIRE_NIGHT__20241105__20241105', 'BONFIRE_NIGHT', 'Bonfire Night (Guy Fawkes)', 'Big Moments', 2024, DATE '2024-11-05', DATE '2024-11-05', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('INTERNATIONAL_MENS_DAY__20241119__20241119', 'INTERNATIONAL_MENS_DAY', 'International Men''s Day', 'Mini Moments', 2024, DATE '2024-11-19', DATE '2024-11-19', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('WORLD_CHILDRENS_DAY__20241120__20241120', 'WORLD_CHILDRENS_DAY', 'World Children''s Day', 'Mini Moments', 2024, DATE '2024-11-20', DATE '2024-11-20', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('ST_ANDREWS_DAY__20241130__20241130', 'ST_ANDREWS_DAY', 'St Andrew''s Day', 'Religious & Cultural Moments', 2024, DATE '2024-11-30', DATE '2024-11-30', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('CHRISTMAS_DAY__20241225__20241225', 'CHRISTMAS_DAY', 'Christmas Day', 'Big Moments', 2024, DATE '2024-12-25', DATE '2024-12-25', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('BOXING_DAY__20241226__20241226', 'BOXING_DAY', 'Boxing Day', 'Big Moments', 2024, DATE '2024-12-26', DATE '2024-12-26', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('NEW_YEARS_DAY__20250101__20250101', 'NEW_YEARS_DAY', 'New Year''s Day', 'Big Moments', 2025, DATE '2025-01-01', DATE '2025-01-01', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('BURNS_NIGHT__20250125__20250125', 'BURNS_NIGHT', 'Burns Night', 'Religious & Cultural Moments', 2025, DATE '2025-01-25', DATE '2025-01-25', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('LUNAR_NEW_YEAR__20250129__20250129', 'LUNAR_NEW_YEAR', 'Chinese / Lunar New Year', 'Religious & Cultural Moments', 2025, DATE '2025-01-29', DATE '2025-01-29', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('RUGBY_SIX_NATIONS__20250131__20250315', 'RUGBY_SIX_NATIONS', 'Rugby Six Nations', 'Big Moments', 2025, DATE '2025-01-31', DATE '2025-03-15', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 2, '2025, 2026', 'PARTIAL_DATES'),
        ('VALENTINES_GALENTINES__20250213__20250214', 'VALENTINES_GALENTINES', 'Valentine''s Day & Galentine''s Day', 'Big Moments', 2025, DATE '2025-02-13', DATE '2025-02-14', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('BAFTAS__20250216__20250216', 'BAFTAS', 'BAFTAs', 'Religious & Cultural Moments', 2025, DATE '2025-02-16', DATE '2025-02-16', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('RANDOM_ACTS_OF_KINDNESS_DAY__20250217__20250217', 'RANDOM_ACTS_OF_KINDNESS_DAY', 'Random Acts of Kindness Day', 'Religious & Cultural Moments', 2025, DATE '2025-02-17', DATE '2025-02-17', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('RAMADAN__20250301__20250330', 'RAMADAN', 'Ramadan', 'Religious & Cultural Moments', 2025, DATE '2025-03-01', DATE '2025-03-30', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('ST_DAVIDS_DAY__20250301__20250301', 'ST_DAVIDS_DAY', 'St David''s Day', 'Religious & Cultural Moments', 2025, DATE '2025-03-01', DATE '2025-03-01', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('OSCARS__20250302__20250302', 'OSCARS', 'Oscars', 'Religious & Cultural Moments', 2025, DATE '2025-03-02', DATE '2025-03-02', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('WORLD_BOOK_DAY_UK_IRELAND__20250306__20250306', 'WORLD_BOOK_DAY_UK_IRELAND', 'World Book Day (UK & Ireland)', 'Religious & Cultural Moments', 2025, DATE '2025-03-06', DATE '2025-03-06', 'SOURCE_EXPLICIT', 'RULE_BASED_ANNUAL_DATE', 1, '2025', 'PARTIAL_DATES'),
        ('INTERNATIONAL_WOMENS_DAY__20250308__20250308', 'INTERNATIONAL_WOMENS_DAY', 'International Women''s Day', 'Mini Moments', 2025, DATE '2025-03-08', DATE '2025-03-08', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('HOLI__20250314__20250314', 'HOLI', 'Holi', 'Religious & Cultural Moments', 2025, DATE '2025-03-14', DATE '2025-03-14', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('ST_PATRICKS_DAY__20250317__20250317', 'ST_PATRICKS_DAY', 'St Patrick''s Day', 'Religious & Cultural Moments', 2025, DATE '2025-03-17', DATE '2025-03-17', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('RED_NOSE_DAY__20250321__20250321', 'RED_NOSE_DAY', 'Comic Relief / Red Nose Day', 'Religious & Cultural Moments', 2025, DATE '2025-03-21', DATE '2025-03-21', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('EID_AL_FITR__20250330__20250331', 'EID_AL_FITR', 'Eid al-Fitr', 'Religious & Cultural Moments', 2025, DATE '2025-03-30', DATE '2025-03-31', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('MOTHERS_DAY__20250330__20250330', 'MOTHERS_DAY', 'Mother''s Day', 'Big Moments', 2025, DATE '2025-03-30', DATE '2025-03-30', 'SOURCE_EXPLICIT', 'RULE_BASED_ANNUAL_DATE', 1, '2025', 'PARTIAL_DATES'),
        ('GRAND_NATIONAL__20250405__20250405', 'GRAND_NATIONAL', 'Grand National', 'Big Moments', 2025, DATE '2025-04-05', DATE '2025-04-05', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('WORLD_HEALTH_DAY__20250407__20250407', 'WORLD_HEALTH_DAY', 'World Health Day', 'Religious & Cultural Moments', 2025, DATE '2025-04-07', DATE '2025-04-07', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('PASSOVER__20250413__20250420', 'PASSOVER', 'Passover', 'Religious & Cultural Moments', 2025, DATE '2025-04-13', DATE '2025-04-20', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('THE_BOAT_RACE__20250413__20250413', 'THE_BOAT_RACE', 'The Boat Race (Oxford vs Cambridge)', 'Big Moments', 2025, DATE '2025-04-13', DATE '2025-04-13', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('EASTER_SUNDAY__20250420__20250420', 'EASTER_SUNDAY', 'Easter Sunday', 'Big Moments', 2025, DATE '2025-04-20', DATE '2025-04-20', 'SOURCE_EXPLICIT', 'RULE_BASED_ANNUAL_DATE', 1, '2025', 'PARTIAL_DATES'),
        ('ST_GEORGES_DAY__20250423__20250423', 'ST_GEORGES_DAY', 'St George''s Day', 'Religious & Cultural Moments', 2025, DATE '2025-04-23', DATE '2025-04-23', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('EARLY_MAY_BANK_HOLIDAY__20250505__20250505', 'EARLY_MAY_BANK_HOLIDAY', 'Early May Bank Holiday', 'Big Moments', 2025, DATE '2025-05-05', DATE '2025-05-05', 'SOURCE_EXPLICIT', 'RULE_BASED_ANNUAL_DATE', 1, '2025', 'PARTIAL_DATES'),
        ('WOMENS_HEALTH_WEEK__20250512__20250518', 'WOMENS_HEALTH_WEEK', 'Women''s Health Week', 'Religious & Cultural Moments', 2025, DATE '2025-05-12', DATE '2025-05-18', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('EUROVISION_FINAL__20250517__20250517', 'EUROVISION_FINAL', 'Eurovision Song Contest Final', 'Religious & Cultural Moments', 2025, DATE '2025-05-17', DATE '2025-05-17', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('FA_CUP_FINAL__20250517__20250517', 'FA_CUP_FINAL', 'FA Cup Final', 'Big Moments', 2025, DATE '2025-05-17', DATE '2025-05-17', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('RHS_CHELSEA_FLOWER_SHOW__20250520__20250524', 'RHS_CHELSEA_FLOWER_SHOW', 'RHS Chelsea Flower Show', 'Religious & Cultural Moments', 2025, DATE '2025-05-20', DATE '2025-05-24', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('SPRING_BANK_HOLIDAY__20250526__20250526', 'SPRING_BANK_HOLIDAY', 'Spring Bank Holiday', 'Big Moments', 2025, DATE '2025-05-26', DATE '2025-05-26', 'SOURCE_EXPLICIT', 'RULE_BASED_ANNUAL_DATE', 1, '2025', 'PARTIAL_DATES'),
        ('MENS_HEALTH_WEEK__20250609__20250615', 'MENS_HEALTH_WEEK', 'Men''s Health Week', 'Religious & Cultural Moments', 2025, DATE '2025-06-09', DATE '2025-06-15', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('FIFA_CLUB_WORLD_CUP__20250615__20250713', 'FIFA_CLUB_WORLD_CUP', 'FIFA Club World Cup', 'Big Moments', 2025, DATE '2025-06-15', DATE '2025-07-13', 'SOURCE_EXPLICIT', 'PERIODIC_OR_ONE_OFF', 1, '2025', 'PARTIAL_DATES'),
        ('FATHERS_DAY__20250615__20250615', 'FATHERS_DAY', 'Father''s Day', 'Big Moments', 2025, DATE '2025-06-15', DATE '2025-06-15', 'SOURCE_EXPLICIT', 'RULE_BASED_ANNUAL_DATE', 1, '2025', 'PARTIAL_DATES'),
        ('ROYAL_ASCOT__20250617__20250621', 'ROYAL_ASCOT', 'Royal Ascot', 'Big Moments', 2025, DATE '2025-06-17', DATE '2025-06-21', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('TASTE_OF_LONDON__20250618__20250622', 'TASTE_OF_LONDON', 'Taste of London', 'Religious & Cultural Moments', 2025, DATE '2025-06-18', DATE '2025-06-22', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('ISLE_OF_WIGHT_FESTIVAL__20250619__20250622', 'ISLE_OF_WIGHT_FESTIVAL', 'Isle of Wight Festival', 'Religious & Cultural Moments', 2025, DATE '2025-06-19', DATE '2025-06-22', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('JUNETEENTH__20250619__20250619', 'JUNETEENTH', 'Juneteenth', 'Religious & Cultural Moments', 2025, DATE '2025-06-19', DATE '2025-06-19', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('GLASTONBURY_FESTIVAL__20250625__20250629', 'GLASTONBURY_FESTIVAL', 'Glastonbury Festival', 'Religious & Cultural Moments', 2025, DATE '2025-06-25', DATE '2025-06-29', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('WIMBLEDON__20250630__20250713', 'WIMBLEDON', 'Wimbledon', 'Big Moments', 2025, DATE '2025-06-30', DATE '2025-07-13', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 2, '2025, 2026', 'PARTIAL_DATES'),
        ('UEFA_WOMENS_EURO__20250702__20250727', 'UEFA_WOMENS_EURO', 'UEFA Women''s EURO', 'Big Moments', 2025, DATE '2025-07-02', DATE '2025-07-27', 'SOURCE_EXPLICIT', 'PERIODIC_OR_ONE_OFF', 1, '2025', 'PARTIAL_DATES'),
        ('BRITISH_GRAND_PRIX__20250704__20250706', 'BRITISH_GRAND_PRIX', 'British Grand Prix', 'Big Moments', 2025, DATE '2025-07-04', DATE '2025-07-06', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 2, '2025, 2026', 'PARTIAL_DATES'),
        ('TOUR_DE_FRANCE__20250705__20250727', 'TOUR_DE_FRANCE', 'Tour de France', 'Big Moments', 2025, DATE '2025-07-05', DATE '2025-07-27', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('THE_OPEN_CHAMPIONSHIP__20250717__20250720', 'THE_OPEN_CHAMPIONSHIP', 'The Open Championship', 'Big Moments', 2025, DATE '2025-07-17', DATE '2025-07-20', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 2, '2025, 2026', 'PARTIAL_DATES'),
        ('BBC_PROMS__20250718__20250913', 'BBC_PROMS', 'BBC Proms', 'Religious & Cultural Moments', 2025, DATE '2025-07-18', DATE '2025-09-13', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('EDINBURGH_FESTIVAL_FRINGE__20250801__20250825', 'EDINBURGH_FESTIVAL_FRINGE', 'Edinburgh Festival Fringe', 'Religious & Cultural Moments', 2025, DATE '2025-08-01', DATE '2025-08-25', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('WOMENS_RUGBY_WORLD_CUP__20250822__20250927', 'WOMENS_RUGBY_WORLD_CUP', 'Women''s Rugby World Cup', 'Big Moments', 2025, DATE '2025-08-22', DATE '2025-09-27', 'SOURCE_EXPLICIT', 'PERIODIC_OR_ONE_OFF', 1, '2025', 'PARTIAL_DATES'),
        ('SUMMER_BANK_HOLIDAY__20250825__20250825', 'SUMMER_BANK_HOLIDAY', 'Summer / August Bank Holiday', 'Big Moments', 2025, DATE '2025-08-25', DATE '2025-08-25', 'SOURCE_EXPLICIT', 'RULE_BASED_ANNUAL_DATE', 2, '2025, 2026', 'PARTIAL_DATES'),
        ('BACK_TO_SCHOOL_UNIVERSITY__20250901__20250930', 'BACK_TO_SCHOOL_UNIVERSITY', 'Back to School / University', 'Big Moments', 2025, DATE '2025-09-01', DATE '2025-09-30', 'SOURCE_EXPLICIT', 'APPROXIMATE_BUSINESS_WINDOW', 1, '2025', 'PARTIAL_DATES'),
        ('FAIRTRADE_FORTNIGHT__20250922__20251005', 'FAIRTRADE_FORTNIGHT', 'Fairtrade Fortnight', 'Religious & Cultural Moments', 2025, DATE '2025-09-22', DATE '2025-10-05', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('BRITISH_FOOD_FORTNIGHT__20250926__20251012', 'BRITISH_FOOD_FORTNIGHT', 'British Food Fortnight', 'Religious & Cultural Moments', 2025, DATE '2025-09-26', DATE '2025-10-12', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('WORLD_VEGETARIAN_DAY__20251001__20251001', 'WORLD_VEGETARIAN_DAY', 'World Vegetarian Day', 'Religious & Cultural Moments', 2025, DATE '2025-10-01', DATE '2025-10-01', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('DIWALI__20251021__20251021', 'DIWALI', 'Diwali', 'Religious & Cultural Moments', 2025, DATE '2025-10-21', DATE '2025-10-21', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 2, '2025, 2026', 'PARTIAL_DATES'),
        ('HALLOWEEN__20251031__20251031', 'HALLOWEEN', 'Halloween', 'Big Moments', 2025, DATE '2025-10-31', DATE '2025-10-31', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('BONFIRE_NIGHT__20251105__20251105', 'BONFIRE_NIGHT', 'Bonfire Night (Guy Fawkes)', 'Big Moments', 2025, DATE '2025-11-05', DATE '2025-11-05', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('REMEMBRANCE_SUNDAY__20251109__20251109', 'REMEMBRANCE_SUNDAY', 'Remembrance Sunday', 'Religious & Cultural Moments', 2025, DATE '2025-11-09', DATE '2025-11-09', 'SOURCE_EXPLICIT', 'RULE_BASED_ANNUAL_DATE', 1, '2025', 'PARTIAL_DATES'),
        ('INTERNATIONAL_MENS_DAY__20251119__20251119', 'INTERNATIONAL_MENS_DAY', 'International Men''s Day', 'Mini Moments', 2025, DATE '2025-11-19', DATE '2025-11-19', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('WORLD_CHILDRENS_DAY__20251120__20251120', 'WORLD_CHILDRENS_DAY', 'World Children''s Day', 'Mini Moments', 2025, DATE '2025-11-20', DATE '2025-11-20', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('THE_ASHES__20251121__20260108', 'THE_ASHES', 'The Ashes', 'Big Moments', 2025, DATE '2025-11-21', DATE '2026-01-08', 'SOURCE_EXPLICIT', 'PERIODIC_OR_ONE_OFF', 1, '2025', 'PARTIAL_DATES'),
        ('BLACK_FRIDAY__20251128__20251128', 'BLACK_FRIDAY', 'Black Friday', 'Big Moments', 2025, DATE '2025-11-28', DATE '2025-11-28', 'SOURCE_EXPLICIT', 'RULE_BASED_ANNUAL_DATE', 2, '2025, 2026', 'PARTIAL_DATES'),
        ('ST_ANDREWS_DAY__20251130__20251130', 'ST_ANDREWS_DAY', 'St Andrew''s Day', 'Religious & Cultural Moments', 2025, DATE '2025-11-30', DATE '2025-11-30', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('HANUKKAH__20251215__20251222', 'HANUKKAH', 'Hanukkah', 'Religious & Cultural Moments', 2025, DATE '2025-12-15', DATE '2025-12-22', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 1, '2025', 'PARTIAL_DATES'),
        ('CHRISTMAS_DAY__20251225__20251225', 'CHRISTMAS_DAY', 'Christmas Day', 'Big Moments', 2025, DATE '2025-12-25', DATE '2025-12-25', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('BOXING_DAY__20251226__20251226', 'BOXING_DAY', 'Boxing Day', 'Big Moments', 2025, DATE '2025-12-26', DATE '2025-12-26', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('NEW_YEARS_DAY__20260101__20260101', 'NEW_YEARS_DAY', 'New Year''s Day', 'Big Moments', 2026, DATE '2026-01-01', DATE '2026-01-01', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('BURNS_NIGHT__20260125__20260125', 'BURNS_NIGHT', 'Burns Night', 'Religious & Cultural Moments', 2026, DATE '2026-01-25', DATE '2026-01-25', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('RUGBY_SIX_NATIONS__20260205__20260314', 'RUGBY_SIX_NATIONS', 'Rugby Six Nations', 'Big Moments', 2026, DATE '2026-02-05', DATE '2026-03-14', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 2, '2025, 2026', 'PARTIAL_DATES'),
        ('WINTER_OLYMPICS__20260206__20260222', 'WINTER_OLYMPICS', 'Winter Olympics', 'Looking Forward', 2026, DATE '2026-02-06', DATE '2026-02-22', 'SOURCE_EXPLICIT', 'PERIODIC_OR_ONE_OFF', 1, '2026', 'PARTIAL_DATES'),
        ('VALENTINES_GALENTINES__20260213__20260214', 'VALENTINES_GALENTINES', 'Valentine''s Day & Galentine''s Day', 'Big Moments', 2026, DATE '2026-02-13', DATE '2026-02-14', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('PANCAKE_DAY__20260217__20260217', 'PANCAKE_DAY', 'Pancake Day', 'Looking Forward', 2026, DATE '2026-02-17', DATE '2026-02-17', 'SOURCE_EXPLICIT', 'RULE_BASED_ANNUAL_DATE', 1, '2026', 'PARTIAL_DATES'),
        ('RANDOM_ACTS_OF_KINDNESS_DAY__20260217__20260217', 'RANDOM_ACTS_OF_KINDNESS_DAY', 'Random Acts of Kindness Day', 'Religious & Cultural Moments', 2026, DATE '2026-02-17', DATE '2026-02-17', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('ST_DAVIDS_DAY__20260301__20260301', 'ST_DAVIDS_DAY', 'St David''s Day', 'Religious & Cultural Moments', 2026, DATE '2026-03-01', DATE '2026-03-01', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('INTERNATIONAL_WOMENS_DAY__20260308__20260308', 'INTERNATIONAL_WOMENS_DAY', 'International Women''s Day', 'Mini Moments', 2026, DATE '2026-03-08', DATE '2026-03-08', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('ST_PATRICKS_DAY__20260317__20260317', 'ST_PATRICKS_DAY', 'St Patrick''s Day', 'Religious & Cultural Moments', 2026, DATE '2026-03-17', DATE '2026-03-17', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('WORLD_HEALTH_DAY__20260407__20260407', 'WORLD_HEALTH_DAY', 'World Health Day', 'Religious & Cultural Moments', 2026, DATE '2026-04-07', DATE '2026-04-07', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('ST_GEORGES_DAY__20260423__20260423', 'ST_GEORGES_DAY', 'St George''s Day', 'Religious & Cultural Moments', 2026, DATE '2026-04-23', DATE '2026-04-23', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('FIFA_WORLD_CUP__20260611__20260719', 'FIFA_WORLD_CUP', 'FIFA World Cup', 'Looking Forward', 2026, DATE '2026-06-11', DATE '2026-07-19', 'SOURCE_EXPLICIT', 'PERIODIC_OR_ONE_OFF', 1, '2026', 'PARTIAL_DATES'),
        ('JUNETEENTH__20260619__20260619', 'JUNETEENTH', 'Juneteenth', 'Religious & Cultural Moments', 2026, DATE '2026-06-19', DATE '2026-06-19', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('WIMBLEDON__20260629__20260712', 'WIMBLEDON', 'Wimbledon', 'Looking Forward', 2026, DATE '2026-06-29', DATE '2026-07-12', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 2, '2025, 2026', 'PARTIAL_DATES'),
        ('BRITISH_GRAND_PRIX__20260705__20260705', 'BRITISH_GRAND_PRIX', 'British Grand Prix', 'Looking Forward', 2026, DATE '2026-07-05', DATE '2026-07-05', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 2, '2025, 2026', 'PARTIAL_DATES'),
        ('THE_OPEN_CHAMPIONSHIP__20260716__20260719', 'THE_OPEN_CHAMPIONSHIP', 'The Open Championship', 'Looking Forward', 2026, DATE '2026-07-16', DATE '2026-07-19', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 2, '2025, 2026', 'PARTIAL_DATES'),
        ('COMMONWEALTH_GAMES__20260723__20260802', 'COMMONWEALTH_GAMES', 'Commonwealth Games', 'Looking Forward', 2026, DATE '2026-07-23', DATE '2026-08-02', 'SOURCE_EXPLICIT', 'PERIODIC_OR_ONE_OFF', 1, '2026', 'PARTIAL_DATES'),
        ('SUMMER_BANK_HOLIDAY__20260831__20260831', 'SUMMER_BANK_HOLIDAY', 'Summer / August Bank Holiday', 'Looking Forward', 2026, DATE '2026-08-31', DATE '2026-08-31', 'SOURCE_EXPLICIT', 'RULE_BASED_ANNUAL_DATE', 2, '2025, 2026', 'PARTIAL_DATES'),
        ('WORLD_VEGETARIAN_DAY__20261001__20261001', 'WORLD_VEGETARIAN_DAY', 'World Vegetarian Day', 'Religious & Cultural Moments', 2026, DATE '2026-10-01', DATE '2026-10-01', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('HALLOWEEN__20261031__20261031', 'HALLOWEEN', 'Halloween', 'Big Moments', 2026, DATE '2026-10-31', DATE '2026-10-31', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('BONFIRE_NIGHT__20261105__20261105', 'BONFIRE_NIGHT', 'Bonfire Night (Guy Fawkes)', 'Big Moments', 2026, DATE '2026-11-05', DATE '2026-11-05', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('DIWALI__20261108__20261108', 'DIWALI', 'Diwali', 'Looking Forward', 2026, DATE '2026-11-08', DATE '2026-11-08', 'SOURCE_EXPLICIT', 'EXPLICIT_YEARLY_DATE_REQUIRED', 2, '2025, 2026', 'PARTIAL_DATES'),
        ('INTERNATIONAL_MENS_DAY__20261119__20261119', 'INTERNATIONAL_MENS_DAY', 'International Men''s Day', 'Mini Moments', 2026, DATE '2026-11-19', DATE '2026-11-19', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('WORLD_CHILDRENS_DAY__20261120__20261120', 'WORLD_CHILDRENS_DAY', 'World Children''s Day', 'Mini Moments', 2026, DATE '2026-11-20', DATE '2026-11-20', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('BLACK_FRIDAY__20261127__20261127', 'BLACK_FRIDAY', 'Black Friday', 'Looking Forward', 2026, DATE '2026-11-27', DATE '2026-11-27', 'SOURCE_EXPLICIT', 'RULE_BASED_ANNUAL_DATE', 2, '2025, 2026', 'PARTIAL_DATES'),
        ('ST_ANDREWS_DAY__20261130__20261130', 'ST_ANDREWS_DAY', 'St Andrew''s Day', 'Religious & Cultural Moments', 2026, DATE '2026-11-30', DATE '2026-11-30', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('CHRISTMAS_DAY__20261225__20261225', 'CHRISTMAS_DAY', 'Christmas Day', 'Looking Forward', 2026, DATE '2026-12-25', DATE '2026-12-25', 'SOURCE_EXPLICIT', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026'),
        ('BOXING_DAY__20261226__20261226', 'BOXING_DAY', 'Boxing Day', 'Big Moments', 2026, DATE '2026-12-26', DATE '2026-12-26', 'SAFE_RULE_EXPANSION', 'FIXED_ANNUAL_DATE', 4, '2023, 2024, 2025, 2026', 'COMPLETE_2023_2026')
    AS ec(
        event_occurrence_id,
        occasion_key,
        occasion_name,
        occasion_group,
        event_year,
        event_start_date,
        event_end_date,
        date_origin,
        recurrence_handling,
        calendar_available_year_count,
        calendar_available_years,
        calendar_completeness
    )
),

analysis_years AS (
    SELECT EXPLODE(SEQUENCE(2023, 2026)) AS y
),

-- Easter Sunday by the anonymous Gregorian algorithm (Meeus / Jones / Butcher).
easter_step1 AS (
    SELECT y, y % 19 AS a, y DIV 100 AS b, y % 100 AS c
    FROM analysis_years
),
easter_step2 AS (
    SELECT y, a, b, c, b DIV 4 AS d, b % 4 AS e, (b + 8) DIV 25 AS f, c DIV 4 AS i, c % 4 AS k
    FROM easter_step1
),
easter_step3 AS (
    SELECT y, a, e, i, k, (19 * a + b - d - ((b - f + 1) DIV 3) + 15) % 30 AS h
    FROM easter_step2
),
easter_step4 AS (
    SELECT y, a, h, (32 + 2 * e + 2 * i - h - k) % 7 AS l
    FROM easter_step3
),
easter AS (
    SELECT
        y,
        MAKE_DATE(
            y,
            CAST((h + l - 7 * ((a + 11 * h + 22 * l) DIV 451) + 114) DIV 31 AS INT),
            CAST(((h + l - 7 * ((a + 11 * h + 22 * l) DIV 451) + 114) % 31) + 1 AS INT)
        ) AS easter_sunday
    FROM easter_step4
),

computed_rule_dates AS (
    SELECT
        r.occasion_key,
        r.occasion_name,
        r.occasion_group,
        e.y AS event_year,
        r.start_date AS event_start_date,
        r.end_date AS event_end_date,
        'RULE_COMPUTED_V2' AS date_origin,
        r.recurrence_handling
    FROM easter e
    LATERAL VIEW INLINE(ARRAY(
        NAMED_STRUCT('k', 'EASTER_SUNDAY', 'n', 'Easter Sunday', 'g', 'Big Moments',
                     's', e.easter_sunday, 'e', e.easter_sunday, 'r', 'RULE_BASED_ANNUAL_DATE'),
        NAMED_STRUCT('k', 'MOTHERS_DAY', 'n', 'Mother''s Day', 'g', 'Big Moments',
                     's', DATE_SUB(e.easter_sunday, 21), 'e', DATE_SUB(e.easter_sunday, 21), 'r', 'RULE_BASED_ANNUAL_DATE'),
        NAMED_STRUCT('k', 'PANCAKE_DAY', 'n', 'Pancake Day', 'g', 'Big Moments',
                     's', DATE_SUB(e.easter_sunday, 47), 'e', DATE_SUB(e.easter_sunday, 47), 'r', 'RULE_BASED_ANNUAL_DATE'),
        NAMED_STRUCT('k', 'FATHERS_DAY', 'n', 'Father''s Day', 'g', 'Big Moments',
                     's', DATE_ADD(NEXT_DAY(MAKE_DATE(e.y, 5, 31), 'SU'), 14),
                     'e', DATE_ADD(NEXT_DAY(MAKE_DATE(e.y, 5, 31), 'SU'), 14), 'r', 'RULE_BASED_ANNUAL_DATE'),
        NAMED_STRUCT('k', 'EARLY_MAY_BANK_HOLIDAY', 'n', 'Early May Bank Holiday', 'g', 'Big Moments',
                     's', NEXT_DAY(MAKE_DATE(e.y, 4, 30), 'MO'), 'e', NEXT_DAY(MAKE_DATE(e.y, 4, 30), 'MO'), 'r', 'RULE_BASED_ANNUAL_DATE'),
        NAMED_STRUCT('k', 'SPRING_BANK_HOLIDAY', 'n', 'Spring Bank Holiday', 'g', 'Big Moments',
                     's', NEXT_DAY(MAKE_DATE(e.y, 5, 24), 'MO'), 'e', NEXT_DAY(MAKE_DATE(e.y, 5, 24), 'MO'), 'r', 'RULE_BASED_ANNUAL_DATE'),
        NAMED_STRUCT('k', 'SUMMER_BANK_HOLIDAY', 'n', 'Summer / August Bank Holiday', 'g', 'Big Moments',
                     's', NEXT_DAY(MAKE_DATE(e.y, 8, 24), 'MO'), 'e', NEXT_DAY(MAKE_DATE(e.y, 8, 24), 'MO'), 'r', 'RULE_BASED_ANNUAL_DATE'),
        NAMED_STRUCT('k', 'BLACK_FRIDAY', 'n', 'Black Friday', 'g', 'Big Moments',
                     's', DATE_ADD(NEXT_DAY(MAKE_DATE(e.y, 10, 31), 'TH'), 22),
                     'e', DATE_ADD(NEXT_DAY(MAKE_DATE(e.y, 10, 31), 'TH'), 22), 'r', 'RULE_BASED_ANNUAL_DATE'),
        NAMED_STRUCT('k', 'REMEMBRANCE_SUNDAY', 'n', 'Remembrance Sunday', 'g', 'Religious & Cultural Moments',
                     's', DATE_ADD(NEXT_DAY(MAKE_DATE(e.y, 10, 31), 'SU'), 7),
                     'e', DATE_ADD(NEXT_DAY(MAKE_DATE(e.y, 10, 31), 'SU'), 7), 'r', 'RULE_BASED_ANNUAL_DATE'),
        NAMED_STRUCT('k', 'WORLD_BOOK_DAY_UK_IRELAND', 'n', 'World Book Day (UK & Ireland)', 'g', 'Religious & Cultural Moments',
                     's', NEXT_DAY(DATE_SUB(MAKE_DATE(e.y, 3, 1), 1), 'TH'),
                     'e', NEXT_DAY(DATE_SUB(MAKE_DATE(e.y, 3, 1), 1), 'TH'), 'r', 'RULE_BASED_ANNUAL_DATE'),
        NAMED_STRUCT('k', 'BACK_TO_SCHOOL_UNIVERSITY', 'n', 'Back to School / University', 'g', 'Big Moments',
                     's', MAKE_DATE(e.y, 9, 1), 'e', MAKE_DATE(e.y, 9, 30), 'r', 'APPROXIMATE_BUSINESS_WINDOW'),
        NAMED_STRUCT('k', 'NEW_YEARS_EVE', 'n', 'New Year''s Eve', 'g', 'Big Moments',
                     's', MAKE_DATE(e.y, 12, 31), 'e', MAKE_DATE(e.y, 12, 31), 'r', 'FIXED_ANNUAL_DATE')
    )) r AS occasion_key, occasion_name, occasion_group, start_date, end_date, recurrence_handling
),

supplementary_dates AS (
    SELECT
        occasion_key,
        occasion_name,
        occasion_group,
        event_year,
        event_start_date,
        event_end_date,
        'SUPPLEMENT_V2_VERIFY' AS date_origin,
        'EXPLICIT_YEARLY_DATE_REQUIRED' AS recurrence_handling
    FROM VALUES
        ('LUNAR_NEW_YEAR',    'Chinese / Lunar New Year', 'Religious & Cultural Moments', 2023, DATE '2023-01-22', DATE '2023-01-22'),
        ('LUNAR_NEW_YEAR',    'Chinese / Lunar New Year', 'Religious & Cultural Moments', 2024, DATE '2024-02-10', DATE '2024-02-10'),
        ('LUNAR_NEW_YEAR',    'Chinese / Lunar New Year', 'Religious & Cultural Moments', 2026, DATE '2026-02-17', DATE '2026-02-17'),
        ('DIWALI',            'Diwali',                   'Religious & Cultural Moments', 2023, DATE '2023-11-12', DATE '2023-11-12'),
        ('DIWALI',            'Diwali',                   'Religious & Cultural Moments', 2024, DATE '2024-11-01', DATE '2024-11-01'),
        ('RAMADAN',           'Ramadan',                  'Religious & Cultural Moments', 2023, DATE '2023-03-23', DATE '2023-04-20'),
        ('RAMADAN',           'Ramadan',                  'Religious & Cultural Moments', 2024, DATE '2024-03-11', DATE '2024-04-09'),
        ('RAMADAN',           'Ramadan',                  'Religious & Cultural Moments', 2026, DATE '2026-02-18', DATE '2026-03-19'),
        ('EID_AL_FITR',       'Eid al-Fitr',              'Religious & Cultural Moments', 2023, DATE '2023-04-21', DATE '2023-04-22'),
        ('EID_AL_FITR',       'Eid al-Fitr',              'Religious & Cultural Moments', 2024, DATE '2024-04-10', DATE '2024-04-11'),
        ('EID_AL_FITR',       'Eid al-Fitr',              'Religious & Cultural Moments', 2026, DATE '2026-03-20', DATE '2026-03-21'),
        ('HOLI',              'Holi',                     'Religious & Cultural Moments', 2023, DATE '2023-03-08', DATE '2023-03-08'),
        ('HOLI',              'Holi',                     'Religious & Cultural Moments', 2024, DATE '2024-03-25', DATE '2024-03-25'),
        ('HOLI',              'Holi',                     'Religious & Cultural Moments', 2026, DATE '2026-03-04', DATE '2026-03-04'),
        ('PASSOVER',          'Passover',                 'Religious & Cultural Moments', 2023, DATE '2023-04-06', DATE '2023-04-13'),
        ('PASSOVER',          'Passover',                 'Religious & Cultural Moments', 2024, DATE '2024-04-23', DATE '2024-04-30'),
        ('PASSOVER',          'Passover',                 'Religious & Cultural Moments', 2026, DATE '2026-04-02', DATE '2026-04-09'),
        ('HANUKKAH',          'Hanukkah',                 'Religious & Cultural Moments', 2023, DATE '2023-12-08', DATE '2023-12-15'),
        ('HANUKKAH',          'Hanukkah',                 'Religious & Cultural Moments', 2024, DATE '2024-12-26', DATE '2025-01-02'),
        ('WIMBLEDON',         'Wimbledon',                'Big Moments',                  2023, DATE '2023-07-03', DATE '2023-07-16'),
        ('WIMBLEDON',         'Wimbledon',                'Big Moments',                  2024, DATE '2024-07-01', DATE '2024-07-14'),
        ('RUGBY_SIX_NATIONS', 'Rugby Six Nations',        'Big Moments',                  2023, DATE '2023-02-04', DATE '2023-03-18'),
        ('RUGBY_SIX_NATIONS', 'Rugby Six Nations',        'Big Moments',                  2024, DATE '2024-02-02', DATE '2024-03-16'),
        ('GRAND_NATIONAL',    'Grand National',           'Big Moments',                  2023, DATE '2023-04-15', DATE '2023-04-15'),
        ('GRAND_NATIONAL',    'Grand National',           'Big Moments',                  2024, DATE '2024-04-13', DATE '2024-04-13'),
        ('GRAND_NATIONAL',    'Grand National',           'Big Moments',                  2026, DATE '2026-04-11', DATE '2026-04-11'),
        ('FA_CUP_FINAL',      'FA Cup Final',             'Big Moments',                  2023, DATE '2023-06-03', DATE '2023-06-03'),
        ('FA_CUP_FINAL',      'FA Cup Final',             'Big Moments',                  2024, DATE '2024-05-25', DATE '2024-05-25'),
        ('FA_CUP_FINAL',      'FA Cup Final',             'Big Moments',                  2026, DATE '2026-05-16', DATE '2026-05-16')
    AS s(occasion_key, occasion_name, occasion_group, event_year, event_start_date, event_end_date)
),

all_dated_rows AS (
    SELECT * FROM source_calendar
    UNION ALL
    SELECT
        CONCAT(occasion_key, '__', DATE_FORMAT(event_start_date, 'yyyyMMdd'), '__', DATE_FORMAT(event_end_date, 'yyyyMMdd')),
        occasion_key, occasion_name, occasion_group, event_year,
        event_start_date, event_end_date, date_origin, recurrence_handling
    FROM computed_rule_dates
    UNION ALL
    SELECT
        CONCAT(occasion_key, '__', DATE_FORMAT(event_start_date, 'yyyyMMdd'), '__', DATE_FORMAT(event_end_date, 'yyyyMMdd')),
        occasion_key, occasion_name, occasion_group, event_year,
        event_start_date, event_end_date, date_origin, recurrence_handling
    FROM supplementary_dates
),

ranked AS (
    SELECT
        a.*,
        ROW_NUMBER() OVER (
            PARTITION BY a.occasion_key, a.event_year
            ORDER BY
                CASE a.date_origin
                    WHEN 'SOURCE_EXPLICIT'      THEN 1
                    WHEN 'SAFE_RULE_EXPANSION'  THEN 2
                    WHEN 'RULE_COMPUTED_V2'     THEN 3
                    WHEN 'SUPPLEMENT_V2_VERIFY' THEN 4
                    ELSE 5
                END,
                a.event_start_date,
                a.event_end_date,
                a.event_occurrence_id
        ) AS calendar_rn
    FROM all_dated_rows a
    WHERE a.recurrence_handling <> 'FIXED_ANNUAL_MONTH'
      AND a.event_year BETWEEN 2023 AND 2026
      AND a.event_start_date <= a.event_end_date
)

SELECT
    event_occurrence_id,
    occasion_key,
    occasion_name,
    occasion_group,
    event_year,
    event_start_date,
    event_end_date,
    date_origin,
    recurrence_handling
FROM ranked
WHERE calendar_rn = 1;


-- =====================================================================================
-- 5  CALENDAR DAYS  (every day in the data: market totals and a low-traffic flag)
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_calendar_days;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_calendar_days
USING PARQUET AS
WITH coverage AS (
    SELECT
        MIN(sale_date) AS data_start_date,
        LEAST(MAX(sale_date), CURRENT_DATE()) AS data_through_date
    FROM lab_customer_value_analytics.seasonality_v2_daily_fact
    WHERE entity_type = 'ALL'
      AND baskets > 0
),

days AS (
    SELECT EXPLODE(SEQUENCE(data_start_date, data_through_date)) AS cal_date
    FROM coverage
),

day_totals AS (
    SELECT sale_date, baskets AS all_baskets, items AS all_items, sales AS all_sales
    FROM lab_customer_value_analytics.seasonality_v2_daily_fact
    WHERE entity_type = 'ALL'
),

day_outlets AS (
    SELECT sale_date, COUNT(*) AS active_outlets
    FROM lab_customer_value_analytics.seasonality_v2_daily_fact
    WHERE entity_type = 'OUTLET'
    GROUP BY sale_date
),

joined AS (
    SELECT
        d.cal_date,
        YEAR(d.cal_date)            AS cal_year,
        DAYOFWEEK(d.cal_date)       AS weekday,
        COALESCE(t.all_baskets, 0)  AS all_baskets,
        COALESCE(t.all_items, 0)    AS all_items,
        COALESCE(t.all_sales, 0.0)  AS all_sales,
        COALESCE(o.active_outlets, 0) AS active_outlets
    FROM days d
    LEFT JOIN day_totals t ON d.cal_date = t.sale_date
    LEFT JOIN day_outlets o ON d.cal_date = o.sale_date
),

weekday_norm AS (
    SELECT cal_year, weekday, PERCENTILE_APPROX(all_baskets, 0.5) AS median_weekday_baskets
    FROM joined
    GROUP BY cal_year, weekday
)

SELECT
    j.cal_date,
    j.cal_year,
    j.weekday,
    j.all_baskets,
    j.all_items,
    j.all_sales,
    j.active_outlets,
    n.median_weekday_baskets,
    CASE WHEN j.all_baskets < p.low_traffic_ratio * n.median_weekday_baskets THEN 'Y' ELSE 'N' END
        AS is_low_traffic_day,
    c.data_start_date,
    c.data_through_date
FROM joined j
JOIN weekday_norm n
  ON j.cal_year = n.cal_year
 AND j.weekday = n.weekday
CROSS JOIN coverage c
CROSS JOIN lab_customer_value_analytics.seasonality_v2_params p;


-- =====================================================================================
-- 5b  BUILD-UP CALIBRATION  (when does the extra demand for an occasion really start?)
--     Runs for occasions with buildup_search_days set (statement 3), on subclasses,
--     before any product is flagged, so it does not lean on the fixed windows it adjusts.
--     For each completed occasion year:
--       1. Responders: subclasses whose share of items in the occasion's own window
--          (pre-event + event) is at least uplift_index_min x their baseline and
--          significant (z >= z_min), or that are concentrated there: at least
--          min_specific_baskets item slots and specific_ratio_min x the slots expected
--          from the baseline (this catches seasonal ranges with little off-season sale).
--          A subclass that responds to two occasions whose search ranges overlap (Easter
--          eggs at Mother's Day, Christmas lines at Halloween) counts only for the one
--          where its share of items in the window is highest.
--       2. Baseline: the nearest 28 usable days before the search range (7-day gap).
--          Usable = not a low-traffic day and not inside another major occasion's window
--          or search range. If that finds fewer than min_baseline_days, the nearest 28
--          usable days after the event (7-day gap) are added.
--       3. Daily extra demand = responders' item slots - expected slots (weekday-matched,
--          as in statement 11), summed, from the start of the search range to the end of
--          the event. Days inside another major occasion's window count as zero.
--       4. Shopping start = the day after which the running total of extra demand stays
--          at or above timing_start_pct (10%) of the year's total. Midpoint (50%) and end
--          (90%) work the same way. Early noise that touches 10% and falls back is ignored.
--     calibrated_lead_days = median over valid years of (event start - shopping start).
--     has_buildup = Y when at least min_years years are valid and the calibrated start is
--     at least min_buildup_days earlier than the occasion's own pre-event window.
--     Statement 6 then adds "<occasion> build-up" from the calibrated start to the day
--     before the pre-event window. top_responder_subclasses shows what drives each year:
--     review it, and set buildup_search_days to NULL for an occasion whose build-up is
--     really another occasion's season (for example Diwali inside the Christmas build-up).
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_buildup_calibration;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_buildup_calibration
USING PARQUET AS
WITH prm AS (
    SELECT * FROM lab_customer_value_analytics.seasonality_v2_params
),

coverage AS (
    SELECT MIN(data_start_date) AS data_start_date, MIN(data_through_date) AS data_through_date
    FROM lab_customer_value_analytics.seasonality_v2_calendar_days
),

occasion_setup AS (
    SELECT
        c.occasion_key,
        c.occasion_name,
        c.event_year,
        c.event_start_date,
        c.event_end_date,
        COALESCE(g.is_major, 'N')  AS is_major,
        COALESCE(g.lead_days, 2)   AS lead_days,
        COALESCE(g.lag_days, 0)    AS lag_days,
        g.buildup_search_days
    FROM lab_customer_value_analytics.seasonality_v2_event_calendar c
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_occasion_config g
      ON c.occasion_key = g.occasion_key
),

-- Days that belong to major occasions: their own window (kept out of every other
-- occasion's demand curve) and, wider, their possible build-up (kept out of baselines).
major_days AS (
    SELECT
        t.major_date,
        o.occasion_key,
        MAX(CASE WHEN t.major_date >= DATE_SUB(o.event_start_date, o.lead_days) THEN 1 ELSE 0 END) AS in_window
    FROM occasion_setup o
    LATERAL VIEW EXPLODE(SEQUENCE(
        DATE_SUB(o.event_start_date, GREATEST(o.lead_days, COALESCE(o.buildup_search_days, 0))),
        DATE_ADD(o.event_end_date, o.lag_days))) t AS major_date
    WHERE o.is_major = 'Y'
    GROUP BY t.major_date, o.occasion_key
),

major_day_keys AS (
    SELECT
        major_date,
        COLLECT_SET(occasion_key)                                   AS search_keys,
        COLLECT_SET(CASE WHEN in_window = 1 THEN occasion_key END)  AS window_keys
    FROM major_days
    GROUP BY major_date
),

calibration_occurrences AS (
    SELECT
        o.*,
        DATE_SUB(o.event_start_date, o.buildup_search_days) AS range_start,
        DATE_SUB(o.event_start_date, o.lead_days)           AS window_start,
        p.baseline_max_scan_days                            AS scan_days
    FROM occasion_setup o
    CROSS JOIN coverage cv
    CROSS JOIN prm p
    WHERE o.buildup_search_days IS NOT NULL
      AND o.buildup_search_days > o.lead_days
      AND o.event_end_date <= cv.data_through_date
      AND DATE_SUB(o.event_start_date, o.buildup_search_days) >= cv.data_start_date
),

calibration_days AS (
    SELECT o.occasion_key, o.event_year, 'BEFORE' AS role, t.role_date
    FROM calibration_occurrences o
    LATERAL VIEW EXPLODE(SEQUENCE(DATE_SUB(o.range_start, 7 + o.scan_days), DATE_SUB(o.range_start, 8))) t AS role_date
    UNION ALL
    SELECT o.occasion_key, o.event_year, 'AFTER', t.role_date
    FROM calibration_occurrences o
    LATERAL VIEW EXPLODE(SEQUENCE(DATE_ADD(o.event_end_date, o.lag_days + 8),
                                  DATE_ADD(o.event_end_date, o.lag_days + 7 + o.scan_days))) t AS role_date
    UNION ALL
    SELECT o.occasion_key, o.event_year,
           CASE WHEN t.role_date >= o.window_start THEN 'WINDOW' ELSE 'SEARCH' END, t.role_date
    FROM calibration_occurrences o
    LATERAL VIEW EXPLODE(SEQUENCE(o.range_start, o.event_end_date)) t AS role_date
),

calibration_days_flagged AS (
    SELECT /*+ BROADCAST(k) */
        d.occasion_key,
        d.event_year,
        d.role,
        d.role_date,
        c.weekday,
        c.all_items,
        c.is_low_traffic_day,
        COALESCE(SIZE(FILTER(k.search_keys, x -> x <> d.occasion_key)) > 0, FALSE) AS in_other_search,
        COALESCE(SIZE(FILTER(k.window_keys, x -> x <> d.occasion_key)) > 0, FALSE) AS in_other_window
    FROM calibration_days d
    JOIN lab_customer_value_analytics.seasonality_v2_calendar_days c
      ON d.role_date = c.cal_date
    LEFT JOIN major_day_keys k
      ON d.role_date = k.major_date
),

base_ranked AS (
    SELECT
        f.*,
        ROW_NUMBER() OVER (
            PARTITION BY f.occasion_key, f.event_year, f.role
            ORDER BY CASE WHEN f.role = 'BEFORE' THEN DATEDIFF(DATE '2100-01-01', f.role_date)
                          ELSE DATEDIFF(f.role_date, DATE '1900-01-01') END
        ) AS nearest_rank
    FROM calibration_days_flagged f
    WHERE f.role IN ('BEFORE', 'AFTER')
      AND f.is_low_traffic_day = 'N'
      AND NOT f.in_other_search
),

base_before_counts AS (
    SELECT occasion_key, event_year,
           SUM(CASE WHEN role = 'BEFORE' AND nearest_rank <= 28 THEN 1 ELSE 0 END) AS baseline_days_before
    FROM base_ranked
    GROUP BY occasion_key, event_year
),

day_roles AS (
    SELECT occasion_key, event_year, role, role_date, weekday, all_items, in_other_window
    FROM calibration_days_flagged
    WHERE role IN ('SEARCH', 'WINDOW')
    UNION ALL
    SELECT b.occasion_key, b.event_year, 'BASE', b.role_date, b.weekday, b.all_items, b.in_other_window
    FROM base_ranked b
    JOIN base_before_counts n
      ON b.occasion_key = n.occasion_key
     AND b.event_year = n.event_year
    CROSS JOIN prm p
    WHERE b.nearest_rank <= 28
      AND (b.role = 'BEFORE' OR n.baseline_days_before < p.min_baseline_days)
),

-- calendar side: baseline days per weekday and window items per weekday
occasion_base AS (
    SELECT
        occasion_key,
        event_year,
        COUNT(*) AS baseline_days,
        ARRAY(
            SUM(CASE WHEN weekday = 1 THEN 1 ELSE 0 END), SUM(CASE WHEN weekday = 2 THEN 1 ELSE 0 END),
            SUM(CASE WHEN weekday = 3 THEN 1 ELSE 0 END), SUM(CASE WHEN weekday = 4 THEN 1 ELSE 0 END),
            SUM(CASE WHEN weekday = 5 THEN 1 ELSE 0 END), SUM(CASE WHEN weekday = 6 THEN 1 ELSE 0 END),
            SUM(CASE WHEN weekday = 7 THEN 1 ELSE 0 END)
        ) AS base_days_by_dow
    FROM day_roles
    WHERE role = 'BASE'
    GROUP BY occasion_key, event_year
),

occasion_window AS (
    SELECT
        occasion_key,
        event_year,
        ARRAY(
            SUM(CASE WHEN weekday = 1 THEN all_items ELSE 0 END), SUM(CASE WHEN weekday = 2 THEN all_items ELSE 0 END),
            SUM(CASE WHEN weekday = 3 THEN all_items ELSE 0 END), SUM(CASE WHEN weekday = 4 THEN all_items ELSE 0 END),
            SUM(CASE WHEN weekday = 5 THEN all_items ELSE 0 END), SUM(CASE WHEN weekday = 6 THEN all_items ELSE 0 END),
            SUM(CASE WHEN weekday = 7 THEN all_items ELSE 0 END)
        ) AS window_items_by_dow,
        SUM(CAST(all_items AS DOUBLE) * all_items) AS window_items_sq,
        SUM(all_items) AS window_items
    FROM day_roles
    WHERE role = 'WINDOW'
    GROUP BY occasion_key, event_year
),

-- subclass x day item slots (same unit as statement 9's subclass rows)
subclass_daily AS (
    SELECT /*+ BROADCAST(p) */
        t.sale_date,
        p.subclass_entity_id AS entity_id,
        SUM(t.baskets)       AS slots
    FROM lab_customer_value_analytics.seasonality_v2_daily_fact t
    JOIN lab_customer_value_analytics.seasonality_v2_product_dim p
      ON TRY_CAST(t.entity_id AS BIGINT) = p.tpnb
    WHERE t.entity_type = 'TPNB'
      AND t.entity_id <> 'NULL_TPNB'
      AND p.subclass_entity_id IS NOT NULL
    GROUP BY t.sale_date, p.subclass_entity_id
),

subclass_roles AS (
    SELECT /*+ BROADCAST(r) */
        s.entity_id,
        r.occasion_key,
        r.event_year,
        r.role,
        r.weekday,
        s.slots,
        s.slots / NULLIF(r.all_items, 0) AS item_share
    FROM subclass_daily s
    JOIN day_roles r
      ON s.sale_date = r.role_date
),

subclass_occasion AS (
    SELECT
        entity_id,
        occasion_key,
        event_year,
        ARRAY(
            SUM(CASE WHEN role = 'BASE' AND weekday = 1 THEN item_share ELSE CAST(0 AS DOUBLE) END),
            SUM(CASE WHEN role = 'BASE' AND weekday = 2 THEN item_share ELSE CAST(0 AS DOUBLE) END),
            SUM(CASE WHEN role = 'BASE' AND weekday = 3 THEN item_share ELSE CAST(0 AS DOUBLE) END),
            SUM(CASE WHEN role = 'BASE' AND weekday = 4 THEN item_share ELSE CAST(0 AS DOUBLE) END),
            SUM(CASE WHEN role = 'BASE' AND weekday = 5 THEN item_share ELSE CAST(0 AS DOUBLE) END),
            SUM(CASE WHEN role = 'BASE' AND weekday = 6 THEN item_share ELSE CAST(0 AS DOUBLE) END),
            SUM(CASE WHEN role = 'BASE' AND weekday = 7 THEN item_share ELSE CAST(0 AS DOUBLE) END)
        ) AS share_sum_by_dow,
        ARRAY(
            SUM(CASE WHEN role = 'BASE' AND weekday = 1 THEN item_share * item_share ELSE CAST(0 AS DOUBLE) END),
            SUM(CASE WHEN role = 'BASE' AND weekday = 2 THEN item_share * item_share ELSE CAST(0 AS DOUBLE) END),
            SUM(CASE WHEN role = 'BASE' AND weekday = 3 THEN item_share * item_share ELSE CAST(0 AS DOUBLE) END),
            SUM(CASE WHEN role = 'BASE' AND weekday = 4 THEN item_share * item_share ELSE CAST(0 AS DOUBLE) END),
            SUM(CASE WHEN role = 'BASE' AND weekday = 5 THEN item_share * item_share ELSE CAST(0 AS DOUBLE) END),
            SUM(CASE WHEN role = 'BASE' AND weekday = 6 THEN item_share * item_share ELSE CAST(0 AS DOUBLE) END),
            SUM(CASE WHEN role = 'BASE' AND weekday = 7 THEN item_share * item_share ELSE CAST(0 AS DOUBLE) END)
        ) AS share_sq_by_dow,
        SUM(CASE WHEN role = 'WINDOW' THEN slots ELSE 0 END) AS window_slots
    FROM subclass_roles
    GROUP BY entity_id, occasion_key, event_year
),

subclass_model AS (
    SELECT
        s.*,
        b.baseline_days,
        b.base_days_by_dow,
        w.window_items_by_dow,
        w.window_items_sq,
        w.window_items,
        AGGREGATE(s.share_sum_by_dow, CAST(0 AS DOUBLE), (acc, x) -> acc + x) AS base_share_total,
        AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
            acc + CASE WHEN ELEMENT_AT(b.base_days_by_dow, wd) > 0
                       THEN ELEMENT_AT(s.share_sq_by_dow, wd)
                            - POW(ELEMENT_AT(s.share_sum_by_dow, wd), 2) / ELEMENT_AT(b.base_days_by_dow, wd)
                       ELSE CAST(0 AS DOUBLE) END
        ) AS share_ss_within,
        SIZE(FILTER(b.base_days_by_dow, x -> x > 0)) AS baseline_weekdays
    FROM subclass_occasion s
    JOIN occasion_base b
      ON s.occasion_key = b.occasion_key
     AND s.event_year = b.event_year
    JOIN occasion_window w
      ON s.occasion_key = w.occasion_key
     AND s.event_year = w.event_year
),

subclass_model2 AS (
    SELECT
        m.*,
        TRANSFORM(SEQUENCE(1, 7), wd ->
            CASE WHEN ELEMENT_AT(m.base_days_by_dow, wd) > 0
                 THEN ELEMENT_AT(m.share_sum_by_dow, wd) / ELEMENT_AT(m.base_days_by_dow, wd)
                 ELSE m.base_share_total / NULLIF(m.baseline_days, 0) END
        ) AS mean_share_by_dow,
        TRANSFORM(SEQUENCE(1, 7), wd ->
            CAST(CASE WHEN ELEMENT_AT(m.base_days_by_dow, wd) > 0
                      THEN ELEMENT_AT(m.base_days_by_dow, wd)
                      ELSE m.baseline_days END AS DOUBLE)
        ) AS mean_days_by_dow,
        CASE WHEN m.baseline_days - m.baseline_weekdays > 0
             THEN GREATEST(m.share_ss_within, CAST(0 AS DOUBLE)) / (m.baseline_days - m.baseline_weekdays)
        END AS share_variance
    FROM subclass_model m
),

subclass_test AS (
    SELECT
        t.*,
        AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
            acc + ELEMENT_AT(t.window_items_by_dow, wd) * ELEMENT_AT(t.mean_share_by_dow, wd)) AS window_expected_slots,
        SQRT(COALESCE(t.share_variance, CAST(0 AS DOUBLE)) * (t.window_items_sq +
            AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
                acc + POW(ELEMENT_AT(t.window_items_by_dow, wd), 2) / ELEMENT_AT(t.mean_days_by_dow, wd)))) AS window_model_sd
    FROM subclass_model2 t
),

responder_candidates AS (
    SELECT r.entity_id, r.occasion_key, r.event_year, r.mean_share_by_dow,
           r.window_slots / NULLIF(r.window_items, 0) AS window_share
    FROM subclass_test r
    CROSS JOIN prm p
    WHERE r.baseline_days >= p.min_baseline_days
      AND (
            (r.window_expected_slots >= p.min_expected_baskets
             AND r.window_slots / NULLIF(r.window_expected_slots, 0) >= p.uplift_index_min
             AND (r.window_slots - r.window_expected_slots)
                 / NULLIF(GREATEST(r.window_model_sd, SQRT(r.window_expected_slots)), 0) >= p.z_min)
         OR (r.window_slots >= p.min_specific_baskets
             AND r.window_slots >= p.specific_ratio_min * r.window_expected_slots)
      )
),

-- occasion years whose search ranges overlap each other
calibration_overlaps AS (
    SELECT a.occasion_key AS key_a, a.event_year AS year_a, b.occasion_key AS key_b, b.event_year AS year_b
    FROM calibration_occurrences a
    JOIN calibration_occurrences b
      ON a.occasion_key <> b.occasion_key
     AND a.range_start <= b.event_end_date
     AND b.range_start <= a.event_end_date
),

-- a candidate is dropped when the same subclass has a higher window share for an
-- overlapping occasion year
beaten AS (
    SELECT DISTINCT c1.entity_id, c1.occasion_key, c1.event_year
    FROM responder_candidates c1
    JOIN calibration_overlaps o
      ON c1.occasion_key = o.key_a
     AND c1.event_year = o.year_a
    JOIN responder_candidates c2
      ON c2.entity_id = c1.entity_id
     AND c2.occasion_key = o.key_b
     AND c2.event_year = o.year_b
    WHERE c2.window_share > c1.window_share
),

responders AS (
    SELECT c.entity_id, c.occasion_key, c.event_year, c.mean_share_by_dow
    FROM responder_candidates c
    LEFT ANTI JOIN beaten b
      ON c.entity_id = b.entity_id
     AND c.occasion_key = b.occasion_key
     AND c.event_year = b.event_year
),

responder_days AS (
    SELECT /*+ BROADCAST(d) */
        r.entity_id,
        r.occasion_key,
        r.event_year,
        d.role_date,
        d.in_other_window,
        d.all_items * ELEMENT_AT(r.mean_share_by_dow, d.weekday) AS expected_slots
    FROM responders r
    JOIN day_roles d
      ON r.occasion_key = d.occasion_key
     AND r.event_year = d.event_year
    WHERE d.role IN ('SEARCH', 'WINDOW')
),

responder_day_values AS (
    SELECT
        d.entity_id,
        d.occasion_key,
        d.event_year,
        d.role_date,
        d.in_other_window,
        CASE WHEN d.in_other_window THEN CAST(0 AS DOUBLE)
             ELSE COALESCE(s.slots, 0) - d.expected_slots END AS excess_slots
    FROM responder_days d
    LEFT JOIN subclass_daily s
      ON d.entity_id = s.entity_id
     AND d.role_date = s.sale_date
),

demand_curve AS (
    SELECT
        occasion_key,
        event_year,
        role_date,
        MAX(CASE WHEN in_other_window THEN 1 ELSE 0 END) AS in_other_window,
        COUNT(*)          AS responders,
        SUM(excess_slots) AS excess_slots
    FROM responder_day_values
    GROUP BY occasion_key, event_year, role_date
),

subclass_names AS (
    SELECT subclass_entity_id, MAX(hierarchy_subclass_name) AS subclass_name
    FROM lab_customer_value_analytics.seasonality_v2_product_dim
    WHERE subclass_entity_id IS NOT NULL
    GROUP BY subclass_entity_id
),

-- the five responders adding the most extra demand, for review
top_responders AS (
    SELECT
        occasion_key,
        event_year,
        CONCAT_WS(', ', TRANSFORM(SORT_ARRAY(COLLECT_LIST(NAMED_STRUCT('r', responder_rank, 'n', subclass_label))),
                                  x -> x.n)) AS top_responder_subclasses
    FROM (
        SELECT
            t.occasion_key,
            t.event_year,
            COALESCE(n.subclass_name, t.entity_id) AS subclass_label,
            ROW_NUMBER() OVER (PARTITION BY t.occasion_key, t.event_year
                               ORDER BY t.total_excess DESC, t.entity_id) AS responder_rank
        FROM (
            SELECT occasion_key, event_year, entity_id, SUM(excess_slots) AS total_excess
            FROM responder_day_values
            GROUP BY occasion_key, event_year, entity_id
        ) t
        LEFT JOIN subclass_names n
          ON t.entity_id = n.subclass_entity_id
    ) ranked
    WHERE responder_rank <= 5
    GROUP BY occasion_key, event_year
),

running AS (
    SELECT
        c.*,
        SUM(c.excess_slots) OVER (
            PARTITION BY c.occasion_key, c.event_year ORDER BY c.role_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_excess_slots,
        SUM(c.excess_slots) OVER (
            PARTITION BY c.occasion_key, c.event_year ORDER BY c.role_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_excess_slots,
        MAX(CASE WHEN c.in_other_window = 0 THEN c.excess_slots END) OVER (
            PARTITION BY c.occasion_key, c.event_year) AS peak_excess_slots
    FROM demand_curve c
),

year_result AS (
    SELECT
        r.occasion_key,
        r.event_year,
        MAX(r.responders)          AS responder_subclasses,
        MAX(r.total_excess_slots)  AS total_excess_slots,
        CASE WHEN MAX(r.total_excess_slots) > 0 THEN COALESCE(
            DATE_ADD(MAX(CASE WHEN r.cum_excess_slots < p.timing_start_pct * r.total_excess_slots THEN r.role_date END), 1),
            MIN(r.role_date)) END AS shopping_start_date,
        CASE WHEN MAX(r.total_excess_slots) > 0 THEN COALESCE(
            DATE_ADD(MAX(CASE WHEN r.cum_excess_slots < p.timing_mid_pct * r.total_excess_slots THEN r.role_date END), 1),
            MIN(r.role_date)) END AS shopping_mid_date,
        CASE WHEN MAX(r.total_excess_slots) > 0 THEN COALESCE(
            DATE_ADD(MAX(CASE WHEN r.cum_excess_slots < p.timing_end_pct * r.total_excess_slots THEN r.role_date END), 1),
            MIN(r.role_date)) END AS shopping_end_date,
        MIN(CASE WHEN r.in_other_window = 0 AND r.excess_slots = r.peak_excess_slots
                 THEN r.role_date END) AS peak_date
    FROM running r
    CROSS JOIN prm p
    GROUP BY r.occasion_key, r.event_year
),

year_summary AS (
    SELECT
        o.occasion_key,
        o.occasion_name,
        o.event_year,
        o.event_start_date,
        o.event_end_date,
        o.lead_days,
        o.buildup_search_days,
        o.range_start                                       AS search_start_date,
        COALESCE(b.baseline_days, 0)                        AS baseline_days,
        COALESCE(n.baseline_days_before, 0)                 AS baseline_days_before,
        COALESCE(y.responder_subclasses, 0)                 AS responder_subclasses,
        tr.top_responder_subclasses,
        y.total_excess_slots,
        y.shopping_start_date,
        y.shopping_mid_date,
        y.shopping_end_date,
        y.peak_date,
        DATEDIFF(o.event_start_date, y.shopping_start_date) AS shopping_start_days_before,
        DATEDIFF(o.event_start_date, y.shopping_mid_date)   AS shopping_mid_days_before,
        DATEDIFF(o.event_start_date, y.shopping_end_date)   AS shopping_end_days_before,
        DATEDIFF(o.event_start_date, y.peak_date)           AS peak_days_before,
        CASE WHEN COALESCE(b.baseline_days, 0) >= p.min_baseline_days
              AND COALESCE(y.responder_subclasses, 0) > 0
              AND y.total_excess_slots >= p.min_calibration_excess
             THEN 'Y' ELSE 'N' END                          AS is_valid_year
    FROM calibration_occurrences o
    CROSS JOIN prm p
    LEFT JOIN occasion_base b
      ON o.occasion_key = b.occasion_key AND o.event_year = b.event_year
    LEFT JOIN base_before_counts n
      ON o.occasion_key = n.occasion_key AND o.event_year = n.event_year
    LEFT JOIN year_result y
      ON o.occasion_key = y.occasion_key AND o.event_year = y.event_year
    LEFT JOIN top_responders tr
      ON o.occasion_key = tr.occasion_key AND o.event_year = tr.event_year
),

occasion_level AS (
    SELECT
        s.*,
        SUM(CASE WHEN s.is_valid_year = 'Y' THEN 1 ELSE 0 END) OVER (PARTITION BY s.occasion_key) AS years_calibrated,
        PERCENTILE_APPROX(CASE WHEN s.is_valid_year = 'Y' THEN s.shopping_start_days_before END, 0.5)
            OVER (PARTITION BY s.occasion_key) AS calibrated_lead_days
    FROM year_summary s
)

SELECT
    l.*,
    l.calibrated_lead_days - l.lead_days AS buildup_days,
    CASE WHEN l.years_calibrated >= p.min_years
          AND l.calibrated_lead_days - l.lead_days >= p.min_buildup_days THEN 'Y' ELSE 'N' END AS has_buildup
FROM occasion_level l
CROSS JOIN prm p;


-- =====================================================================================
-- 6  OCCURRENCES  (window and baseline dates for every occasion x year)
--    window_type EVENT    the occasion's own windows (pre-event, event, post-event)
--    window_type BUILDUP  "<occasion> build-up": from the calibrated start of shopping
--                         (statement 5b) to the day before the pre-event window. It is an
--                         occasion in its own right, in the same family and with the same
--                         major flag, so a product can be flagged for the build-up, the
--                         event, or both.
--    Baseline columns hold the range scanned on each side. Statement 7 keeps the nearest
--    7 x baseline_weeks usable days in it.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_occurrences;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_occurrences
USING PARQUET AS
WITH coverage AS (
    SELECT MIN(data_start_date) AS data_start_date, MIN(data_through_date) AS data_through_date
    FROM lab_customer_value_analytics.seasonality_v2_calendar_days
),

buildup AS (
    SELECT occasion_key, MAX(calibrated_lead_days) AS calibrated_lead_days
    FROM lab_customer_value_analytics.seasonality_v2_buildup_calibration
    WHERE has_buildup = 'Y'
    GROUP BY occasion_key
),

occ_event AS (
    SELECT
        c.event_occurrence_id                     AS occurrence_id,
        c.occasion_key,
        c.occasion_key                            AS parent_occasion_key,
        'EVENT'                                   AS window_type,
        c.occasion_name,
        c.occasion_group,
        COALESCE(g.occasion_family, 'OTHER')      AS occasion_family,
        COALESCE(g.is_major, 'N')                 AS is_major_occasion,
        c.event_year,
        c.event_start_date,
        c.event_end_date,
        DATEDIFF(c.event_end_date, c.event_start_date) + 1 AS event_length_days,
        c.date_origin,
        c.recurrence_handling,
        COALESCE(g.lead_days, 2)                  AS lead_days,
        COALESCE(g.lag_days, 0)                   AS lag_days,
        COALESCE(g.baseline_gap_days, 7)          AS baseline_gap_days,
        COALESCE(g.baseline_weeks, 4)             AS baseline_weeks,
        COALESCE(g.baseline_side, 'BOTH')         AS baseline_side,
        b.calibrated_lead_days                    AS buildup_lead_days
    FROM lab_customer_value_analytics.seasonality_v2_event_calendar c
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_occasion_config g
      ON c.occasion_key = g.occasion_key
    LEFT JOIN buildup b
      ON c.occasion_key = b.occasion_key
),

occ_buildup AS (
    SELECT
        CONCAT(e.occasion_key, '_BUILDUP__',
               DATE_FORMAT(DATE_SUB(e.event_start_date, e.buildup_lead_days), 'yyyyMMdd'), '__',
               DATE_FORMAT(DATE_SUB(e.event_start_date, e.lead_days + 1), 'yyyyMMdd')) AS occurrence_id,
        CONCAT(e.occasion_key, '_BUILDUP')        AS occasion_key,
        e.occasion_key                            AS parent_occasion_key,
        'BUILDUP'                                 AS window_type,
        CONCAT(e.occasion_name, ' build-up')      AS occasion_name,
        e.occasion_group,
        e.occasion_family,
        e.is_major_occasion,
        e.event_year,
        DATE_SUB(e.event_start_date, e.buildup_lead_days) AS event_start_date,
        DATE_SUB(e.event_start_date, e.lead_days + 1)     AS event_end_date,
        e.buildup_lead_days - e.lead_days         AS event_length_days,
        'BUILDUP_CALIBRATED_V2'                   AS date_origin,
        e.recurrence_handling,
        0                                         AS lead_days,
        0                                         AS lag_days,
        7                                         AS baseline_gap_days,
        4                                         AS baseline_weeks,
        'BEFORE'                                  AS baseline_side,
        CAST(NULL AS INT)                         AS buildup_lead_days
    FROM occ_event e
    WHERE e.buildup_lead_days IS NOT NULL
),

occ AS (
    SELECT * FROM occ_event
    UNION ALL
    SELECT * FROM occ_buildup
),

dated AS (
    SELECT
        o.*,
        DATE_SUB(o.event_start_date, o.lead_days) AS pre_start_date,
        DATE_SUB(o.event_start_date, 1)           AS pre_end_date,
        DATE_ADD(o.event_end_date, 1)             AS post_start_date,
        DATE_ADD(o.event_end_date, o.lag_days)    AS post_end_date,
        CASE WHEN o.baseline_side IN ('BOTH', 'BEFORE')
             THEN DATE_SUB(o.event_start_date, o.lead_days + o.baseline_gap_days + p.baseline_max_scan_days) END
            AS base_before_start_date,
        CASE WHEN o.baseline_side IN ('BOTH', 'BEFORE')
             THEN DATE_SUB(o.event_start_date, o.lead_days + o.baseline_gap_days + 1) END
            AS base_before_end_date,
        CASE WHEN o.baseline_side IN ('BOTH', 'AFTER')
             THEN DATE_ADD(o.event_end_date, o.lag_days + o.baseline_gap_days + 1) END
            AS base_after_start_date,
        CASE WHEN o.baseline_side IN ('BOTH', 'AFTER')
             THEN DATE_ADD(o.event_end_date, o.lag_days + o.baseline_gap_days + p.baseline_max_scan_days) END
            AS base_after_end_date,
        cv.data_start_date,
        cv.data_through_date,
        CASE WHEN DATE_ADD(o.event_end_date, o.lag_days) <= cv.data_through_date
              AND o.event_end_date >= cv.data_start_date
             THEN 'Y' ELSE 'N' END AS is_completed
    FROM occ o
    CROSS JOIN coverage cv
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_params p
)

SELECT
    d.*,
    SUM(CASE WHEN d.is_completed = 'Y' THEN 1 ELSE 0 END) OVER (PARTITION BY d.occasion_key)
        AS calendar_years_available
FROM dated d;


-- =====================================================================================
-- 7  DATE ROLES  (the role each date plays for each completed occurrence)
--    PRE / EVENT / POST  the occasion's windows
--    BASE                comparison days: on each baseline side, the nearest
--                        7 x baseline_weeks usable days, scanning up to
--                        baseline_max_scan_days beyond the gap. Usable = not a
--                        low-traffic day, not inside a major occasion's own windows,
--                        and not inside the same occasion's build-up or event windows.
--                        Other occasions' build-ups stay in: they are the season an
--                        occasion has to stand out from.
--    YEAR                every day of its calendar year (pseudo-occurrence YEAR_<year>)
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_date_roles;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_date_roles
USING PARQUET AS
WITH occ AS (
    SELECT *
    FROM lab_customer_value_analytics.seasonality_v2_occurrences
    WHERE is_completed = 'Y'
),

window_ranges AS (
    SELECT occurrence_id, event_year, 'PRE' AS role, pre_start_date AS range_start, pre_end_date AS range_end
    FROM occ WHERE lead_days > 0
    UNION ALL
    SELECT occurrence_id, event_year, 'EVENT', event_start_date, event_end_date
    FROM occ
    UNION ALL
    SELECT occurrence_id, event_year, 'POST', post_start_date, post_end_date
    FROM occ WHERE lag_days > 0
),

window_days AS (
    SELECT r.occurrence_id, r.event_year, r.role, t.role_date
    FROM window_ranges r
    LATERAL VIEW EXPLODE(SEQUENCE(r.range_start, r.range_end)) t AS role_date
    WHERE r.range_start <= r.range_end
),

-- every day inside a major occasion's own windows (pre-event, event, post-event)
major_days AS (
    SELECT DISTINCT t.major_date
    FROM lab_customer_value_analytics.seasonality_v2_occurrences o
    LATERAL VIEW EXPLODE(SEQUENCE(o.pre_start_date, o.post_end_date)) t AS major_date
    WHERE o.is_major_occasion = 'Y'
      AND o.window_type = 'EVENT'
),

-- every day inside any window of the same occasion (its build-up and its event)
own_days AS (
    SELECT DISTINCT o.parent_occasion_key, t.own_date
    FROM lab_customer_value_analytics.seasonality_v2_occurrences o
    LATERAL VIEW EXPLODE(SEQUENCE(o.pre_start_date, o.post_end_date)) t AS own_date
),

base_candidates AS (
    SELECT o.occurrence_id, o.event_year, o.parent_occasion_key, o.baseline_weeks, 'BEFORE' AS side, t.role_date
    FROM occ o
    LATERAL VIEW EXPLODE(SEQUENCE(o.base_before_start_date, o.base_before_end_date)) t AS role_date
    WHERE o.base_before_start_date IS NOT NULL
    UNION ALL
    SELECT o.occurrence_id, o.event_year, o.parent_occasion_key, o.baseline_weeks, 'AFTER', t.role_date
    FROM occ o
    LATERAL VIEW EXPLODE(SEQUENCE(o.base_after_start_date, o.base_after_end_date)) t AS role_date
    WHERE o.base_after_start_date IS NOT NULL
),

base_usable AS (
    SELECT /*+ BROADCAST(c, m, od) */
        b.occurrence_id,
        b.event_year,
        b.baseline_weeks,
        b.role_date,
        ROW_NUMBER() OVER (
            PARTITION BY b.occurrence_id, b.side
            ORDER BY CASE WHEN b.side = 'BEFORE' THEN DATEDIFF(DATE '2100-01-01', b.role_date)
                          ELSE DATEDIFF(b.role_date, DATE '1900-01-01') END
        ) AS nearest_rank
    FROM base_candidates b
    JOIN lab_customer_value_analytics.seasonality_v2_calendar_days c
      ON b.role_date = c.cal_date
    LEFT JOIN major_days m
      ON b.role_date = m.major_date
    LEFT JOIN own_days od
      ON b.parent_occasion_key = od.parent_occasion_key
     AND b.role_date = od.own_date
    WHERE c.is_low_traffic_day = 'N'
      AND m.major_date IS NULL
      AND od.own_date IS NULL
),

role_days AS (
    SELECT occurrence_id, event_year, role, role_date
    FROM window_days
    UNION ALL
    SELECT occurrence_id, event_year, 'BASE', role_date
    FROM base_usable
    WHERE nearest_rank <= 7 * baseline_weeks
)

SELECT
    rd.occurrence_id,
    rd.event_year,
    rd.role,
    rd.role_date,
    c.weekday,
    c.all_baskets,
    c.all_items,
    c.is_low_traffic_day
FROM role_days rd
JOIN lab_customer_value_analytics.seasonality_v2_calendar_days c
  ON rd.role_date = c.cal_date

UNION ALL

SELECT
    CONCAT('YEAR_', CAST(c.cal_year AS STRING)) AS occurrence_id,
    c.cal_year AS event_year,
    'YEAR' AS role,
    c.cal_date AS role_date,
    c.weekday,
    c.all_baskets,
    c.all_items,
    c.is_low_traffic_day
FROM lab_customer_value_analytics.seasonality_v2_calendar_days c;


-- =====================================================================================
-- 8  OCCASION WINDOWS  (one row per completed occurrence: calendar-side totals)
--    *_by_dow arrays have 7 slots: 1 = Sunday ... 7 = Saturday (Spark DAYOFWEEK).
--    CORE = PRE + EVENT. base_days lists every usable baseline day for the
--    availability check in statement 11. overlapping_core lists other occasions whose
--    core window shares days with this one: what share of this core they cover
--    (inside_pct), what share of their own core lies inside this one (other_inside_pct)
--    and whether they are a build-up window.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_occasion_windows;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_occasion_windows
USING PARQUET AS
WITH r AS (
    SELECT *
    FROM lab_customer_value_analytics.seasonality_v2_date_roles
    WHERE role <> 'YEAR'
),

role_rows AS (
    SELECT occurrence_id, role AS role_group, weekday, all_items, all_baskets FROM r
    UNION ALL
    SELECT occurrence_id, 'CORE', weekday, all_items, all_baskets FROM r WHERE role IN ('PRE', 'EVENT')
),

by_dow AS (
    SELECT
        occurrence_id,
        role_group,
        weekday,
        COUNT(*)                                     AS days,
        SUM(all_items)                               AS items,
        SUM(all_baskets)                             AS baskets,
        SUM(CAST(all_items AS DOUBLE) * all_items)   AS items_sq
    FROM role_rows
    GROUP BY occurrence_id, role_group, weekday
),

by_role AS (
    SELECT
        occurrence_id,
        role_group,
        ARRAY(
            SUM(CASE WHEN weekday = 1 THEN days ELSE 0 END), SUM(CASE WHEN weekday = 2 THEN days ELSE 0 END),
            SUM(CASE WHEN weekday = 3 THEN days ELSE 0 END), SUM(CASE WHEN weekday = 4 THEN days ELSE 0 END),
            SUM(CASE WHEN weekday = 5 THEN days ELSE 0 END), SUM(CASE WHEN weekday = 6 THEN days ELSE 0 END),
            SUM(CASE WHEN weekday = 7 THEN days ELSE 0 END)
        ) AS days_by_dow,
        ARRAY(
            SUM(CASE WHEN weekday = 1 THEN items ELSE 0 END), SUM(CASE WHEN weekday = 2 THEN items ELSE 0 END),
            SUM(CASE WHEN weekday = 3 THEN items ELSE 0 END), SUM(CASE WHEN weekday = 4 THEN items ELSE 0 END),
            SUM(CASE WHEN weekday = 5 THEN items ELSE 0 END), SUM(CASE WHEN weekday = 6 THEN items ELSE 0 END),
            SUM(CASE WHEN weekday = 7 THEN items ELSE 0 END)
        ) AS items_by_dow,
        ARRAY(
            SUM(CASE WHEN weekday = 1 THEN baskets ELSE 0 END), SUM(CASE WHEN weekday = 2 THEN baskets ELSE 0 END),
            SUM(CASE WHEN weekday = 3 THEN baskets ELSE 0 END), SUM(CASE WHEN weekday = 4 THEN baskets ELSE 0 END),
            SUM(CASE WHEN weekday = 5 THEN baskets ELSE 0 END), SUM(CASE WHEN weekday = 6 THEN baskets ELSE 0 END),
            SUM(CASE WHEN weekday = 7 THEN baskets ELSE 0 END)
        ) AS baskets_by_dow,
        SUM(items_sq) AS items_sq
    FROM by_dow
    GROUP BY occurrence_id, role_group
),

pivoted AS (
    SELECT
        occurrence_id,
        MAX(CASE WHEN role_group = 'PRE'   THEN days_by_dow END)    AS pre_days_by_dow,
        MAX(CASE WHEN role_group = 'PRE'   THEN items_by_dow END)   AS pre_items_by_dow,
        MAX(CASE WHEN role_group = 'PRE'   THEN baskets_by_dow END) AS pre_baskets_by_dow,
        MAX(CASE WHEN role_group = 'PRE'   THEN items_sq END)       AS pre_items_sq,
        MAX(CASE WHEN role_group = 'EVENT' THEN days_by_dow END)    AS event_days_by_dow,
        MAX(CASE WHEN role_group = 'EVENT' THEN items_by_dow END)   AS event_items_by_dow,
        MAX(CASE WHEN role_group = 'EVENT' THEN baskets_by_dow END) AS event_baskets_by_dow,
        MAX(CASE WHEN role_group = 'EVENT' THEN items_sq END)       AS event_items_sq,
        MAX(CASE WHEN role_group = 'POST'  THEN days_by_dow END)    AS post_days_by_dow,
        MAX(CASE WHEN role_group = 'POST'  THEN items_by_dow END)   AS post_items_by_dow,
        MAX(CASE WHEN role_group = 'POST'  THEN baskets_by_dow END) AS post_baskets_by_dow,
        MAX(CASE WHEN role_group = 'POST'  THEN items_sq END)       AS post_items_sq,
        MAX(CASE WHEN role_group = 'CORE'  THEN days_by_dow END)    AS core_days_by_dow,
        MAX(CASE WHEN role_group = 'CORE'  THEN items_by_dow END)   AS core_items_by_dow,
        MAX(CASE WHEN role_group = 'CORE'  THEN baskets_by_dow END) AS core_baskets_by_dow,
        MAX(CASE WHEN role_group = 'CORE'  THEN items_sq END)       AS core_items_sq,
        MAX(CASE WHEN role_group = 'BASE'  THEN days_by_dow END)    AS base_days_by_dow,
        MAX(CASE WHEN role_group = 'BASE'  THEN items_by_dow END)   AS base_items_by_dow,
        MAX(CASE WHEN role_group = 'BASE'  THEN baskets_by_dow END) AS base_baskets_by_dow
    FROM by_role
    GROUP BY occurrence_id
),

base_list AS (
    SELECT
        occurrence_id,
        SORT_ARRAY(COLLECT_LIST(NAMED_STRUCT('d', role_date, 'dow', weekday, 'items', all_items, 'baskets', all_baskets)))
            AS base_days,
        MIN(role_date) AS base_min_date,
        MAX(role_date) AS base_max_date
    FROM r
    WHERE role = 'BASE'
    GROUP BY occurrence_id
),

core_dates AS (
    SELECT
        occurrence_id,
        MIN(role_date) AS core_start_in_data,
        MAX(role_date) AS core_end_in_data,
        SUM(CASE WHEN role = 'EVENT' AND is_low_traffic_day = 'Y' THEN 1 ELSE 0 END) AS event_low_traffic_days
    FROM r
    WHERE role IN ('PRE', 'EVENT')
    GROUP BY occurrence_id
),

-- Core-window overlaps between different occasions (for attribution in statement 11).
core_days AS (
    SELECT r.occurrence_id, o.occasion_key, r.role_date
    FROM r
    JOIN lab_customer_value_analytics.seasonality_v2_occurrences o
      ON r.occurrence_id = o.occurrence_id
    WHERE r.role IN ('PRE', 'EVENT')
),

core_length AS (
    SELECT occurrence_id, COUNT(*) AS core_days_in_data
    FROM core_days
    GROUP BY occurrence_id
),

core_pairs AS (
    SELECT a.occurrence_id, b.occurrence_id AS other_occurrence_id, COUNT(*) AS shared_days
    FROM core_days a
    JOIN core_days b
      ON a.role_date = b.role_date
     AND a.occasion_key <> b.occasion_key
    GROUP BY a.occurrence_id, b.occurrence_id
),

occurrence_seq AS (
    SELECT occurrence_id, CAST(ROW_NUMBER() OVER (ORDER BY occurrence_id) AS INT) AS occurrence_seq
    FROM lab_customer_value_analytics.seasonality_v2_occurrences
    WHERE is_completed = 'Y'
),

overlaps AS (
    SELECT
        cp.occurrence_id,
        COLLECT_LIST(NAMED_STRUCT(
            'seq', s.occurrence_seq,
            'name', o.occasion_name,
            'inside_pct', cp.shared_days / l.core_days_in_data,
            'other_inside_pct', cp.shared_days / lo.core_days_in_data,
            'other_is_buildup', o.window_type = 'BUILDUP'
        )) AS overlapping_core,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(o.occasion_name))) AS overlapping_occasion_names
    FROM core_pairs cp
    JOIN core_length l ON cp.occurrence_id = l.occurrence_id
    JOIN core_length lo ON cp.other_occurrence_id = lo.occurrence_id
    JOIN occurrence_seq s ON cp.other_occurrence_id = s.occurrence_id
    JOIN lab_customer_value_analytics.seasonality_v2_occurrences o ON cp.other_occurrence_id = o.occurrence_id
    GROUP BY cp.occurrence_id
),

year_info AS (
    SELECT
        cal_year,
        COUNT(*)       AS year_days_in_data,
        SUM(all_items) AS year_items,
        CASE WHEN MIN(cal_date) = MAKE_DATE(cal_year, 1, 1) AND MAX(cal_date) = MAKE_DATE(cal_year, 12, 31)
             THEN 'Y' ELSE 'N' END AS year_is_complete
    FROM lab_customer_value_analytics.seasonality_v2_calendar_days
    GROUP BY cal_year
)

SELECT
    o.occurrence_id,
    sq.occurrence_seq,
    o.occasion_key,
    o.parent_occasion_key,
    o.window_type,
    o.occasion_name,
    o.occasion_group,
    o.occasion_family,
    o.is_major_occasion,
    o.event_year,
    o.event_start_date,
    o.event_end_date,
    o.event_length_days,
    o.date_origin,
    o.lead_days,
    o.lag_days,
    o.baseline_gap_days,
    o.baseline_weeks,
    o.baseline_side,
    o.pre_start_date,
    o.post_end_date,
    o.calendar_years_available,
    COALESCE(p.pre_days_by_dow,      ARRAY(0L, 0L, 0L, 0L, 0L, 0L, 0L)) AS pre_days_by_dow,
    COALESCE(p.pre_items_by_dow,     ARRAY(0L, 0L, 0L, 0L, 0L, 0L, 0L)) AS pre_items_by_dow,
    COALESCE(p.pre_baskets_by_dow,   ARRAY(0L, 0L, 0L, 0L, 0L, 0L, 0L)) AS pre_baskets_by_dow,
    COALESCE(p.pre_items_sq, 0.0)                                         AS pre_items_sq,
    COALESCE(p.event_days_by_dow,    ARRAY(0L, 0L, 0L, 0L, 0L, 0L, 0L)) AS event_days_by_dow,
    COALESCE(p.event_items_by_dow,   ARRAY(0L, 0L, 0L, 0L, 0L, 0L, 0L)) AS event_items_by_dow,
    COALESCE(p.event_baskets_by_dow, ARRAY(0L, 0L, 0L, 0L, 0L, 0L, 0L)) AS event_baskets_by_dow,
    COALESCE(p.event_items_sq, 0.0)                                       AS event_items_sq,
    COALESCE(p.post_days_by_dow,     ARRAY(0L, 0L, 0L, 0L, 0L, 0L, 0L)) AS post_days_by_dow,
    COALESCE(p.post_items_by_dow,    ARRAY(0L, 0L, 0L, 0L, 0L, 0L, 0L)) AS post_items_by_dow,
    COALESCE(p.post_baskets_by_dow,  ARRAY(0L, 0L, 0L, 0L, 0L, 0L, 0L)) AS post_baskets_by_dow,
    COALESCE(p.post_items_sq, 0.0)                                        AS post_items_sq,
    COALESCE(p.core_days_by_dow,     ARRAY(0L, 0L, 0L, 0L, 0L, 0L, 0L)) AS core_days_by_dow,
    COALESCE(p.core_items_by_dow,    ARRAY(0L, 0L, 0L, 0L, 0L, 0L, 0L)) AS core_items_by_dow,
    COALESCE(p.core_baskets_by_dow,  ARRAY(0L, 0L, 0L, 0L, 0L, 0L, 0L)) AS core_baskets_by_dow,
    COALESCE(p.core_items_sq, 0.0)                                        AS core_items_sq,
    COALESCE(p.base_days_by_dow,     ARRAY(0L, 0L, 0L, 0L, 0L, 0L, 0L)) AS base_days_by_dow,
    COALESCE(p.base_items_by_dow,    ARRAY(0L, 0L, 0L, 0L, 0L, 0L, 0L)) AS base_items_by_dow,
    COALESCE(p.base_baskets_by_dow,  ARRAY(0L, 0L, 0L, 0L, 0L, 0L, 0L)) AS base_baskets_by_dow,
    COALESCE(
        b.base_days,
        FILTER(
            ARRAY(NAMED_STRUCT('d', CAST(NULL AS DATE), 'dow', CAST(NULL AS INT),
                               'items', CAST(NULL AS BIGINT), 'baskets', CAST(NULL AS BIGINT))),
            x -> FALSE
        )
    ) AS base_days,
    b.base_min_date,
    b.base_max_date,
    cd.core_start_in_data,
    cd.core_end_in_data,
    COALESCE(cd.event_low_traffic_days, 0) AS event_low_traffic_days,
    y.year_days_in_data,
    y.year_items,
    y.year_is_complete,
    COALESCE(
        ov.overlapping_core,
        FILTER(ARRAY(NAMED_STRUCT('seq', CAST(NULL AS INT), 'name', CAST(NULL AS STRING), 'inside_pct', CAST(NULL AS DOUBLE),
                                  'other_inside_pct', CAST(NULL AS DOUBLE), 'other_is_buildup', CAST(NULL AS BOOLEAN))),
               x -> FALSE)
    ) AS overlapping_core,
    COALESCE(ov.overlapping_occasion_names, '') AS overlapping_occasion_names
FROM lab_customer_value_analytics.seasonality_v2_occurrences o
JOIN occurrence_seq sq   ON o.occurrence_id = sq.occurrence_id
LEFT JOIN overlaps ov    ON o.occurrence_id = ov.occurrence_id
LEFT JOIN pivoted p      ON o.occurrence_id = p.occurrence_id
LEFT JOIN base_list b    ON o.occurrence_id = b.occurrence_id
LEFT JOIN core_dates cd  ON o.occurrence_id = cd.occurrence_id
LEFT JOIN year_info y    ON o.event_year = y.cal_year
WHERE o.is_completed = 'Y';


-- =====================================================================================
-- 9  ENTITY DAILY  (day x entity, where entity is a TPNB or a subclass)
--    A subclass's "baskets" = the sum of its TPNBs' baskets, i.e. its item slots. That is
--    the same unit as the market's items, so a subclass's share of items is consistent.
--    The NULL_TPNB catch-all is left out of entity-level analysis.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_entity_daily;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_entity_daily
USING PARQUET AS
WITH tpnb_daily AS (
    SELECT
        sale_date,
        sale_year,
        sale_day_of_week,
        'TPNB' AS entity_level,
        entity_id,
        baskets,
        sales,
        units
    FROM lab_customer_value_analytics.seasonality_v2_daily_fact
    WHERE entity_type = 'TPNB'
      AND entity_id <> 'NULL_TPNB'
),

subclass_daily AS (
    SELECT /*+ BROADCAST(p) */
        t.sale_date,
        t.sale_year,
        t.sale_day_of_week,
        'SUBCLASS' AS entity_level,
        p.subclass_entity_id AS entity_id,
        SUM(t.baskets) AS baskets,
        SUM(t.sales)   AS sales,
        SUM(t.units)   AS units
    FROM tpnb_daily t
    JOIN lab_customer_value_analytics.seasonality_v2_product_dim p
      ON TRY_CAST(t.entity_id AS BIGINT) = p.tpnb
    WHERE p.subclass_entity_id IS NOT NULL
    GROUP BY t.sale_date, t.sale_year, t.sale_day_of_week, p.subclass_entity_id
)

SELECT * FROM tpnb_daily
UNION ALL
SELECT * FROM subclass_daily;


-- =====================================================================================
-- 10  AVAILABILITY SPELLS  (runs of sale days with no gap longer than spell_max_gap_days)
--     Days inside a spell count as "on sale" even when nothing sold. Days outside every
--     spell (before launch, after delisting, long out-of-stock) are left out of uplift
--     baselines.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_availability_spells;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_availability_spells
USING PARQUET AS
WITH gaps AS (
    SELECT
        entity_level,
        entity_id,
        sale_date,
        DATEDIFF(sale_date, LAG(sale_date) OVER (PARTITION BY entity_id ORDER BY sale_date))
            AS days_since_previous_sale
    FROM lab_customer_value_analytics.seasonality_v2_entity_daily
),

flagged AS (
    SELECT
        g.*,
        CASE WHEN g.days_since_previous_sale <= p.spell_max_gap_days THEN 0 ELSE 1 END AS starts_new_spell
    FROM gaps g
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_params p
),

numbered AS (
    SELECT
        f.*,
        SUM(f.starts_new_spell) OVER (
            PARTITION BY f.entity_id
            ORDER BY f.sale_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS spell_no
    FROM flagged f
)

SELECT
    entity_level,
    entity_id,
    spell_no,
    MIN(sale_date)                                  AS spell_start,
    MAX(sale_date)                                  AS spell_end,
    COUNT(*)                                        AS sale_days,
    DATEDIFF(MAX(sale_date), MIN(sale_date)) + 1    AS spell_days
FROM numbered
GROUP BY entity_level, entity_id, spell_no;


-- =====================================================================================
-- 11  OCCASION METRICS  (MAIN OUTPUT: one row per entity x occasion x year)
--
--     For each window W (PRE, EVENT, POST, and CORE = PRE + EVENT):
--       expected_baskets_W = sum over weekdays k of
--                            market items in W on weekday k  x  entity's mean daily share
--                            of items on weekday k in its baseline
--       share_index_W      = actual baskets / expected baskets   (traffic and basket-size adjusted)
--       penetration_index  = actual baskets / (market baskets x baseline penetration)  (traffic only)
--       z_W                = (actual - expected) / max(model sd, sqrt(expected))
--     The model sd uses the day-to-day spread of the entity's share in its own baseline,
--     plus the uncertainty of the baseline mean itself.
--     Overlapping occasions: a positive year counts only for the occasion where the
--     entity's share of items is clearly higher, and only positive occasions can claim
--     a year. An occasion inside a build-up window keeps a year only if its share is at
--     least nested_occasion_min_ratio x the build-up's (see attribution below).
--
--     Shuffles: one large one, by entity. The role join, the aggregation, the annual
--     totals, the overlap check and the verdict window all run inside those partitions.
--     The calendar, spell and product lookups are small and broadcast.
--     400 = partition count, tune it to the cluster.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_occasion_metrics;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_occasion_metrics
USING PARQUET AS
WITH entity_daily_by_entity AS (
    SELECT /*+ REPARTITION(400, entity_id) */
        entity_level,
        entity_id,
        sale_date,
        baskets,
        sales,
        units
    FROM lab_customer_value_analytics.seasonality_v2_entity_daily
),

-- Each sale day picks up every role it plays (pre, event, post, baseline of any
-- occurrence, plus its calendar year). Broadcast join: stays in the partition.
role_rows AS (
    SELECT /*+ BROADCAST(r) */
        d.entity_level,
        d.entity_id,
        r.occurrence_id,
        r.event_year,
        r.role,
        r.weekday,
        d.sale_date,
        d.baskets,
        d.sales,
        d.units,
        d.baskets / NULLIF(r.all_items, 0) AS item_share
    FROM entity_daily_by_entity d
    JOIN lab_customer_value_analytics.seasonality_v2_date_roles r
      ON d.sale_date = r.role_date
),

entity_occurrence AS (
    SELECT
        entity_level,
        entity_id,
        occurrence_id,
        event_year,
        MAX(CASE WHEN role = 'YEAR' THEN 1 ELSE 0 END) AS is_year_row,

        SUM(CASE WHEN role = 'PRE'   THEN baskets ELSE 0 END)   AS pre_baskets,
        SUM(CASE WHEN role = 'PRE'   THEN sales   ELSE 0.0 END) AS pre_sales,
        SUM(CASE WHEN role = 'PRE'   THEN units   ELSE 0.0 END) AS pre_units,
        SUM(CASE WHEN role = 'EVENT' THEN baskets ELSE 0 END)   AS event_baskets,
        SUM(CASE WHEN role = 'EVENT' THEN sales   ELSE 0.0 END) AS event_sales,
        SUM(CASE WHEN role = 'EVENT' THEN units   ELSE 0.0 END) AS event_units,
        SUM(CASE WHEN role = 'EVENT' THEN 1       ELSE 0 END)   AS event_active_days,
        SUM(CASE WHEN role = 'POST'  THEN baskets ELSE 0 END)   AS post_baskets,
        SUM(CASE WHEN role = 'POST'  THEN sales   ELSE 0.0 END) AS post_sales,
        SUM(CASE WHEN role = 'POST'  THEN units   ELSE 0.0 END) AS post_units,

        SUM(CASE WHEN role = 'BASE'  THEN baskets ELSE 0 END)   AS base_baskets,
        SUM(CASE WHEN role = 'BASE'  THEN sales   ELSE 0.0 END) AS base_sales,
        SUM(CASE WHEN role = 'BASE'  THEN units   ELSE 0.0 END) AS base_units,

        -- baseline share of items, summed and squared, by weekday (1 = Sunday)
        SUM(CASE WHEN role = 'BASE' AND weekday = 1 THEN item_share ELSE 0.0 END) AS bs1,
        SUM(CASE WHEN role = 'BASE' AND weekday = 2 THEN item_share ELSE 0.0 END) AS bs2,
        SUM(CASE WHEN role = 'BASE' AND weekday = 3 THEN item_share ELSE 0.0 END) AS bs3,
        SUM(CASE WHEN role = 'BASE' AND weekday = 4 THEN item_share ELSE 0.0 END) AS bs4,
        SUM(CASE WHEN role = 'BASE' AND weekday = 5 THEN item_share ELSE 0.0 END) AS bs5,
        SUM(CASE WHEN role = 'BASE' AND weekday = 6 THEN item_share ELSE 0.0 END) AS bs6,
        SUM(CASE WHEN role = 'BASE' AND weekday = 7 THEN item_share ELSE 0.0 END) AS bs7,
        SUM(CASE WHEN role = 'BASE' AND weekday = 1 THEN item_share * item_share ELSE 0.0 END) AS bq1,
        SUM(CASE WHEN role = 'BASE' AND weekday = 2 THEN item_share * item_share ELSE 0.0 END) AS bq2,
        SUM(CASE WHEN role = 'BASE' AND weekday = 3 THEN item_share * item_share ELSE 0.0 END) AS bq3,
        SUM(CASE WHEN role = 'BASE' AND weekday = 4 THEN item_share * item_share ELSE 0.0 END) AS bq4,
        SUM(CASE WHEN role = 'BASE' AND weekday = 5 THEN item_share * item_share ELSE 0.0 END) AS bq5,
        SUM(CASE WHEN role = 'BASE' AND weekday = 6 THEN item_share * item_share ELSE 0.0 END) AS bq6,
        SUM(CASE WHEN role = 'BASE' AND weekday = 7 THEN item_share * item_share ELSE 0.0 END) AS bq7,
        SUM(CASE WHEN role = 'BASE' AND weekday = 1 THEN baskets ELSE 0 END) AS bb1,
        SUM(CASE WHEN role = 'BASE' AND weekday = 2 THEN baskets ELSE 0 END) AS bb2,
        SUM(CASE WHEN role = 'BASE' AND weekday = 3 THEN baskets ELSE 0 END) AS bb3,
        SUM(CASE WHEN role = 'BASE' AND weekday = 4 THEN baskets ELSE 0 END) AS bb4,
        SUM(CASE WHEN role = 'BASE' AND weekday = 5 THEN baskets ELSE 0 END) AS bb5,
        SUM(CASE WHEN role = 'BASE' AND weekday = 6 THEN baskets ELSE 0 END) AS bb6,
        SUM(CASE WHEN role = 'BASE' AND weekday = 7 THEN baskets ELSE 0 END) AS bb7,

        -- calendar-year totals (only on the YEAR_<year> pseudo-occurrence rows)
        SUM(CASE WHEN role = 'YEAR'  THEN baskets ELSE 0 END)   AS year_baskets,
        SUM(CASE WHEN role = 'YEAR'  THEN sales   ELSE 0.0 END) AS year_sales,
        SUM(CASE WHEN role = 'YEAR'  THEN units   ELSE 0.0 END) AS year_units,
        SUM(CASE WHEN role = 'YEAR'  THEN 1       ELSE 0 END)   AS year_active_days,
        MIN(CASE WHEN role = 'YEAR'  THEN sale_date END)        AS year_first_sale,
        MAX(CASE WHEN role = 'YEAR'  THEN sale_date END)        AS year_last_sale
    FROM role_rows
    GROUP BY entity_level, entity_id, occurrence_id, event_year
),

-- Annual totals copied onto each occurrence row of the same year (window, no shuffle).
entity_occurrence_annual AS (
    SELECT
        eo.*,
        MAX(CASE WHEN eo.is_year_row = 1 THEN eo.year_baskets END)     OVER ey AS annual_baskets,
        MAX(CASE WHEN eo.is_year_row = 1 THEN eo.year_sales END)       OVER ey AS annual_sales,
        MAX(CASE WHEN eo.is_year_row = 1 THEN eo.year_units END)       OVER ey AS annual_units,
        MAX(CASE WHEN eo.is_year_row = 1 THEN eo.year_active_days END) OVER ey AS annual_active_days,
        MAX(CASE WHEN eo.is_year_row = 1 THEN eo.year_first_sale END)  OVER ey AS year_first_sale_date,
        MAX(CASE WHEN eo.is_year_row = 1 THEN eo.year_last_sale END)   OVER ey AS year_last_sale_date
    FROM entity_occurrence eo
    WINDOW ey AS (PARTITION BY eo.entity_level, eo.entity_id, eo.event_year)
),

-- Small lookup tables, broadcast to every partition.
occurrence_lookup AS (
    SELECT w.*, p.*
    FROM lab_customer_value_analytics.seasonality_v2_occasion_windows w
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_params p
),

spells_by_entity AS (
    SELECT entity_id, COLLECT_LIST(NAMED_STRUCT('s', spell_start, 'e', spell_end)) AS spells
    FROM lab_customer_value_analytics.seasonality_v2_availability_spells
    GROUP BY entity_id
),

product_lookup AS (
    SELECT tpnb, product_description, hierarchy_subclass_code, hierarchy_subclass_name,
           gb_launch_date, gb_deactive_date
    FROM lab_customer_value_analytics.seasonality_v2_product_dim
),

subclass_lookup AS (
    SELECT subclass_entity_id,
           MAX(hierarchy_subclass_code) AS subclass_code,
           MAX(hierarchy_subclass_name) AS subclass_name
    FROM lab_customer_value_analytics.seasonality_v2_product_dim
    WHERE subclass_entity_id IS NOT NULL
    GROUP BY subclass_entity_id
),

joined AS (
    SELECT /*+ BROADCAST(o, s, p, sc) */
        a.*,
        o.occasion_key,
        o.parent_occasion_key,
        o.window_type,
        o.occasion_name,
        o.occasion_group,
        o.occasion_family,
        o.is_major_occasion,
        o.event_start_date,
        o.event_end_date,
        o.event_length_days,
        o.date_origin,
        o.lead_days,
        o.lag_days,
        o.baseline_side,
        o.pre_start_date,
        o.post_end_date,
        o.calendar_years_available,
        o.occurrence_seq,
        o.overlapping_core,
        o.overlapping_occasion_names,
        o.pre_items_by_dow,   o.pre_baskets_by_dow,   o.pre_items_sq,
        o.event_items_by_dow, o.event_baskets_by_dow, o.event_items_sq,
        o.post_items_by_dow,  o.post_baskets_by_dow,  o.post_items_sq,
        o.core_items_by_dow,  o.core_baskets_by_dow,  o.core_items_sq,
        o.base_days_by_dow,   o.base_items_by_dow,    o.base_baskets_by_dow,
        o.base_days,
        o.base_min_date,
        o.base_max_date,
        o.core_start_in_data,
        o.core_end_in_data,
        o.event_low_traffic_days,
        o.year_items,
        o.year_is_complete,
        o.min_baseline_days,
        o.min_expected_baskets,
        o.min_expected_baskets_window,
        o.uplift_index_min,
        o.z_min,
        o.specific_ratio_min,
        o.min_specific_baskets,
        o.promo_price_change_max_pct,
        o.min_years,
        o.min_year_share,
        o.nested_occasion_min_ratio,
        s.spells,
        COALESCE(p.product_description, sc.subclass_name)     AS entity_description,
        COALESCE(p.hierarchy_subclass_code, sc.subclass_code) AS subclass_code,
        COALESCE(p.hierarchy_subclass_name, sc.subclass_name) AS subclass_name,
        p.gb_launch_date,
        p.gb_deactive_date
    FROM entity_occurrence_annual a
    JOIN occurrence_lookup o
      ON a.occurrence_id = o.occurrence_id
    LEFT JOIN spells_by_entity s
      ON a.entity_id = s.entity_id
    LEFT JOIN product_lookup p
      ON TRY_CAST(a.entity_id AS BIGINT) = p.tpnb
     AND a.entity_level = 'TPNB'
    LEFT JOIN subclass_lookup sc
      ON a.entity_id = sc.subclass_entity_id
    WHERE a.is_year_row = 0
),

-- Availability: baseline days count only when the entity was on sale. Most rows are on
-- sale for the whole baseline and reuse the occasion's precomputed totals. Only rows
-- whose on-sale spells cover part of the baseline filter the day list.
with_availability AS (
    SELECT
        j.*,
        COALESCE(EXISTS(j.spells, sp -> sp.s <= j.base_min_date AND sp.e >= j.base_max_date), FALSE)
            AS baseline_fully_on_sale,
        COALESCE(EXISTS(j.spells, sp -> sp.s <= j.core_end_in_data AND sp.e >= j.core_start_in_data), FALSE)
            AS on_sale_in_core
    FROM joined j
),

available_baseline AS (
    SELECT
        w.*,
        CASE WHEN w.baseline_fully_on_sale THEN w.base_days_by_dow
             ELSE TRANSFORM(SEQUENCE(1, 7), wd ->
                    CAST(SIZE(FILTER(w.base_days, x -> x.dow = wd
                                     AND EXISTS(w.spells, sp -> x.d BETWEEN sp.s AND sp.e))) AS BIGINT))
        END AS avail_days_by_dow,
        CASE WHEN w.baseline_fully_on_sale THEN w.base_items_by_dow
             ELSE TRANSFORM(SEQUENCE(1, 7), wd ->
                    AGGREGATE(FILTER(w.base_days, x -> x.dow = wd
                                     AND EXISTS(w.spells, sp -> x.d BETWEEN sp.s AND sp.e)),
                              CAST(0 AS BIGINT), (acc, x) -> acc + x.items))
        END AS avail_items_by_dow,
        CASE WHEN w.baseline_fully_on_sale THEN w.base_baskets_by_dow
             ELSE TRANSFORM(SEQUENCE(1, 7), wd ->
                    AGGREGATE(FILTER(w.base_days, x -> x.dow = wd
                                     AND EXISTS(w.spells, sp -> x.d BETWEEN sp.s AND sp.e)),
                              CAST(0 AS BIGINT), (acc, x) -> acc + x.baskets))
        END AS avail_baskets_by_dow
    FROM with_availability w
),

baseline_model AS (
    SELECT
        v.*,
        ARRAY(v.bs1, v.bs2, v.bs3, v.bs4, v.bs5, v.bs6, v.bs7) AS share_sum_by_dow,
        ARRAY(v.bq1, v.bq2, v.bq3, v.bq4, v.bq5, v.bq6, v.bq7) AS share_sq_by_dow,
        ARRAY(v.bb1, v.bb2, v.bb3, v.bb4, v.bb5, v.bb6, v.bb7) AS entity_base_baskets_by_dow,
        AGGREGATE(v.avail_days_by_dow, CAST(0 AS BIGINT), (acc, x) -> acc + x)    AS baseline_days_available,
        SIZE(FILTER(v.avail_days_by_dow, x -> x > 0))                               AS baseline_weekdays_available,
        AGGREGATE(v.avail_baskets_by_dow, CAST(0 AS BIGINT), (acc, x) -> acc + x) AS baseline_market_baskets
    FROM available_baseline v
),

baseline_model2 AS (
    SELECT
        m.*,
        (m.bs1 + m.bs2 + m.bs3 + m.bs4 + m.bs5 + m.bs6 + m.bs7)
            / NULLIF(m.baseline_days_available, 0) AS pooled_mean_share,
        (m.bb1 + m.bb2 + m.bb3 + m.bb4 + m.bb5 + m.bb6 + m.bb7)
            / NULLIF(m.baseline_market_baskets, 0) AS pooled_penetration,
        -- sum of squares of the daily share around each weekday's mean
        AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
            acc + CASE WHEN ELEMENT_AT(m.avail_days_by_dow, wd) > 0
                       THEN ELEMENT_AT(m.share_sq_by_dow, wd)
                            - POW(ELEMENT_AT(m.share_sum_by_dow, wd), 2) / ELEMENT_AT(m.avail_days_by_dow, wd)
                       ELSE CAST(0 AS DOUBLE) END
        ) AS share_ss_within
    FROM baseline_model m
),

baseline_model3 AS (
    SELECT
        m.*,
        -- mean daily share of items per weekday (pooled mean where a weekday has no baseline day)
        TRANSFORM(SEQUENCE(1, 7), wd ->
            CASE WHEN ELEMENT_AT(m.avail_days_by_dow, wd) > 0
                 THEN ELEMENT_AT(m.share_sum_by_dow, wd) / ELEMENT_AT(m.avail_days_by_dow, wd)
                 ELSE m.pooled_mean_share END
        ) AS mean_share_by_dow,
        -- penetration (baskets with the entity / all baskets) per weekday
        TRANSFORM(SEQUENCE(1, 7), wd ->
            CASE WHEN ELEMENT_AT(m.avail_baskets_by_dow, wd) > 0
                 THEN ELEMENT_AT(m.entity_base_baskets_by_dow, wd) / ELEMENT_AT(m.avail_baskets_by_dow, wd)
                 ELSE m.pooled_penetration END
        ) AS penetration_by_dow,
        -- baseline days behind each weekday's mean (all days where the pooled mean is used)
        TRANSFORM(SEQUENCE(1, 7), wd ->
            CAST(CASE WHEN ELEMENT_AT(m.avail_days_by_dow, wd) > 0
                      THEN ELEMENT_AT(m.avail_days_by_dow, wd)
                      ELSE m.baseline_days_available END AS DOUBLE)
        ) AS mean_days_by_dow,
        CASE WHEN m.baseline_days_available - m.baseline_weekdays_available > 0
             THEN GREATEST(m.share_ss_within, CAST(0 AS DOUBLE))
                  / (m.baseline_days_available - m.baseline_weekdays_available)
        END AS share_variance
    FROM baseline_model2 m
),

window_expectations AS (
    SELECT
        x.*,
        CASE WHEN x.baseline_days_available > 0 THEN
            AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
                acc + ELEMENT_AT(x.pre_items_by_dow, wd) * ELEMENT_AT(x.mean_share_by_dow, wd)) END
            AS pre_expected_baskets,
        CASE WHEN x.baseline_days_available > 0 THEN
            AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
                acc + ELEMENT_AT(x.event_items_by_dow, wd) * ELEMENT_AT(x.mean_share_by_dow, wd)) END
            AS event_expected_baskets,
        CASE WHEN x.baseline_days_available > 0 THEN
            AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
                acc + ELEMENT_AT(x.post_items_by_dow, wd) * ELEMENT_AT(x.mean_share_by_dow, wd)) END
            AS post_expected_baskets,
        CASE WHEN x.baseline_days_available > 0 THEN
            AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
                acc + ELEMENT_AT(x.core_items_by_dow, wd) * ELEMENT_AT(x.mean_share_by_dow, wd)) END
            AS core_expected_baskets,

        -- model sd = sqrt( share variance x (sum of squared daily items in W
        --                  + sum over weekdays of (items in W on that weekday)^2 / baseline days) )
        CASE WHEN x.baseline_days_available > 0 THEN
            SQRT(COALESCE(x.share_variance, CAST(0 AS DOUBLE)) * (x.pre_items_sq +
                AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
                    acc + POW(ELEMENT_AT(x.pre_items_by_dow, wd), 2) / ELEMENT_AT(x.mean_days_by_dow, wd)))) END
            AS pre_model_sd,
        CASE WHEN x.baseline_days_available > 0 THEN
            SQRT(COALESCE(x.share_variance, CAST(0 AS DOUBLE)) * (x.event_items_sq +
                AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
                    acc + POW(ELEMENT_AT(x.event_items_by_dow, wd), 2) / ELEMENT_AT(x.mean_days_by_dow, wd)))) END
            AS event_model_sd,
        CASE WHEN x.baseline_days_available > 0 THEN
            SQRT(COALESCE(x.share_variance, CAST(0 AS DOUBLE)) * (x.post_items_sq +
                AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
                    acc + POW(ELEMENT_AT(x.post_items_by_dow, wd), 2) / ELEMENT_AT(x.mean_days_by_dow, wd)))) END
            AS post_model_sd,
        CASE WHEN x.baseline_days_available > 0 THEN
            SQRT(COALESCE(x.share_variance, CAST(0 AS DOUBLE)) * (x.core_items_sq +
                AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
                    acc + POW(ELEMENT_AT(x.core_items_by_dow, wd), 2) / ELEMENT_AT(x.mean_days_by_dow, wd)))) END
            AS core_model_sd,

        -- traffic-only expectation: market baskets x baseline penetration
        CASE WHEN x.baseline_days_available > 0 THEN
            AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
                acc + ELEMENT_AT(x.core_baskets_by_dow, wd) * ELEMENT_AT(x.penetration_by_dow, wd)) END
            AS core_expected_baskets_traffic_only,

        AGGREGATE(x.core_items_by_dow, CAST(0 AS BIGINT), (acc, v) -> acc + v) AS core_market_items
    FROM baseline_model3 x
),

window_metrics AS (
    SELECT
        w.*,
        w.pre_baskets + w.event_baskets AS core_baskets,
        w.pre_sales + w.event_sales     AS core_sales,
        w.pre_units + w.event_units     AS core_units,

        w.pre_baskets   / NULLIF(w.pre_expected_baskets, 0)   AS pre_share_index,
        w.event_baskets / NULLIF(w.event_expected_baskets, 0) AS event_share_index,
        w.post_baskets  / NULLIF(w.post_expected_baskets, 0)  AS post_share_index,
        (w.pre_baskets + w.event_baskets) / NULLIF(w.core_expected_baskets, 0) AS core_share_index,
        (w.pre_baskets + w.event_baskets) / NULLIF(w.core_expected_baskets_traffic_only, 0) AS core_penetration_index,

        (w.pre_baskets - w.pre_expected_baskets)
            / NULLIF(GREATEST(w.pre_model_sd, SQRT(GREATEST(w.pre_expected_baskets, CAST(0 AS DOUBLE)))), 0) AS pre_z,
        (w.event_baskets - w.event_expected_baskets)
            / NULLIF(GREATEST(w.event_model_sd, SQRT(GREATEST(w.event_expected_baskets, CAST(0 AS DOUBLE)))), 0) AS event_z,
        (w.post_baskets - w.post_expected_baskets)
            / NULLIF(GREATEST(w.post_model_sd, SQRT(GREATEST(w.post_expected_baskets, CAST(0 AS DOUBLE)))), 0) AS post_z,
        (w.pre_baskets + w.event_baskets - w.core_expected_baskets)
            / NULLIF(GREATEST(w.core_model_sd, SQRT(GREATEST(w.core_expected_baskets, CAST(0 AS DOUBLE)))), 0) AS core_z,

        -- sales and price, valued at the entity's own baseline sales per basket
        w.core_expected_baskets * (w.base_sales / NULLIF(w.base_baskets, 0)) AS core_expected_sales,
        w.base_sales / NULLIF(w.base_units, 0) AS baseline_avg_unit_price,
        (w.pre_sales + w.event_sales) / NULLIF(w.pre_units + w.event_units, 0) AS core_avg_unit_price,

        -- annual view for the "specifically seasonal" test (also traffic adjusted)
        GREATEST(COALESCE(w.annual_baskets, 0) - (w.pre_baskets + w.event_baskets), 0) AS rest_of_year_baskets,
        w.year_items - w.core_market_items AS rest_of_year_market_items
    FROM window_expectations w
),

year_tests AS (
    SELECT
        m.*,
        m.core_baskets / NULLIF(m.core_market_items, 0) AS core_item_share,
        m.core_sales - m.core_expected_sales AS core_incremental_sales,
        100.0 * (m.core_sales / NULLIF(m.core_expected_sales, 0) - 1.0) AS core_sales_uplift_pct,
        100.0 * (m.core_avg_unit_price / NULLIF(m.baseline_avg_unit_price, 0) - 1.0) AS core_price_change_pct,

        (m.core_baskets / NULLIF(m.core_market_items, 0))
            / NULLIF(m.rest_of_year_baskets / NULLIF(m.rest_of_year_market_items, 0), 0) AS specific_share_ratio,

        -- Observed availability spells are the eligibility source of truth.
        -- D9 showed material contradictions between product.product GB lifecycle dates
        -- and observed sales, including high-volume TPNBs. Keep the GB dates in the
        -- output for diagnostics, but do not use them as hard event-year exclusions.
        CASE
            WHEN NOT m.on_sale_in_core THEN 'NOT_ON_SALE_AROUND_EVENT'
            ELSE 'ELIGIBLE'
        END AS eligibility_status,

        CASE
            WHEN m.baseline_days_available < m.min_baseline_days THEN 'INSUFFICIENT_BASELINE'
            WHEN m.core_expected_baskets IS NULL OR m.core_expected_baskets < m.min_expected_baskets THEN 'LOW_VOLUME'
            WHEN m.core_share_index >= m.uplift_index_min AND m.core_z >= m.z_min THEN 'SIGNIFICANT_UPLIFT'
            WHEN m.core_share_index <= 1.0 / m.uplift_index_min AND m.core_z <= -m.z_min THEN 'SIGNIFICANT_DECLINE'
            ELSE 'NOT_SIGNIFICANT'
        END AS core_status,

        COALESCE(m.baseline_days_available >= m.min_baseline_days
                 AND m.pre_expected_baskets >= m.min_expected_baskets_window
                 AND m.pre_share_index >= m.uplift_index_min AND m.pre_z >= m.z_min, FALSE)   AS pre_uplift,
        COALESCE(m.baseline_days_available >= m.min_baseline_days
                 AND m.event_expected_baskets >= m.min_expected_baskets_window
                 AND m.event_share_index >= m.uplift_index_min AND m.event_z >= m.z_min, FALSE) AS event_uplift,
        COALESCE(m.baseline_days_available >= m.min_baseline_days
                 AND m.post_expected_baskets >= m.min_expected_baskets_window
                 AND m.post_share_index >= m.uplift_index_min AND m.post_z >= m.z_min, FALSE)  AS post_uplift
    FROM window_metrics m
),

-- Overlapping occasions: when two different occasions' core windows share days, a
-- positive result counts only once. Only another positive window can claim a year:
--   * an occasion nested inside a build-up window (at least half its core inside) is
--     claimed by the build-up unless its share is >= nested_occasion_min_ratio x the
--     build-up's share. A build-up is never claimed by an occasion nested inside it.
--   * otherwise the clearly higher share wins: a window mostly inside a longer one must
--     beat it by more than 10% to keep the year.
-- Uses this entity's other occurrences in the same year (window, no shuffle).
specific_tests AS (
    SELECT
        t.*,
        -- The specific test compares the core with the rest of the calendar year.
        -- An incomplete year (currently 2026 YTD) must not provide annual-concentration
        -- evidence, although it may still contribute to the local uplift test.
        COALESCE(t.eligibility_status = 'ELIGIBLE'
                 AND t.year_is_complete = 'Y'
                 AND t.core_baskets >= t.min_specific_baskets
                 AND (t.rest_of_year_baskets = 0 OR t.specific_share_ratio >= t.specific_ratio_min), FALSE)
            AS passes_specific_test
    FROM year_tests t
),

positive_candidates AS (
    SELECT
        s.*,
        MAP_FROM_ENTRIES(COLLECT_LIST(
            CASE WHEN SIZE(s.overlapping_core) > 0
                  AND (s.passes_specific_test OR s.core_status = 'SIGNIFICANT_UPLIFT')
                 THEN STRUCT(s.occurrence_seq, COALESCE(s.core_item_share, CAST(0 AS DOUBLE))) END
        ) OVER (PARTITION BY s.entity_level, s.entity_id, s.event_year)) AS same_year_core_shares
    FROM specific_tests s
),

attribution AS (
    SELECT
        c.*,
        FILTER(c.overlapping_core, oc ->
            ARRAY_CONTAINS(MAP_KEYS(c.same_year_core_shares), oc.seq)
            AND NOT (c.window_type = 'BUILDUP' AND oc.other_inside_pct >= 0.5)
            AND CASE
                    WHEN oc.other_is_buildup AND oc.inside_pct >= 0.5
                        THEN COALESCE(c.core_item_share, CAST(0 AS DOUBLE))
                             < c.nested_occasion_min_ratio * c.same_year_core_shares[oc.seq]
                    ELSE (oc.inside_pct >= 0.5
                          AND c.same_year_core_shares[oc.seq] >= 0.9 * COALESCE(c.core_item_share, CAST(0 AS DOUBLE)))
                         OR c.same_year_core_shares[oc.seq] > 1.1 * COALESCE(c.core_item_share, CAST(0 AS DOUBLE))
                END
        ) AS stronger_overlaps
    FROM positive_candidates c
),

year_flags AS (
    SELECT
        a.*,
        (a.passes_specific_test OR a.core_status = 'SIGNIFICANT_UPLIFT')
            AND SIZE(a.stronger_overlaps) > 0 AS is_attributed_elsewhere,
        CASE WHEN (a.passes_specific_test OR a.core_status = 'SIGNIFICANT_UPLIFT') AND SIZE(a.stronger_overlaps) > 0
             THEN CONCAT_WS(', ', SORT_ARRAY(TRANSFORM(a.stronger_overlaps, x -> x.name))) END AS attributed_to,
        a.eligibility_status = 'ELIGIBLE'
            AND NOT ((a.passes_specific_test OR a.core_status = 'SIGNIFICANT_UPLIFT') AND SIZE(a.stronger_overlaps) > 0)
            AS is_eligible_year
    FROM attribution a
),

year_flags2 AS (
    SELECT
        y.*,
        y.is_eligible_year AND y.passes_specific_test AS is_specific_year,
        y.is_eligible_year
            AND y.core_status IN ('SIGNIFICANT_UPLIFT', 'SIGNIFICANT_DECLINE', 'NOT_SIGNIFICANT') AS is_valid_year,
        y.is_eligible_year AND y.core_status = 'SIGNIFICANT_UPLIFT'  AS is_uplift_year,
        y.is_eligible_year AND y.core_status = 'SIGNIFICANT_DECLINE' AS is_decline_year,
        COALESCE(y.is_eligible_year AND y.core_status = 'SIGNIFICANT_UPLIFT'
                 AND y.core_price_change_pct <= y.promo_price_change_max_pct, FALSE) AS is_price_drop_uplift_year,
        COALESCE(y.is_eligible_year
                 AND y.baseline_days_available >= y.min_baseline_days
                 AND y.post_expected_baskets >= y.min_expected_baskets_window, FALSE) AS is_post_valid_year
    FROM year_flags y
),

-- Across years for each entity x occasion (window: same partitions, no shuffle).
across_years AS (
    SELECT
        f.*,
        SUM(CASE WHEN f.is_eligible_year THEN 1 ELSE 0 END)                                   OVER w AS years_eligible,
        SUM(CASE WHEN f.is_attributed_elsewhere THEN 1 ELSE 0 END)                            OVER w AS years_attributed_elsewhere,
        SUM(CASE WHEN f.is_post_valid_year THEN 1 ELSE 0 END)                                 OVER w AS years_post_valid,
        SUM(CASE WHEN f.is_specific_year THEN 1 ELSE 0 END)                                   OVER w AS years_specific,
        SUM(CASE WHEN f.is_valid_year THEN 1 ELSE 0 END)                                      OVER w AS years_valid,
        SUM(CASE WHEN f.is_uplift_year THEN 1 ELSE 0 END)                                     OVER w AS years_uplift,
        SUM(CASE WHEN f.is_decline_year THEN 1 ELSE 0 END)                                    OVER w AS years_decline,
        SUM(CASE WHEN f.is_price_drop_uplift_year THEN 1 ELSE 0 END)                          OVER w AS years_uplift_with_price_drop,
        SUM(CASE WHEN f.is_eligible_year AND f.core_status = 'LOW_VOLUME' THEN 1 ELSE 0 END)  OVER w AS years_low_volume,
        SUM(CASE WHEN f.is_eligible_year AND f.core_status = 'INSUFFICIENT_BASELINE' THEN 1 ELSE 0 END)
                                                                                              OVER w AS years_insufficient_baseline,
        SUM(CASE WHEN f.is_valid_year AND f.pre_uplift THEN 1 ELSE 0 END)                     OVER w AS years_pre_uplift,
        SUM(CASE WHEN f.is_valid_year AND f.event_uplift THEN 1 ELSE 0 END)                   OVER w AS years_event_uplift,
        SUM(CASE WHEN f.is_post_valid_year AND f.post_uplift THEN 1 ELSE 0 END)               OVER w AS years_post_uplift,
        PERCENTILE_APPROX(CASE WHEN f.is_valid_year THEN f.core_share_index END, 0.5)         OVER w AS median_core_share_index,
        SUM(CASE WHEN f.is_valid_year THEN f.core_baskets END)                                OVER w AS valid_years_core_baskets,
        SUM(CASE WHEN f.is_valid_year THEN f.core_expected_baskets END)                       OVER w AS valid_years_core_expected_baskets
    FROM year_flags2 f
    WINDOW w AS (PARTITION BY f.entity_level, f.entity_id, f.occasion_key)
),

verdict AS (
    SELECT
        a.*,
        a.valid_years_core_baskets / NULLIF(a.valid_years_core_expected_baskets, 0) AS pooled_core_share_index,
        CASE
            WHEN a.years_eligible < a.min_years
                THEN 'Insufficient evidence'
            WHEN a.years_specific > 0 AND a.years_specific >= a.min_year_share * a.years_eligible
                THEN 'Specifically seasonal'
            WHEN a.years_valid < a.min_years
                THEN 'Insufficient evidence'
            WHEN a.years_uplift >= a.min_year_share * a.years_valid
             AND a.median_core_share_index >= a.uplift_index_min
                THEN 'Uplift seasonal'
            WHEN a.years_decline >= a.min_year_share * a.years_valid
             AND a.median_core_share_index <= 1.0 / a.uplift_index_min
             AND a.event_length_days <= 14
                THEN 'Occasion-suppressed'
            ELSE 'Regular'
        END AS seasonality_flag
    FROM across_years a
)

SELECT
    -- who and which occasion
    v.entity_level,
    v.entity_id,
    CASE WHEN v.entity_level = 'TPNB' THEN v.entity_id END AS tpnb,
    v.entity_description,
    v.subclass_code,
    v.subclass_name,
    v.occasion_key,
    v.parent_occasion_key,
    v.window_type,
    v.occasion_name,
    v.occasion_family,
    v.occasion_group,
    v.is_major_occasion,
    v.occurrence_id,
    v.event_year,
    CONCAT(v.occasion_name, ' | ', CAST(v.event_year AS STRING)) AS excel_pivot_column_label,
    v.date_origin,
    v.event_start_date,
    v.event_end_date,
    v.pre_start_date,
    v.post_end_date,
    v.lead_days,
    v.lag_days,
    v.baseline_side,
    v.calendar_years_available,
    v.event_length_days,
    v.overlapping_occasion_names,
    v.baseline_days_available,
    v.baseline_fully_on_sale,
    v.event_low_traffic_days,

    -- the year
    COALESCE(v.annual_baskets, 0)     AS annual_baskets,
    COALESCE(v.annual_sales, 0.0)     AS annual_sales,
    COALESCE(v.annual_units, 0.0)     AS annual_units,
    COALESCE(v.annual_active_days, 0) AS annual_active_days,
    v.year_first_sale_date,
    v.year_last_sale_date,
    v.year_is_complete,
    v.eligibility_status,

    -- pre-event window
    v.pre_baskets,
    v.pre_expected_baskets,
    v.pre_share_index,
    v.pre_z,
    v.pre_sales,
    -- event window
    v.event_baskets,
    v.event_expected_baskets,
    v.event_share_index,
    v.event_z,
    v.event_sales,
    v.event_active_days,
    -- post-event window
    v.post_baskets,
    v.post_expected_baskets,
    v.post_share_index,
    v.post_z,
    v.post_sales,
    -- core window (pre + event): drives the verdict
    v.core_baskets,
    v.core_expected_baskets,
    v.core_share_index,
    v.core_penetration_index,
    v.core_z,
    v.core_status,
    v.core_sales,
    v.core_expected_sales,
    v.core_incremental_sales,
    v.core_sales_uplift_pct,
    v.core_avg_unit_price,
    v.baseline_avg_unit_price,
    v.core_price_change_pct,
    v.specific_share_ratio,
    v.core_item_share,

    -- this year's tests
    v.is_eligible_year,
    v.is_attributed_elsewhere,
    v.attributed_to,
    v.is_specific_year,
    v.is_valid_year,
    v.is_uplift_year,
    v.is_decline_year,
    v.is_price_drop_uplift_year,
    v.pre_uplift,
    v.event_uplift,
    v.post_uplift,

    -- across years (same on every year row of the entity x occasion)
    v.years_eligible,
    v.years_attributed_elsewhere,
    v.years_valid,
    v.years_specific,
    v.years_uplift,
    v.years_decline,
    v.years_uplift_with_price_drop,
    v.years_low_volume,
    v.years_insufficient_baseline,
    v.years_pre_uplift,
    v.years_event_uplift,
    v.years_post_valid,
    v.years_post_uplift,
    v.median_core_share_index,
    v.pooled_core_share_index,
    v.seasonality_flag,
    CASE
        WHEN v.calendar_years_available < v.min_years
            THEN 'LIMITED: calendar has dates for fewer than 2 years'
        WHEN v.years_eligible < v.min_years AND v.years_attributed_elsewhere > 0
            THEN 'LIMITED: overlaps an occasion with a stronger signal'
        WHEN v.years_eligible < v.min_years
            THEN 'LIMITED: on sale around fewer than 2 occurrences'
        WHEN v.seasonality_flag = 'Specifically seasonal'
            THEN 'SUFFICIENT: sells almost only around the occasion'
        WHEN v.years_valid < v.min_years AND v.years_insufficient_baseline > v.years_low_volume
            THEN 'LIMITED: insufficient baseline'
        WHEN v.years_valid < v.min_years
            THEN 'LIMITED: low volume'
        ELSE 'SUFFICIENT'
    END AS evidence_status,
    CASE
        WHEN v.seasonality_flag = 'Uplift seasonal'
         AND v.years_uplift > 0
         AND 2 * v.years_uplift_with_price_drop >= v.years_uplift
            THEN 'POSSIBLY_PROMOTION_DRIVEN'
    END AS promotion_flag,
    CASE
        WHEN v.lag_days > 0
         AND v.years_post_valid >= v.min_years
         AND v.years_post_uplift >= v.min_year_share * v.years_post_valid
            THEN 'Post-event uplift'
    END AS post_event_flag,
    NULLIF(CONCAT_WS('+',
        CASE WHEN v.years_valid >= v.min_years AND v.years_pre_uplift >= v.min_year_share * v.years_valid THEN 'PRE' END,
        CASE WHEN v.years_valid >= v.min_years AND v.years_event_uplift >= v.min_year_share * v.years_valid THEN 'EVENT' END,
        CASE WHEN v.years_post_valid >= v.min_years AND v.years_post_uplift >= v.min_year_share * v.years_post_valid THEN 'POST' END
    ), '') AS timing_profile,
    CONCAT(
        'Specific in ', CAST(v.years_specific AS STRING), '/', CAST(v.years_eligible AS STRING), ' eligible years; ',
        'uplift in ', CAST(v.years_uplift AS STRING), '/', CAST(v.years_valid AS STRING), ' valid years; ',
        'decline in ', CAST(v.years_decline AS STRING), '/', CAST(v.years_valid AS STRING), ' valid years; ',
        CASE WHEN v.years_attributed_elsewhere > 0
             THEN CONCAT(CAST(v.years_attributed_elsewhere AS STRING), ' years credited to an overlapping occasion; ')
             ELSE '' END,
        'median basket-share index ', COALESCE(CAST(ROUND(v.median_core_share_index, 2) AS STRING), 'n/a')
    ) AS classification_reason
FROM verdict v;


-- =====================================================================================
-- 12  WEEKLY PROFILE  (entity x week of year, complete calendar years only)
--     week_index = entity's share of items that week / its share over the whole profile.
--     1.0 = a normal week, 2.0 = twice its usual share. Traffic and basket-size adjusted.
--     Only days between the entity's first and last on-sale date count, so weeks before a
--     launch or after a delisting do not look like zero demand. Gaps inside that range
--     (a seasonal product off the shelf) do count as zero.
--     Weeks are day-of-year blocks of 7 (week 52 absorbs days 358-366), so the same week
--     number means the same dates every year.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_weekly_profile;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_weekly_profile
USING PARQUET AS
WITH complete_years AS (
    SELECT cal_year
    FROM lab_customer_value_analytics.seasonality_v2_calendar_days
    GROUP BY cal_year
    HAVING MIN(cal_date) = MAKE_DATE(cal_year, 1, 1)
       AND MAX(cal_date) = MAKE_DATE(cal_year, 12, 31)
),

-- running total of market items, so any date range's items = two lookups
market_cumulative AS (
    SELECT
        cal_date,
        all_items,
        SUM(all_items) OVER (ORDER BY cal_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_items
    FROM lab_customer_value_analytics.seasonality_v2_calendar_days
),

week_bounds AS (
    SELECT
        c.cal_year,
        LEAST(52, CAST(CEIL(DAYOFYEAR(c.cal_date) / 7.0) AS INT)) AS week_of_year,
        MIN(c.cal_date) AS week_start,
        MAX(c.cal_date) AS week_end
    FROM lab_customer_value_analytics.seasonality_v2_calendar_days c
    JOIN complete_years y ON c.cal_year = y.cal_year
    GROUP BY c.cal_year, LEAST(52, CAST(CEIL(DAYOFYEAR(c.cal_date) / 7.0) AS INT))
),

on_sale_range AS (
    SELECT entity_level, entity_id, MIN(spell_start) AS on_sale_from, MAX(spell_end) AS on_sale_to
    FROM lab_customer_value_analytics.seasonality_v2_availability_spells
    GROUP BY entity_level, entity_id
),

entity_week_range AS (
    SELECT /*+ BROADCAST(w) */
        r.entity_level,
        r.entity_id,
        w.cal_year,
        w.week_of_year,
        GREATEST(w.week_start, r.on_sale_from) AS range_start,
        LEAST(w.week_end, r.on_sale_to)        AS range_end
    FROM on_sale_range r
    CROSS JOIN week_bounds w
    WHERE GREATEST(w.week_start, r.on_sale_from) <= LEAST(w.week_end, r.on_sale_to)
),

entity_week_market AS (
    SELECT /*+ BROADCAST(c1, c2) */
        e.entity_level,
        e.entity_id,
        e.week_of_year,
        SUM(c2.cum_items - c1.cum_items + c1.all_items) AS market_items,
        COUNT(*)                                        AS profile_years
    FROM entity_week_range e
    JOIN market_cumulative c1 ON e.range_start = c1.cal_date
    JOIN market_cumulative c2 ON e.range_end = c2.cal_date
    GROUP BY e.entity_level, e.entity_id, e.week_of_year
),

entity_week_baskets AS (
    SELECT /*+ BROADCAST(y) */
        d.entity_level,
        d.entity_id,
        LEAST(52, CAST(CEIL(DAYOFYEAR(d.sale_date) / 7.0) AS INT)) AS week_of_year,
        SUM(d.baskets) AS baskets
    FROM lab_customer_value_analytics.seasonality_v2_entity_daily d
    JOIN complete_years y ON d.sale_year = y.cal_year
    GROUP BY d.entity_level, d.entity_id, LEAST(52, CAST(CEIL(DAYOFYEAR(d.sale_date) / 7.0) AS INT))
),

joined AS (
    SELECT
        em.entity_level,
        em.entity_id,
        em.week_of_year,
        em.profile_years,
        COALESCE(eb.baskets, 0) AS baskets,
        em.market_items
    FROM entity_week_market em
    LEFT JOIN entity_week_baskets eb
      ON em.entity_level = eb.entity_level
     AND em.entity_id = eb.entity_id
     AND em.week_of_year = eb.week_of_year
)

SELECT
    j.entity_level,
    j.entity_id,
    j.week_of_year,
    j.profile_years,
    j.baskets,
    j.market_items,
    j.baskets / NULLIF(j.market_items, 0) AS item_share,
    (j.baskets / NULLIF(j.market_items, 0))
        / NULLIF(SUM(j.baskets) OVER e / NULLIF(SUM(j.market_items) OVER e, 0), 0) AS week_index,
    SUM(j.baskets) OVER e AS profile_baskets
FROM joined j
WINDOW e AS (PARTITION BY j.entity_level, j.entity_id);


-- =====================================================================================
-- 13  ENTITY PROFILE  (one row per entity)
--     Time of year: share of the weekly index in the strongest 8 consecutive weeks
--                   (wrapping over New Year). Flat = 8 / 52 = 15.4%.
--     Day of week:  share of items on sale days by weekday group, against the entity's
--                   overall share. Ordinary days only: short occasions' core windows,
--                   major occasions' windows and low-traffic days are left out.
--     Availability: pattern of on-sale spells.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_entity_profile;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_entity_profile
USING PARQUET AS
WITH weekly AS (
    SELECT
        entity_level,
        entity_id,
        MAX(profile_baskets) AS profile_baskets,
        MAX(profile_years)   AS profile_years,
        MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(week_of_year, COALESCE(week_index, CAST(0 AS DOUBLE))))) AS week_map
    FROM lab_customer_value_analytics.seasonality_v2_weekly_profile
    GROUP BY entity_level, entity_id
),

weekly_array AS (
    SELECT
        w.entity_level,
        w.entity_id,
        w.profile_baskets,
        w.profile_years,
        TRANSFORM(SEQUENCE(1, 52), wk ->
            CASE WHEN ARRAY_CONTAINS(MAP_KEYS(w.week_map), wk) THEN w.week_map[wk] ELSE CAST(0 AS DOUBLE) END
        ) AS week_idx
    FROM weekly w
),

weekly_windows AS (
    SELECT
        w.*,
        TRANSFORM(SEQUENCE(0, 51), s ->
            AGGREGATE(SEQUENCE(0, 7), CAST(0 AS DOUBLE), (acc, i) ->
                acc + ELEMENT_AT(w.week_idx, CAST(PMOD(s + i, 52) + 1 AS INT)))
        ) AS idx_8wk,
        AGGREGATE(w.week_idx, CAST(0 AS DOUBLE), (acc, x) -> acc + x) AS idx_total
    FROM weekly_array w
),

weekly_summary AS (
    SELECT
        ww.entity_level,
        ww.entity_id,
        ww.profile_baskets,
        ww.profile_years,
        100.0 * ARRAY_MAX(ww.idx_8wk) / NULLIF(ww.idx_total, 0)  AS best_8_week_share_pct,
        CAST(ARRAY_POSITION(ww.idx_8wk, ARRAY_MAX(ww.idx_8wk)) AS INT) AS best_8_week_start,
        CAST(ARRAY_POSITION(ww.week_idx, ARRAY_MAX(ww.week_idx)) AS INT) AS peak_week,
        SQRT(AGGREGATE(ww.week_idx, CAST(0 AS DOUBLE), (acc, x) -> acc + POW(x - ww.idx_total / 52.0, 2)) / 52.0)
            / NULLIF(ww.idx_total / 52.0, 0) AS seasonality_strength_cv
    FROM weekly_windows ww
),

weekly_labels AS (
    SELECT
        s.*,
        s.best_8_week_share_pct / (100.0 * 8.0 / 52.0) AS seasonal_concentration_index,
        CONCAT('Weeks ', LPAD(CAST(s.best_8_week_start AS STRING), 2, '0'), '-',
               LPAD(CAST(PMOD(s.best_8_week_start - 1 + 7, 52) + 1 AS STRING), 2, '0')) AS peak_weeks
    FROM weekly_summary s
),

weekly_final AS (
    SELECT
        l.*,
        CASE
            WHEN MONTH(DATE_ADD(DATE '2023-01-01', (l.peak_week - 1) * 7 + 3)) IN (12, 1, 2)  THEN 'Winter'
            WHEN MONTH(DATE_ADD(DATE '2023-01-01', (l.peak_week - 1) * 7 + 3)) IN (3, 4, 5)   THEN 'Spring'
            WHEN MONTH(DATE_ADD(DATE '2023-01-01', (l.peak_week - 1) * 7 + 3)) IN (6, 7, 8)   THEN 'Summer'
            ELSE 'Autumn'
        END AS peak_season
    FROM weekly_labels l
),

occasion_days AS (
    SELECT DISTINCT r.role_date AS occasion_date
    FROM lab_customer_value_analytics.seasonality_v2_date_roles r
    JOIN lab_customer_value_analytics.seasonality_v2_occurrences o
      ON r.occurrence_id = o.occurrence_id
    WHERE (r.role IN ('PRE', 'EVENT') AND o.event_length_days <= 7)
       OR (r.role IN ('PRE', 'EVENT', 'POST') AND o.is_major_occasion = 'Y')
),

ordinary_days AS (
    SELECT /*+ BROADCAST(od) */ c.cal_date, c.all_items
    FROM lab_customer_value_analytics.seasonality_v2_calendar_days c
    LEFT ANTI JOIN occasion_days od ON c.cal_date = od.occasion_date
    WHERE c.is_low_traffic_day = 'N'
),

day_of_week AS (
    SELECT /*+ BROADCAST(c) */
        d.entity_level,
        d.entity_id,
        -- day-of-week figures use ordinary days only (o is NULL on occasion and low-traffic days)
        SUM(CASE WHEN o.cal_date IS NOT NULL AND d.sale_day_of_week IN (1, 7) THEN d.baskets ELSE 0 END)          AS weekend_baskets,
        SUM(CASE WHEN o.cal_date IS NOT NULL AND d.sale_day_of_week IN (1, 7) THEN c.all_items ELSE 0 END)        AS weekend_items,
        SUM(CASE WHEN o.cal_date IS NOT NULL AND d.sale_day_of_week = 6 THEN d.baskets ELSE 0 END)                AS friday_baskets,
        SUM(CASE WHEN o.cal_date IS NOT NULL AND d.sale_day_of_week = 6 THEN c.all_items ELSE 0 END)              AS friday_items,
        SUM(CASE WHEN o.cal_date IS NOT NULL AND d.sale_day_of_week BETWEEN 2 AND 5 THEN d.baskets ELSE 0 END)    AS midweek_baskets,
        SUM(CASE WHEN o.cal_date IS NOT NULL AND d.sale_day_of_week BETWEEN 2 AND 5 THEN c.all_items ELSE 0 END)  AS midweek_items,
        SUM(CASE WHEN o.cal_date IS NOT NULL THEN d.baskets ELSE 0 END)   AS ordinary_day_baskets,
        SUM(CASE WHEN o.cal_date IS NOT NULL THEN c.all_items ELSE 0 END) AS ordinary_day_items,
        SUM(d.baskets)    AS total_baskets,
        COUNT(DISTINCT d.sale_year) AS years_with_sales,
        SUM(d.sales)      AS total_sales,
        SUM(d.units)      AS total_units,
        SUM(CASE WHEN d.sale_year = 2023 THEN d.sales ELSE 0.0 END) AS sales_2023,
        SUM(CASE WHEN d.sale_year = 2024 THEN d.sales ELSE 0.0 END) AS sales_2024,
        SUM(CASE WHEN d.sale_year = 2025 THEN d.sales ELSE 0.0 END) AS sales_2025,
        SUM(CASE WHEN d.sale_year = 2026 THEN d.sales ELSE 0.0 END) AS sales_2026_ytd,
        COUNT(*)          AS sale_days,
        MIN(d.sale_date)  AS first_sale_date,
        MAX(d.sale_date)  AS last_sale_date
    FROM lab_customer_value_analytics.seasonality_v2_entity_daily d
    JOIN lab_customer_value_analytics.seasonality_v2_calendar_days c
      ON d.sale_date = c.cal_date
    LEFT JOIN ordinary_days o
      ON d.sale_date = o.cal_date
    GROUP BY d.entity_level, d.entity_id
),

spells AS (
    SELECT
        entity_level,
        entity_id,
        COUNT(*)                         AS on_sale_spells,
        MAX(spell_days)                  AS longest_spell_days,
        SUM(spell_days)                  AS on_sale_days,
        MIN(spell_start)                 AS first_on_sale_date,
        MAX(spell_end)                   AS last_on_sale_date,
        COUNT(DISTINCT YEAR(spell_start)) AS spell_start_years
    FROM lab_customer_value_analytics.seasonality_v2_availability_spells
    GROUP BY entity_level, entity_id
),

-- day-of-week indices with a significance check (counts vs expected at the entity's
-- ordinary-day share of items)
dow_metrics AS (
    SELECT
        d.*,
        d.ordinary_day_baskets / NULLIF(d.ordinary_day_items, 0) AS ordinary_share,
        (d.weekend_baskets / NULLIF(d.weekend_items, 0))
            / NULLIF(d.ordinary_day_baskets / NULLIF(d.ordinary_day_items, 0), 0) AS weekend_index,
        (d.friday_baskets / NULLIF(d.friday_items, 0))
            / NULLIF(d.ordinary_day_baskets / NULLIF(d.ordinary_day_items, 0), 0) AS friday_index,
        (d.midweek_baskets / NULLIF(d.midweek_items, 0))
            / NULLIF(d.ordinary_day_baskets / NULLIF(d.ordinary_day_items, 0), 0) AS midweek_index,
        (d.weekend_baskets - d.weekend_items * d.ordinary_day_baskets / NULLIF(d.ordinary_day_items, 0))
            / NULLIF(SQRT(d.weekend_items * d.ordinary_day_baskets / NULLIF(d.ordinary_day_items, 0)), 0) AS weekend_z,
        (d.friday_baskets - d.friday_items * d.ordinary_day_baskets / NULLIF(d.ordinary_day_items, 0))
            / NULLIF(SQRT(d.friday_items * d.ordinary_day_baskets / NULLIF(d.ordinary_day_items, 0)), 0) AS friday_z
    FROM day_of_week d
),

coverage AS (
    SELECT
        MIN(data_start_date)   AS data_start_date,
        MIN(data_through_date) AS data_through_date,
        COUNT(*)               AS coverage_days
    FROM lab_customer_value_analytics.seasonality_v2_calendar_days
)

SELECT
    d.entity_level,
    d.entity_id,

    -- volume
    d.total_baskets,
    d.total_sales,
    d.total_units,
    d.sales_2023,
    d.sales_2024,
    d.sales_2025,
    d.sales_2026_ytd,
    d.total_sales / NULLIF(d.total_units, 0)   AS avg_unit_price,
    d.total_sales / NULLIF(d.total_baskets, 0) AS avg_sales_per_basket,
    d.sale_days,
    d.years_with_sales,
    d.first_sale_date,
    d.last_sale_date,

    -- time of year
    w.profile_years,
    w.profile_baskets,
    w.best_8_week_share_pct,
    w.seasonal_concentration_index,
    w.seasonality_strength_cv,
    w.peak_week,
    w.peak_weeks,
    w.peak_season,
    CASE
        WHEN w.entity_id IS NULL OR w.profile_baskets < p.min_profile_baskets THEN 'Low volume'
        WHEN w.seasonal_concentration_index >= 2.0 THEN CONCAT('Strong ', LOWER(w.peak_season), ' peak')
        WHEN w.seasonal_concentration_index >= 1.4 THEN CONCAT(w.peak_season, ' peak')
        ELSE 'Even through the year'
    END AS time_of_year_pattern,

    -- day of week (ordinary days only)
    d.weekend_index,
    d.friday_index,
    d.midweek_index,
    d.weekend_z,
    d.friday_z,
    CASE
        WHEN d.ordinary_day_baskets < p.min_profile_baskets THEN 'Low volume'
        WHEN d.weekend_index >= p.dow_index_min AND d.weekend_z >= 3.0 AND d.weekend_index >= d.friday_index
            THEN 'Weekend-heavy'
        WHEN d.friday_index >= p.dow_index_min AND d.friday_z >= 3.0
            THEN 'Friday-heavy'
        WHEN d.weekend_index <= 1.0 / p.dow_index_min AND d.weekend_z <= -3.0
            THEN 'Weekday-heavy'
        ELSE 'Even across the week'
    END AS day_of_week_pattern,

    -- availability
    s.on_sale_spells,
    s.longest_spell_days,
    s.on_sale_days,
    s.first_on_sale_date,
    s.last_on_sale_date,
    CASE
        WHEN s.on_sale_days >= 0.8 * cv.coverage_days THEN 'Year-round'
        WHEN s.on_sale_spells >= 2 AND s.longest_spell_days <= 150 AND s.spell_start_years >= 2 THEN 'Recurring seasonal'
        WHEN s.on_sale_spells = 1 AND s.longest_spell_days <= p.limited_edition_max_days
         AND s.first_on_sale_date > DATE_ADD(cv.data_start_date, 28)
         AND s.last_on_sale_date < DATE_SUB(cv.data_through_date, 28) THEN 'One-off limited edition'
        WHEN s.first_on_sale_date > DATE_ADD(cv.data_start_date, 28)
         AND s.last_on_sale_date >= DATE_SUB(cv.data_through_date, 28) THEN 'Launched during period'
        WHEN s.first_on_sale_date <= DATE_ADD(cv.data_start_date, 28)
         AND s.last_on_sale_date < DATE_SUB(cv.data_through_date, 28) THEN 'Delisted during period'
        ELSE 'Intermittent'
    END AS availability_pattern
FROM dow_metrics d
LEFT JOIN weekly_final w
  ON d.entity_level = w.entity_level
 AND d.entity_id = w.entity_id
LEFT JOIN spells s
  ON d.entity_level = s.entity_level
 AND d.entity_id = s.entity_id
CROSS JOIN coverage cv
CROSS JOIN lab_customer_value_analytics.seasonality_v2_params p;


-- =====================================================================================
-- 14  TPNB SUMMARY  (one row per TPNB: every flag)
--     The 3 core flags, rolled up from the per-occasion verdicts:
--       specifically_seasonal_flag  Y if "Specifically seasonal" for at least one occasion
--       uplift_seasonal_flag        Y if "Uplift seasonal" for at least one occasion
--       regular_flag                Y if seasonal for no occasion, suppressed by none, and
--                                     judged "Regular" for at least one
--     Plus occasion_suppressed_flag, promotion_driven_flag, post_event_uplift_flag,
--     occasion-family flags, time of year, day of week, availability, and the subclass
--     fallback for TPNBs sold in 2+ years but too small to judge on their own.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_tpnb_summary;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_tpnb_summary
USING PARQUET AS
WITH entity_occasion AS (
    SELECT
        entity_level,
        entity_id,
        occasion_key,
        MAX(occasion_name)           AS occasion_name,
        MAX(occasion_family)         AS occasion_family,
        MAX(seasonality_flag)        AS seasonality_flag,
        MAX(promotion_flag)          AS promotion_flag,
        MAX(post_event_flag)         AS post_event_flag,
        MAX(timing_profile)          AS timing_profile,
        MAX(median_core_share_index) AS median_core_share_index
    FROM lab_customer_value_analytics.seasonality_v2_occasion_metrics
    GROUP BY entity_level, entity_id, occasion_key
),

entity_flags AS (
    SELECT
        entity_level,
        entity_id,
        COUNT(*) AS occasions_evaluated,
        SUM(CASE WHEN seasonality_flag = 'Specifically seasonal' THEN 1 ELSE 0 END) AS specifically_seasonal_occasions,
        SUM(CASE WHEN seasonality_flag = 'Uplift seasonal'       THEN 1 ELSE 0 END) AS uplift_seasonal_occasions,
        SUM(CASE WHEN seasonality_flag = 'Occasion-suppressed'   THEN 1 ELSE 0 END) AS suppressed_occasions,
        SUM(CASE WHEN seasonality_flag = 'Regular'               THEN 1 ELSE 0 END) AS regular_occasions,
        SUM(CASE WHEN seasonality_flag = 'Insufficient evidence' THEN 1 ELSE 0 END) AS insufficient_evidence_occasions,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(
            CASE WHEN seasonality_flag = 'Specifically seasonal' THEN occasion_name END))) AS specifically_seasonal_occasion_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(
            CASE WHEN seasonality_flag = 'Uplift seasonal' THEN occasion_name END)))       AS uplift_seasonal_occasion_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(
            CASE WHEN seasonality_flag = 'Occasion-suppressed' THEN occasion_name END)))   AS suppressed_occasion_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(
            CASE WHEN promotion_flag IS NOT NULL THEN occasion_name END)))                 AS promotion_driven_occasion_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(
            CASE WHEN post_event_flag IS NOT NULL THEN occasion_name END)))                AS post_event_uplift_occasion_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(
            CASE WHEN seasonality_flag IN ('Specifically seasonal', 'Uplift seasonal') THEN occasion_family END))) AS seasonal_family_list,

        MAX(CASE WHEN occasion_family = 'CHRISTMAS_NEW_YEAR'       AND seasonality_flag IN ('Specifically seasonal', 'Uplift seasonal') THEN 'Y' ELSE 'N' END) AS family_christmas_new_year,
        MAX(CASE WHEN occasion_family = 'EASTER'                   AND seasonality_flag IN ('Specifically seasonal', 'Uplift seasonal') THEN 'Y' ELSE 'N' END) AS family_easter,
        MAX(CASE WHEN occasion_family = 'GIFTING_CELEBRATIONS'     AND seasonality_flag IN ('Specifically seasonal', 'Uplift seasonal') THEN 'Y' ELSE 'N' END) AS family_gifting_celebrations,
        MAX(CASE WHEN occasion_family = 'HALLOWEEN_BONFIRE'        AND seasonality_flag IN ('Specifically seasonal', 'Uplift seasonal') THEN 'Y' ELSE 'N' END) AS family_halloween_bonfire,
        MAX(CASE WHEN occasion_family = 'BANK_HOLIDAYS'            AND seasonality_flag IN ('Specifically seasonal', 'Uplift seasonal') THEN 'Y' ELSE 'N' END) AS family_bank_holidays,
        MAX(CASE WHEN occasion_family = 'FOOD_MOMENTS'             AND seasonality_flag IN ('Specifically seasonal', 'Uplift seasonal') THEN 'Y' ELSE 'N' END) AS family_food_moments,
        MAX(CASE WHEN occasion_family = 'RETAIL_EVENTS'            AND seasonality_flag IN ('Specifically seasonal', 'Uplift seasonal') THEN 'Y' ELSE 'N' END) AS family_retail_events,
        MAX(CASE WHEN occasion_family = 'NATIONAL_DAYS'            AND seasonality_flag IN ('Specifically seasonal', 'Uplift seasonal') THEN 'Y' ELSE 'N' END) AS family_national_days,
        MAX(CASE WHEN occasion_family = 'WORLD_RELIGIOUS_CULTURAL' AND seasonality_flag IN ('Specifically seasonal', 'Uplift seasonal') THEN 'Y' ELSE 'N' END) AS family_world_religious_cultural,
        MAX(CASE WHEN occasion_family = 'SPORT'                    AND seasonality_flag IN ('Specifically seasonal', 'Uplift seasonal') THEN 'Y' ELSE 'N' END) AS family_sport,
        MAX(CASE WHEN occasion_family = 'ENTERTAINMENT_FESTIVALS'  AND seasonality_flag IN ('Specifically seasonal', 'Uplift seasonal') THEN 'Y' ELSE 'N' END) AS family_entertainment_festivals,
        MAX(CASE WHEN occasion_family = 'AWARENESS_DAYS'           AND seasonality_flag IN ('Specifically seasonal', 'Uplift seasonal') THEN 'Y' ELSE 'N' END) AS family_awareness_days,

        -- strongest seasonal occasion: specific ones first, then the highest median index
        MAX_BY(occasion_name, CASE
            WHEN seasonality_flag = 'Specifically seasonal' THEN 1000000.0 + COALESCE(median_core_share_index, 0.0)
            WHEN seasonality_flag = 'Uplift seasonal'       THEN median_core_share_index END) AS peak_occasion,
        MAX_BY(median_core_share_index, CASE
            WHEN seasonality_flag = 'Specifically seasonal' THEN 1000000.0 + COALESCE(median_core_share_index, 0.0)
            WHEN seasonality_flag = 'Uplift seasonal'       THEN median_core_share_index END) AS peak_occasion_median_index,
        MAX_BY(timing_profile, CASE
            WHEN seasonality_flag = 'Specifically seasonal' THEN 1000000.0 + COALESCE(median_core_share_index, 0.0)
            WHEN seasonality_flag = 'Uplift seasonal'       THEN median_core_share_index END) AS peak_occasion_timing
    FROM entity_occasion
    GROUP BY entity_level, entity_id
),

labelled AS (
    SELECT
        f.*,
        CASE
            WHEN f.specifically_seasonal_occasions > 0 THEN 'Specifically seasonal'
            WHEN f.uplift_seasonal_occasions > 0       THEN 'Uplift seasonal'
            WHEN f.suppressed_occasions > 0            THEN 'Occasion-suppressed'
            WHEN f.regular_occasions > 0               THEN 'Regular'
            ELSE 'Insufficient evidence'
        END AS overall_seasonality
    FROM entity_flags f
),

tpnb_entities AS (
    SELECT * FROM lab_customer_value_analytics.seasonality_v2_entity_profile WHERE entity_level = 'TPNB'
),

tpnb_flags AS (
    SELECT * FROM labelled WHERE entity_level = 'TPNB'
),

subclass_flags AS (
    SELECT * FROM labelled WHERE entity_level = 'SUBCLASS'
)

SELECT
    e.entity_id AS tpnb,
    p.product_master_tpna,
    COALESCE(p.product_description, e.entity_id) AS product_description,
    COALESCE(p.product_market_code, 'UNMATCHED')  AS product_market_code,
    p.hierarchy_division_name,
    p.hierarchy_department_name,
    p.hierarchy_section_name,
    p.hierarchy_class_name,
    p.hierarchy_subclass_code,
    p.hierarchy_subclass_name,
    p.gb_launch_date,
    p.gb_deactive_date,

    -- the 3 core flags
    CASE WHEN COALESCE(f.specifically_seasonal_occasions, 0) > 0 THEN 'Y' ELSE 'N' END AS specifically_seasonal_flag,
    CASE WHEN COALESCE(f.uplift_seasonal_occasions, 0) > 0 THEN 'Y' ELSE 'N' END       AS uplift_seasonal_flag,
    CASE WHEN COALESCE(f.overall_seasonality, '') = 'Regular' THEN 'Y' ELSE 'N' END     AS regular_flag,
    -- further flags
    CASE WHEN COALESCE(f.suppressed_occasions, 0) > 0 THEN 'Y' ELSE 'N' END             AS occasion_suppressed_flag,
    CASE WHEN COALESCE(f.promotion_driven_occasion_list, '') <> '' THEN 'Y' ELSE 'N' END AS promotion_driven_flag,
    CASE WHEN COALESCE(f.post_event_uplift_occasion_list, '') <> '' THEN 'Y' ELSE 'N' END AS post_event_uplift_flag,
    e.availability_pattern,
    CASE WHEN e.availability_pattern IN ('One-off limited edition', 'Recurring seasonal') THEN 'Y' ELSE 'N' END
        AS limited_edition_flag,

    -- overall label and where the evidence comes from
    COALESCE(f.overall_seasonality, 'Insufficient evidence') AS product_seasonality,
    sf.overall_seasonality AS subclass_seasonality,
    CASE
        WHEN COALESCE(f.overall_seasonality, 'Insufficient evidence') = 'Insufficient evidence'
         AND sf.overall_seasonality IN ('Specifically seasonal', 'Uplift seasonal')
         AND e.years_with_sales >= 2
            THEN sf.overall_seasonality
    END AS inherited_subclass_seasonality,
    CASE
        WHEN COALESCE(f.overall_seasonality, 'Insufficient evidence') <> 'Insufficient evidence' THEN 'TPNB'
        WHEN sf.overall_seasonality IN ('Specifically seasonal', 'Uplift seasonal') AND e.years_with_sales >= 2
            THEN 'SUBCLASS (inherited)'
        ELSE 'NONE'
    END AS seasonality_evidence_source,

    -- which occasions drive the flags
    f.specifically_seasonal_occasion_list,
    f.uplift_seasonal_occasion_list,
    f.suppressed_occasion_list,
    f.promotion_driven_occasion_list,
    f.post_event_uplift_occasion_list,
    f.seasonal_family_list,
    CASE WHEN sf.specifically_seasonal_occasion_list <> '' OR sf.uplift_seasonal_occasion_list <> ''
         THEN CONCAT_WS(', ', NULLIF(sf.specifically_seasonal_occasion_list, ''), NULLIF(sf.uplift_seasonal_occasion_list, ''))
    END AS subclass_seasonal_occasion_list,
    f.peak_occasion,
    f.peak_occasion_median_index,
    f.peak_occasion_timing,
    COALESCE(f.occasions_evaluated, 0)             AS occasions_evaluated,
    COALESCE(f.specifically_seasonal_occasions, 0) AS specifically_seasonal_occasions,
    COALESCE(f.uplift_seasonal_occasions, 0)       AS uplift_seasonal_occasions,
    COALESCE(f.suppressed_occasions, 0)            AS suppressed_occasions,
    COALESCE(f.regular_occasions, 0)               AS regular_occasions,
    COALESCE(f.insufficient_evidence_occasions, 0) AS insufficient_evidence_occasions,

    -- occasion families (Y = seasonal for at least one occasion in the family)
    COALESCE(f.family_christmas_new_year, 'N')       AS family_christmas_new_year,
    COALESCE(f.family_easter, 'N')                   AS family_easter,
    COALESCE(f.family_gifting_celebrations, 'N')     AS family_gifting_celebrations,
    COALESCE(f.family_halloween_bonfire, 'N')        AS family_halloween_bonfire,
    COALESCE(f.family_bank_holidays, 'N')            AS family_bank_holidays,
    COALESCE(f.family_food_moments, 'N')             AS family_food_moments,
    COALESCE(f.family_retail_events, 'N')            AS family_retail_events,
    COALESCE(f.family_national_days, 'N')            AS family_national_days,
    COALESCE(f.family_world_religious_cultural, 'N') AS family_world_religious_cultural,
    COALESCE(f.family_sport, 'N')                    AS family_sport,
    COALESCE(f.family_entertainment_festivals, 'N')  AS family_entertainment_festivals,
    COALESCE(f.family_awareness_days, 'N')           AS family_awareness_days,

    -- time of year and day of week
    e.time_of_year_pattern,
    e.peak_season,
    e.peak_week,
    e.peak_weeks,
    e.best_8_week_share_pct,
    e.seasonal_concentration_index,
    e.seasonality_strength_cv,
    e.day_of_week_pattern,
    e.weekend_index,
    e.friday_index,
    e.midweek_index,

    -- availability
    e.on_sale_spells,
    e.longest_spell_days,
    e.first_on_sale_date,
    e.last_on_sale_date,

    -- volume, 2023 to the latest data date
    e.total_baskets,
    e.total_sales,
    e.total_units,
    e.sales_2023,
    e.sales_2024,
    e.sales_2025,
    e.sales_2026_ytd,
    e.avg_unit_price,
    e.avg_sales_per_basket,
    e.first_sale_date,
    e.last_sale_date
FROM tpnb_entities e
LEFT JOIN lab_customer_value_analytics.seasonality_v2_product_dim p
  ON TRY_CAST(e.entity_id AS BIGINT) = p.tpnb
LEFT JOIN tpnb_flags f
  ON e.entity_id = f.entity_id
LEFT JOIN subclass_flags sf
  ON p.subclass_entity_id = sf.entity_id;


-- =====================================================================================
-- 15  VALIDATION LABELS  (a seed list to review and extend, not ground truth)
--     Products whose description matches a keyword for something we already know is
--     seasonal, or an everyday staple. Add, remove or relabel rows by hand, then run the
--     scoring queries in section 16 before and after every method change.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_validation_labels;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_validation_labels
USING PARQUET AS
WITH rules AS (
    SELECT * FROM VALUES
        ('%MINCE PIE%',          'SEASONAL', 'CHRISTMAS_NEW_YEAR'),
        ('%CHRISTMAS%',          'SEASONAL', 'CHRISTMAS_NEW_YEAR'),
        ('%XMAS%',               'SEASONAL', 'CHRISTMAS_NEW_YEAR'),
        ('%ADVENT CALENDAR%',    'SEASONAL', 'CHRISTMAS_NEW_YEAR'),
        ('%PANETTONE%',          'SEASONAL', 'CHRISTMAS_NEW_YEAR'),
        ('%STOLLEN%',            'SEASONAL', 'CHRISTMAS_NEW_YEAR'),
        ('%SELECTION BOX%',      'SEASONAL', 'CHRISTMAS_NEW_YEAR'),
        ('%PIGS IN BLANKETS%',   'SEASONAL', 'CHRISTMAS_NEW_YEAR'),
        ('%EASTER EGG%',         'SEASONAL', 'EASTER'),
        ('%HOT CROSS BUN%',      'SEASONAL', 'EASTER'),
        ('%SIMNEL%',             'SEASONAL', 'EASTER'),
        ('%MINI EGGS%',          'SEASONAL', 'EASTER'),
        ('%CREME EGG%',          'SEASONAL', 'EASTER'),
        ('%HALLOWEEN%',          'SEASONAL', 'HALLOWEEN_BONFIRE'),
        ('%CARVING PUMPKIN%',    'SEASONAL', 'HALLOWEEN_BONFIRE'),
        ('%SPARKLER%',           'SEASONAL', 'HALLOWEEN_BONFIRE'),
        ('%VALENTINE%',          'SEASONAL', 'GIFTING_CELEBRATIONS'),
        ('%MOTHERS DAY%',        'SEASONAL', 'GIFTING_CELEBRATIONS'),
        ('%FATHERS DAY%',        'SEASONAL', 'GIFTING_CELEBRATIONS'),
        ('%PANCAKE%',            'SEASONAL', 'FOOD_MOMENTS'),
        ('%HAGGIS%',             'SEASONAL', 'NATIONAL_DAYS'),
        ('%BANANAS%',            'EVERYDAY', CAST(NULL AS STRING)),
        ('%TOILET TISSUE%',      'EVERYDAY', CAST(NULL AS STRING)),
        ('%TOILET ROLL%',        'EVERYDAY', CAST(NULL AS STRING)),
        ('%TEA BAGS%',           'EVERYDAY', CAST(NULL AS STRING)),
        ('%TEABAGS%',            'EVERYDAY', CAST(NULL AS STRING)),
        ('%BAKED BEANS%',        'EVERYDAY', CAST(NULL AS STRING)),
        ('%WASHING UP LIQUID%',  'EVERYDAY', CAST(NULL AS STRING)),
        ('%TOOTHPASTE%',         'EVERYDAY', CAST(NULL AS STRING)),
        ('%SEMI SKIMMED MILK%',  'EVERYDAY', CAST(NULL AS STRING)),
        ('%WHITE BREAD%',        'EVERYDAY', CAST(NULL AS STRING))
    AS r(pattern, expected_label, expected_family)
),

matches AS (
    SELECT /*+ BROADCAST(r) */
        p.tpnb,
        p.product_description,
        r.pattern,
        r.expected_label,
        r.expected_family,
        ROW_NUMBER() OVER (
            PARTITION BY p.tpnb
            ORDER BY CASE r.expected_label WHEN 'SEASONAL' THEN 0 ELSE 1 END, r.pattern
        ) AS match_rn
    FROM lab_customer_value_analytics.seasonality_v2_product_dim p
    JOIN rules r
      ON UPPER(p.product_description) LIKE r.pattern
)

SELECT
    CAST(tpnb AS STRING) AS tpnb,
    product_description,
    expected_label,
    expected_family,
    pattern AS matched_keyword,
    'KEYWORD_SEED' AS label_source
FROM matches
WHERE match_rn = 1;


-- =====================================================================================

-- =====================================================================================
-- PART B - ABNORMAL-TRADING, ABSOLUTE-DEMAND AND SINGLE-OCCURRENCE CANDIDATE (v2.2)
-- =====================================================================================
-- =====================================================================================
-- SEASONALITY THEMES v2.2 CANDIDATE OVERLAY
-- Spark SQL / Databricks / Kyuubi compatible. No QUALIFY.
--
-- Purpose
-- -------
-- Build a parallel v2.2 candidate alongside the existing v2.1 tables so that the method
-- can be A/B tested before the production tables are replaced.
--
-- The overlay addresses the real-data findings:
--   1) separates genuine absolute demand growth from share gains caused by an abnormal
--      low-traffic trading regime (for example Boxing Day / New Year's Day);
--   2) preserves those share gains as an OCCASION_MISSION_SHIFT diagnostic rather than
--      silently calling the product seasonal;
--   3) creates a same-parent-occasion, hierarchy-backed evidence tier for products with
--      only one observed seasonal occurrence;
--   4) keeps periodic/one-off event responses separate from recurring seasonality;
--   5) labels current promotion dates as PRICE_INFERRED, not exact commercial dates.
--
-- Prerequisite
-- ------------
-- Run seasonality_themes_v2_1_d9_patched.sql through statement 15 first. The current
-- v2.1 tables remain untouched. This file creates only seasonality_v2_2_* tables.
--
-- Recommended run order
-- ---------------------
-- A  seasonality_v2_2_params
-- B  seasonality_v2_2_market_context
-- C  seasonality_v2_2_absolute_metrics
-- D  seasonality_v2_2_occasion_metrics
-- E  seasonality_v2_2_product_occasion_evidence
-- F  seasonality_v2_2_tpnb_summary
-- G  seasonality_v2_2_responder_products
-- =====================================================================================


-- =====================================================================================
-- A  V2.2 PARAMETERS
--     These are deliberately isolated so the candidate can be retuned without touching
--     sales.invoice_line or the v2.1 tables.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_2_params;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_2_params
USING PARQUET AS
SELECT
    CAST(0.65 AS DOUBLE) AS abnormal_market_basket_ratio_max,
        -- core market baskets below 65% of the same-weekday baseline expectation
    CAST(0.50 AS DOUBLE) AS abnormal_low_traffic_day_share_min,
        -- at least half of core days are low traffic
    CAST(1.00 AS DOUBLE) AS abnormal_absolute_index_min,
        -- on an abnormal trading window, actual entity baskets/item-slots must at least
        -- match the normal same-weekday absolute level before it can remain true uplift
    CAST(0.00 AS DOUBLE) AS abnormal_absolute_z_min,
        -- provisional guard: actual absolute demand must not be below expectation
    CAST(1 AS INT) AS supported_single_min_positive_years,
    CAST(1 AS INT) AS supported_single_max_positive_years;


-- =====================================================================================
-- B  OCCASION MARKET CONTEXT
--     One row per occurrence. It measures whether the core window is a normal trading
--     regime or a materially reduced-traffic regime.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_2_market_context;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_2_market_context
USING PARQUET AS
WITH roles AS (
    SELECT
        r.occurrence_id,
        r.role,
        r.weekday,
        r.role_date,
        r.all_baskets,
        r.is_low_traffic_day,
        c.active_outlets
    FROM lab_customer_value_analytics.seasonality_v2_date_roles r
    JOIN lab_customer_value_analytics.seasonality_v2_calendar_days c
      ON r.role_date = c.cal_date
    WHERE r.role IN ('PRE', 'EVENT', 'BASE')
),

role_days AS (
    SELECT
        occurrence_id,
        CASE WHEN role IN ('PRE', 'EVENT') THEN 'CORE' ELSE 'BASE' END AS role_group,
        weekday,
        COUNT(*) AS days,
        SUM(all_baskets) AS market_baskets,
        SUM(CASE WHEN is_low_traffic_day = 'Y' THEN 1 ELSE 0 END) AS low_traffic_days,
        AVG(active_outlets) AS avg_active_outlets
    FROM roles
    GROUP BY occurrence_id,
             CASE WHEN role IN ('PRE', 'EVENT') THEN 'CORE' ELSE 'BASE' END,
             weekday
),

pivoted AS (
    SELECT
        occurrence_id,
        ARRAY(
            SUM(CASE WHEN role_group = 'CORE' AND weekday = 1 THEN days ELSE 0 END),
            SUM(CASE WHEN role_group = 'CORE' AND weekday = 2 THEN days ELSE 0 END),
            SUM(CASE WHEN role_group = 'CORE' AND weekday = 3 THEN days ELSE 0 END),
            SUM(CASE WHEN role_group = 'CORE' AND weekday = 4 THEN days ELSE 0 END),
            SUM(CASE WHEN role_group = 'CORE' AND weekday = 5 THEN days ELSE 0 END),
            SUM(CASE WHEN role_group = 'CORE' AND weekday = 6 THEN days ELSE 0 END),
            SUM(CASE WHEN role_group = 'CORE' AND weekday = 7 THEN days ELSE 0 END)
        ) AS core_days_by_dow,
        ARRAY(
            SUM(CASE WHEN role_group = 'CORE' AND weekday = 1 THEN market_baskets ELSE 0 END),
            SUM(CASE WHEN role_group = 'CORE' AND weekday = 2 THEN market_baskets ELSE 0 END),
            SUM(CASE WHEN role_group = 'CORE' AND weekday = 3 THEN market_baskets ELSE 0 END),
            SUM(CASE WHEN role_group = 'CORE' AND weekday = 4 THEN market_baskets ELSE 0 END),
            SUM(CASE WHEN role_group = 'CORE' AND weekday = 5 THEN market_baskets ELSE 0 END),
            SUM(CASE WHEN role_group = 'CORE' AND weekday = 6 THEN market_baskets ELSE 0 END),
            SUM(CASE WHEN role_group = 'CORE' AND weekday = 7 THEN market_baskets ELSE 0 END)
        ) AS core_market_baskets_by_dow,
        ARRAY(
            SUM(CASE WHEN role_group = 'BASE' AND weekday = 1 THEN days ELSE 0 END),
            SUM(CASE WHEN role_group = 'BASE' AND weekday = 2 THEN days ELSE 0 END),
            SUM(CASE WHEN role_group = 'BASE' AND weekday = 3 THEN days ELSE 0 END),
            SUM(CASE WHEN role_group = 'BASE' AND weekday = 4 THEN days ELSE 0 END),
            SUM(CASE WHEN role_group = 'BASE' AND weekday = 5 THEN days ELSE 0 END),
            SUM(CASE WHEN role_group = 'BASE' AND weekday = 6 THEN days ELSE 0 END),
            SUM(CASE WHEN role_group = 'BASE' AND weekday = 7 THEN days ELSE 0 END)
        ) AS base_days_by_dow,
        ARRAY(
            SUM(CASE WHEN role_group = 'BASE' AND weekday = 1 THEN market_baskets ELSE 0 END),
            SUM(CASE WHEN role_group = 'BASE' AND weekday = 2 THEN market_baskets ELSE 0 END),
            SUM(CASE WHEN role_group = 'BASE' AND weekday = 3 THEN market_baskets ELSE 0 END),
            SUM(CASE WHEN role_group = 'BASE' AND weekday = 4 THEN market_baskets ELSE 0 END),
            SUM(CASE WHEN role_group = 'BASE' AND weekday = 5 THEN market_baskets ELSE 0 END),
            SUM(CASE WHEN role_group = 'BASE' AND weekday = 6 THEN market_baskets ELSE 0 END),
            SUM(CASE WHEN role_group = 'BASE' AND weekday = 7 THEN market_baskets ELSE 0 END)
        ) AS base_market_baskets_by_dow,
        SUM(CASE WHEN role_group = 'CORE' THEN days ELSE 0 END) AS core_days,
        SUM(CASE WHEN role_group = 'CORE' THEN low_traffic_days ELSE 0 END) AS core_low_traffic_days,
        SUM(CASE WHEN role_group = 'CORE' THEN market_baskets ELSE 0 END) AS core_market_baskets,
        SUM(CASE WHEN role_group = 'BASE' THEN market_baskets ELSE 0 END) AS base_market_baskets,
        AVG(CASE WHEN role_group = 'CORE' THEN avg_active_outlets END) AS core_avg_active_outlets,
        AVG(CASE WHEN role_group = 'BASE' THEN avg_active_outlets END) AS base_avg_active_outlets
    FROM role_days
    GROUP BY occurrence_id
),

expected AS (
    SELECT
        p.*,
        AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
            acc + CASE
                    WHEN ELEMENT_AT(p.base_days_by_dow, wd) > 0
                    THEN ELEMENT_AT(p.core_days_by_dow, wd)
                         * ELEMENT_AT(p.base_market_baskets_by_dow, wd)
                         / ELEMENT_AT(p.base_days_by_dow, wd)
                    ELSE CAST(0 AS DOUBLE)
                  END
        ) AS core_expected_market_baskets
    FROM pivoted p
)

SELECT
    e.occurrence_id,
    e.core_days,
    e.core_low_traffic_days,
    100.0 * e.core_low_traffic_days / NULLIF(e.core_days, 0) AS core_low_traffic_day_pct,
    e.core_market_baskets,
    e.core_expected_market_baskets,
    e.core_market_baskets / NULLIF(e.core_expected_market_baskets, 0) AS core_market_basket_ratio,
    e.core_avg_active_outlets,
    e.base_avg_active_outlets,
    e.core_avg_active_outlets / NULLIF(e.base_avg_active_outlets, 0) AS core_active_outlet_ratio,
    CASE
        WHEN e.core_market_baskets / NULLIF(e.core_expected_market_baskets, 0) < p.abnormal_market_basket_ratio_max
         AND e.core_low_traffic_days / NULLIF(e.core_days, 0) >= p.abnormal_low_traffic_day_share_min
            THEN 'ABNORMAL_LOW_TRAFFIC'
        ELSE 'NORMAL_OR_UNDETERMINED'
    END AS trading_regime_status
FROM expected e
CROSS JOIN lab_customer_value_analytics.seasonality_v2_2_params p;


-- =====================================================================================
-- C  ABSOLUTE DEMAND METRICS
--     One row per entity x occurrence. Unlike the basket-share index, this compares the
--     actual entity count with the entity's normal same-weekday count without scaling the
--     expectation down when total market traffic collapses.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_2_absolute_metrics;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_2_absolute_metrics
USING PARQUET AS
WITH candidate_keys AS (
    SELECT
        m.entity_level,
        m.entity_id,
        m.occurrence_id,
        m.pre_baskets,
        m.event_baskets,
        m.post_baskets,
        m.core_baskets,
        m.baseline_fully_on_sale
    FROM lab_customer_value_analytics.seasonality_v2_occasion_metrics m
    JOIN lab_customer_value_analytics.seasonality_v2_2_market_context c
      ON m.occurrence_id = c.occurrence_id
    WHERE c.trading_regime_status = 'ABNORMAL_LOW_TRAFFIC'
      AND m.core_status = 'SIGNIFICANT_UPLIFT'
),

candidate_entities AS (
    SELECT DISTINCT entity_level, entity_id
    FROM candidate_keys
),

candidate_occurrences AS (
    SELECT DISTINCT occurrence_id
    FROM candidate_keys
),

entity_daily_by_entity AS (
    SELECT /*+ REPARTITION(400, d.entity_id) */
        d.entity_level,
        d.entity_id,
        d.sale_date,
        d.baskets
    FROM lab_customer_value_analytics.seasonality_v2_entity_daily d
    LEFT SEMI JOIN candidate_entities e
      ON d.entity_level = e.entity_level
     AND d.entity_id = e.entity_id
),

base_roles AS (
    SELECT /*+ BROADCAST(o) */
        r.occurrence_id,
        r.weekday,
        r.role_date
    FROM lab_customer_value_analytics.seasonality_v2_date_roles r
    JOIN candidate_occurrences o
      ON r.occurrence_id = o.occurrence_id
    WHERE r.role = 'BASE'
),

base_rows AS (
    SELECT /*+ BROADCAST(r) */
        d.entity_level,
        d.entity_id,
        r.occurrence_id,
        r.weekday,
        d.sale_date,
        d.baskets
    FROM entity_daily_by_entity d
    JOIN base_roles r
      ON d.sale_date = r.role_date
    JOIN candidate_keys k
      ON d.entity_level = k.entity_level
     AND d.entity_id = k.entity_id
     AND r.occurrence_id = k.occurrence_id
),

base_sums AS (
    SELECT
        entity_level,
        entity_id,
        occurrence_id,
        ARRAY(
            SUM(CASE WHEN weekday = 1 THEN baskets ELSE 0 END),
            SUM(CASE WHEN weekday = 2 THEN baskets ELSE 0 END),
            SUM(CASE WHEN weekday = 3 THEN baskets ELSE 0 END),
            SUM(CASE WHEN weekday = 4 THEN baskets ELSE 0 END),
            SUM(CASE WHEN weekday = 5 THEN baskets ELSE 0 END),
            SUM(CASE WHEN weekday = 6 THEN baskets ELSE 0 END),
            SUM(CASE WHEN weekday = 7 THEN baskets ELSE 0 END)
        ) AS basket_sum_by_dow,
        ARRAY(
            SUM(CASE WHEN weekday = 1 THEN CAST(baskets AS DOUBLE) * baskets ELSE 0.0 END),
            SUM(CASE WHEN weekday = 2 THEN CAST(baskets AS DOUBLE) * baskets ELSE 0.0 END),
            SUM(CASE WHEN weekday = 3 THEN CAST(baskets AS DOUBLE) * baskets ELSE 0.0 END),
            SUM(CASE WHEN weekday = 4 THEN CAST(baskets AS DOUBLE) * baskets ELSE 0.0 END),
            SUM(CASE WHEN weekday = 5 THEN CAST(baskets AS DOUBLE) * baskets ELSE 0.0 END),
            SUM(CASE WHEN weekday = 6 THEN CAST(baskets AS DOUBLE) * baskets ELSE 0.0 END),
            SUM(CASE WHEN weekday = 7 THEN CAST(baskets AS DOUBLE) * baskets ELSE 0.0 END)
        ) AS basket_sq_sum_by_dow
    FROM base_rows
    GROUP BY entity_level, entity_id, occurrence_id
),

spells AS (
    SELECT
        entity_level,
        entity_id,
        COLLECT_LIST(NAMED_STRUCT('s', spell_start, 'e', spell_end)) AS spells
    FROM lab_customer_value_analytics.seasonality_v2_availability_spells s
    LEFT SEMI JOIN candidate_entities e
      ON s.entity_level = e.entity_level
     AND s.entity_id = e.entity_id
    GROUP BY s.entity_level, s.entity_id
),

keys AS (
    SELECT /*+ BROADCAST(w, s) */
        m.entity_level,
        m.entity_id,
        m.occurrence_id,
        m.pre_baskets,
        m.event_baskets,
        m.post_baskets,
        m.core_baskets,
        m.baseline_fully_on_sale,
        w.pre_days_by_dow,
        w.event_days_by_dow,
        w.post_days_by_dow,
        w.core_days_by_dow,
        w.base_days_by_dow,
        w.base_days,
        s.spells,
        COALESCE(b.basket_sum_by_dow, ARRAY(0L, 0L, 0L, 0L, 0L, 0L, 0L)) AS basket_sum_by_dow,
        COALESCE(b.basket_sq_sum_by_dow, ARRAY(CAST(0 AS DOUBLE), CAST(0 AS DOUBLE), CAST(0 AS DOUBLE), CAST(0 AS DOUBLE), CAST(0 AS DOUBLE), CAST(0 AS DOUBLE), CAST(0 AS DOUBLE))) AS basket_sq_sum_by_dow
    FROM candidate_keys m
    JOIN lab_customer_value_analytics.seasonality_v2_occasion_windows w
      ON m.occurrence_id = w.occurrence_id
    LEFT JOIN base_sums b
      ON m.entity_level = b.entity_level
     AND m.entity_id = b.entity_id
     AND m.occurrence_id = b.occurrence_id
    LEFT JOIN spells s
      ON m.entity_level = s.entity_level
     AND m.entity_id = s.entity_id
),

available AS (
    SELECT
        k.*,
        CASE
            WHEN k.baseline_fully_on_sale THEN k.base_days_by_dow
            ELSE TRANSFORM(SEQUENCE(1, 7), wd ->
                CAST(SIZE(FILTER(k.base_days, x -> x.dow = wd
                    AND COALESCE(EXISTS(k.spells, sp -> x.d BETWEEN sp.s AND sp.e), FALSE))) AS BIGINT))
        END AS available_days_by_dow
    FROM keys k
),

model1 AS (
    SELECT
        a.*,
        AGGREGATE(a.available_days_by_dow, CAST(0 AS BIGINT), (acc, x) -> acc + x) AS baseline_days_available_absolute,
        SIZE(FILTER(a.available_days_by_dow, x -> x > 0)) AS baseline_weekdays_available_absolute,
        AGGREGATE(a.basket_sum_by_dow, CAST(0 AS DOUBLE), (acc, x) -> acc + x) AS base_entity_baskets
    FROM available a
),

model2 AS (
    SELECT
        a.*,
        a.base_entity_baskets / NULLIF(a.baseline_days_available_absolute, 0) AS pooled_mean_daily_baskets,
        AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
            acc + CASE
                    WHEN ELEMENT_AT(a.available_days_by_dow, wd) > 0
                    THEN ELEMENT_AT(a.basket_sq_sum_by_dow, wd)
                         - POW(ELEMENT_AT(a.basket_sum_by_dow, wd), 2)
                           / ELEMENT_AT(a.available_days_by_dow, wd)
                    ELSE CAST(0 AS DOUBLE)
                  END
        ) AS basket_ss_within
    FROM model1 a
),

model3 AS (
    SELECT
        a.*,
        TRANSFORM(SEQUENCE(1, 7), wd ->
            CASE
                WHEN ELEMENT_AT(a.available_days_by_dow, wd) > 0
                THEN ELEMENT_AT(a.basket_sum_by_dow, wd) / ELEMENT_AT(a.available_days_by_dow, wd)
                ELSE a.pooled_mean_daily_baskets
            END
        ) AS mean_daily_baskets_by_dow,
        TRANSFORM(SEQUENCE(1, 7), wd ->
            CAST(CASE
                WHEN ELEMENT_AT(a.available_days_by_dow, wd) > 0
                THEN ELEMENT_AT(a.available_days_by_dow, wd)
                ELSE a.baseline_days_available_absolute
            END AS DOUBLE)
        ) AS mean_days_by_dow,
        CASE
            WHEN a.baseline_days_available_absolute - a.baseline_weekdays_available_absolute > 0
            THEN GREATEST(a.basket_ss_within, CAST(0 AS DOUBLE))
                 / (a.baseline_days_available_absolute - a.baseline_weekdays_available_absolute)
        END AS daily_basket_variance
    FROM model2 a
),

expectations AS (
    SELECT
        a.*,
        AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
            acc + ELEMENT_AT(a.pre_days_by_dow, wd) * ELEMENT_AT(a.mean_daily_baskets_by_dow, wd))
            AS pre_expected_baskets_absolute,
        AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
            acc + ELEMENT_AT(a.event_days_by_dow, wd) * ELEMENT_AT(a.mean_daily_baskets_by_dow, wd))
            AS event_expected_baskets_absolute,
        AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
            acc + ELEMENT_AT(a.post_days_by_dow, wd) * ELEMENT_AT(a.mean_daily_baskets_by_dow, wd))
            AS post_expected_baskets_absolute,
        AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
            acc + ELEMENT_AT(a.core_days_by_dow, wd) * ELEMENT_AT(a.mean_daily_baskets_by_dow, wd))
            AS core_expected_baskets_absolute,
        SQRT(COALESCE(a.daily_basket_variance, CAST(0 AS DOUBLE)) * (
            AGGREGATE(a.core_days_by_dow, CAST(0 AS DOUBLE), (acc, x) -> acc + x)
            + AGGREGATE(SEQUENCE(1, 7), CAST(0 AS DOUBLE), (acc, wd) ->
                acc + POW(ELEMENT_AT(a.core_days_by_dow, wd), 2) / NULLIF(ELEMENT_AT(a.mean_days_by_dow, wd), 0))
        )) AS core_absolute_model_sd
    FROM model3 a
)

SELECT
    entity_level,
    entity_id,
    occurrence_id,
    baseline_days_available_absolute,
    pooled_mean_daily_baskets,
    pre_expected_baskets_absolute,
    event_expected_baskets_absolute,
    post_expected_baskets_absolute,
    core_expected_baskets_absolute,
    pre_baskets / NULLIF(pre_expected_baskets_absolute, 0) AS pre_absolute_basket_index,
    event_baskets / NULLIF(event_expected_baskets_absolute, 0) AS event_absolute_basket_index,
    post_baskets / NULLIF(post_expected_baskets_absolute, 0) AS post_absolute_basket_index,
    core_baskets / NULLIF(core_expected_baskets_absolute, 0) AS core_absolute_basket_index,
    (core_baskets - core_expected_baskets_absolute)
        / NULLIF(GREATEST(core_absolute_model_sd,
                          SQRT(GREATEST(core_expected_baskets_absolute, CAST(0 AS DOUBLE)))), 0)
        AS core_absolute_z
FROM expectations;


-- =====================================================================================
-- D  REVISED OCCASION METRICS
--     Keeps every v2.1 field and appends the v2.2 diagnostics and revised verdict.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_2_occasion_metrics;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_2_occasion_metrics
USING PARQUET AS
WITH joined AS (
    SELECT /*+ BROADCAST(c, w, o, p2, p1) */
        m.*,
        c.core_days,
        c.core_low_traffic_days,
        c.core_low_traffic_day_pct,
        c.core_market_baskets,
        c.core_expected_market_baskets,
        c.core_market_basket_ratio,
        c.core_avg_active_outlets,
        c.base_avg_active_outlets,
        c.core_active_outlet_ratio,
        c.trading_regime_status,
        a.baseline_days_available_absolute,
        a.pooled_mean_daily_baskets,
        a.pre_expected_baskets_absolute,
        a.event_expected_baskets_absolute,
        a.post_expected_baskets_absolute,
        a.core_expected_baskets_absolute,
        a.pre_absolute_basket_index,
        a.event_absolute_basket_index,
        a.post_absolute_basket_index,
        a.core_absolute_basket_index,
        a.core_absolute_z,
        w.occurrence_seq,
        w.overlapping_core,
        o.recurrence_handling,
        CASE
            WHEN o.recurrence_handling = 'PERIODIC_OR_ONE_OFF' THEN 'PERIODIC_OR_ONE_OFF'
            WHEN o.recurrence_handling = 'FIXED_ANNUAL_DATE' THEN 'ANNUAL_FIXED'
            WHEN o.recurrence_handling = 'RULE_BASED_ANNUAL_DATE' THEN 'ANNUAL_RULE_BASED'
            WHEN o.recurrence_handling = 'APPROXIMATE_BUSINESS_WINDOW' THEN 'ANNUAL_BUSINESS_WINDOW'
            WHEN o.recurrence_handling = 'EXPLICIT_YEARLY_DATE_REQUIRED' THEN 'ANNUAL_MOVING_OR_EXPLICIT'
            ELSE 'UNKNOWN'
        END AS recurrence_class,
        p1.min_baseline_days,
        p1.min_expected_baskets,
        p1.uplift_index_min,
        p1.z_min,
        p1.specific_ratio_min,
        p1.min_specific_baskets,
        p1.promo_price_change_max_pct,
        p1.min_years,
        p1.min_year_share,
        p1.nested_occasion_min_ratio,
        p2.abnormal_absolute_index_min,
        p2.abnormal_absolute_z_min
    FROM lab_customer_value_analytics.seasonality_v2_occasion_metrics m
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_2_market_context c
      ON m.occurrence_id = c.occurrence_id
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_2_absolute_metrics a
      ON m.entity_level = a.entity_level
     AND m.entity_id = a.entity_id
     AND m.occurrence_id = a.occurrence_id
    JOIN lab_customer_value_analytics.seasonality_v2_occasion_windows w
      ON m.occurrence_id = w.occurrence_id
    JOIN lab_customer_value_analytics.seasonality_v2_occurrences o
      ON m.occurrence_id = o.occurrence_id
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_params p1
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_2_params p2
),

year_tests AS (
    SELECT
        j.*,
        COALESCE(j.eligibility_status = 'ELIGIBLE'
                 AND j.year_is_complete = 'Y'
                 AND j.core_baskets >= j.min_specific_baskets
                 AND (j.annual_baskets - j.core_baskets = 0 OR j.specific_share_ratio >= j.specific_ratio_min), FALSE)
            AS passes_specific_test_v2_2,
        CASE
            WHEN j.baseline_days_available < j.min_baseline_days THEN 'INSUFFICIENT_BASELINE'
            WHEN j.core_expected_baskets IS NULL OR j.core_expected_baskets < j.min_expected_baskets THEN 'LOW_VOLUME'
            WHEN j.core_share_index >= j.uplift_index_min AND j.core_z >= j.z_min
             AND j.trading_regime_status = 'ABNORMAL_LOW_TRAFFIC'
             AND (j.core_absolute_basket_index IS NULL
                  OR j.core_absolute_basket_index < j.abnormal_absolute_index_min
                  OR j.core_absolute_z < j.abnormal_absolute_z_min)
                THEN 'MISSION_MIX_UPLIFT'
            WHEN j.core_share_index >= j.uplift_index_min AND j.core_z >= j.z_min
                THEN 'SIGNIFICANT_UPLIFT'
            WHEN j.core_share_index <= 1.0 / j.uplift_index_min AND j.core_z <= -j.z_min
                THEN 'SIGNIFICANT_DECLINE'
            ELSE 'NOT_SIGNIFICANT'
        END AS core_status_v2_2
    FROM joined j
),

positive_candidates AS (
    SELECT
        t.*,
        MAP_FROM_ENTRIES(COLLECT_LIST(
            CASE
                WHEN SIZE(t.overlapping_core) > 0
                 AND (t.passes_specific_test_v2_2 OR t.core_status_v2_2 = 'SIGNIFICANT_UPLIFT')
                THEN STRUCT(t.occurrence_seq, COALESCE(t.core_item_share, CAST(0 AS DOUBLE)))
            END
        ) OVER (PARTITION BY t.entity_level, t.entity_id, t.event_year)) AS same_year_core_shares_v2_2
    FROM year_tests t
),

attribution AS (
    SELECT
        c.*,
        FILTER(c.overlapping_core, oc ->
            ARRAY_CONTAINS(MAP_KEYS(c.same_year_core_shares_v2_2), oc.seq)
            AND NOT (c.window_type = 'BUILDUP' AND oc.other_inside_pct >= 0.5)
            AND CASE
                    WHEN oc.other_is_buildup AND oc.inside_pct >= 0.5
                        THEN COALESCE(c.core_item_share, CAST(0 AS DOUBLE))
                             < c.nested_occasion_min_ratio * c.same_year_core_shares_v2_2[oc.seq]
                    ELSE (oc.inside_pct >= 0.5
                          AND c.same_year_core_shares_v2_2[oc.seq] >= 0.9 * COALESCE(c.core_item_share, CAST(0 AS DOUBLE)))
                         OR c.same_year_core_shares_v2_2[oc.seq] > 1.1 * COALESCE(c.core_item_share, CAST(0 AS DOUBLE))
                END
        ) AS stronger_overlaps_v2_2
    FROM positive_candidates c
),

year_flags AS (
    SELECT
        a.*,
        (a.passes_specific_test_v2_2 OR a.core_status_v2_2 = 'SIGNIFICANT_UPLIFT')
            AND SIZE(a.stronger_overlaps_v2_2) > 0 AS is_attributed_elsewhere_v2_2,
        CASE
            WHEN (a.passes_specific_test_v2_2 OR a.core_status_v2_2 = 'SIGNIFICANT_UPLIFT')
             AND SIZE(a.stronger_overlaps_v2_2) > 0
            THEN CONCAT_WS(', ', SORT_ARRAY(TRANSFORM(a.stronger_overlaps_v2_2, x -> x.name)))
        END AS attributed_to_v2_2,
        a.eligibility_status = 'ELIGIBLE'
            AND NOT ((a.passes_specific_test_v2_2 OR a.core_status_v2_2 = 'SIGNIFICANT_UPLIFT')
                     AND SIZE(a.stronger_overlaps_v2_2) > 0)
            AS is_eligible_year_v2_2
    FROM attribution a
),

year_flags2 AS (
    SELECT
        y.*,
        y.is_eligible_year_v2_2 AND y.passes_specific_test_v2_2 AS is_specific_year_v2_2,
        y.is_eligible_year_v2_2
            AND y.core_status_v2_2 IN ('SIGNIFICANT_UPLIFT', 'SIGNIFICANT_DECLINE',
                                       'MISSION_MIX_UPLIFT', 'NOT_SIGNIFICANT') AS is_valid_year_v2_2,
        y.is_eligible_year_v2_2 AND y.core_status_v2_2 = 'SIGNIFICANT_UPLIFT' AS is_uplift_year_v2_2,
        y.is_eligible_year_v2_2 AND y.core_status_v2_2 = 'MISSION_MIX_UPLIFT' AS is_mission_mix_year_v2_2,
        y.is_eligible_year_v2_2 AND y.core_status_v2_2 = 'SIGNIFICANT_DECLINE' AS is_decline_year_v2_2,
        COALESCE(y.is_eligible_year_v2_2 AND y.core_status_v2_2 = 'SIGNIFICANT_UPLIFT'
                 AND y.core_price_change_pct <= y.promo_price_change_max_pct, FALSE)
            AS is_price_drop_uplift_year_v2_2
    FROM year_flags y
),

across_years AS (
    SELECT
        f.*,
        SUM(CASE WHEN f.is_eligible_year_v2_2 THEN 1 ELSE 0 END) OVER w AS years_eligible_v2_2,
        SUM(CASE WHEN f.is_attributed_elsewhere_v2_2 THEN 1 ELSE 0 END) OVER w AS years_attributed_elsewhere_v2_2,
        SUM(CASE WHEN f.is_specific_year_v2_2 THEN 1 ELSE 0 END) OVER w AS years_specific_v2_2,
        SUM(CASE WHEN f.is_valid_year_v2_2 THEN 1 ELSE 0 END) OVER w AS years_valid_v2_2,
        SUM(CASE WHEN f.is_uplift_year_v2_2 THEN 1 ELSE 0 END) OVER w AS years_uplift_v2_2,
        SUM(CASE WHEN f.is_mission_mix_year_v2_2 THEN 1 ELSE 0 END) OVER w AS years_mission_mix_v2_2,
        SUM(CASE WHEN f.is_decline_year_v2_2 THEN 1 ELSE 0 END) OVER w AS years_decline_v2_2,
        SUM(CASE WHEN f.is_price_drop_uplift_year_v2_2 THEN 1 ELSE 0 END) OVER w AS years_price_drop_uplift_v2_2,
        PERCENTILE_APPROX(CASE WHEN f.is_valid_year_v2_2 THEN f.core_share_index END, 0.5) OVER w
            AS median_core_share_index_v2_2,
        PERCENTILE_APPROX(CASE WHEN f.is_valid_year_v2_2 THEN f.core_absolute_basket_index END, 0.5) OVER w
            AS median_core_absolute_basket_index_v2_2,
        SUM(CASE WHEN f.is_valid_year_v2_2 THEN f.core_baskets END) OVER w AS valid_core_baskets_v2_2,
        SUM(CASE WHEN f.is_valid_year_v2_2 THEN f.core_expected_baskets END) OVER w AS valid_core_expected_baskets_v2_2
    FROM year_flags2 f
    WINDOW w AS (PARTITION BY f.entity_level, f.entity_id, f.occasion_key)
),

verdict AS (
    SELECT
        a.*,
        a.valid_core_baskets_v2_2 / NULLIF(a.valid_core_expected_baskets_v2_2, 0)
            AS pooled_core_share_index_v2_2,
        CASE
            WHEN a.recurrence_class = 'PERIODIC_OR_ONE_OFF'
                THEN 'Insufficient evidence'
            WHEN a.years_eligible_v2_2 < a.min_years
                THEN 'Insufficient evidence'
            WHEN a.years_specific_v2_2 > 0
             AND a.years_specific_v2_2 >= a.min_year_share * a.years_eligible_v2_2
                THEN 'Specifically seasonal'
            WHEN a.years_valid_v2_2 < a.min_years
                THEN 'Insufficient evidence'
            WHEN a.years_uplift_v2_2 >= a.min_year_share * a.years_valid_v2_2
             AND a.median_core_share_index_v2_2 >= a.uplift_index_min
                THEN 'Uplift seasonal'
            WHEN a.years_mission_mix_v2_2 >= a.min_year_share * a.years_valid_v2_2
             AND a.median_core_share_index_v2_2 >= a.uplift_index_min
                THEN 'Occasion mission shift'
            WHEN a.years_decline_v2_2 >= a.min_year_share * a.years_valid_v2_2
             AND a.median_core_share_index_v2_2 <= 1.0 / a.uplift_index_min
             AND a.event_length_days <= 14
                THEN 'Occasion-suppressed'
            ELSE 'Regular'
        END AS seasonality_flag_v2_2
    FROM across_years a
)

SELECT
    v.*,
    CASE
        WHEN v.recurrence_class = 'PERIODIC_OR_ONE_OFF'
         AND v.eligibility_status = 'ELIGIBLE'
         AND (v.passes_specific_test_v2_2 OR v.core_status_v2_2 = 'SIGNIFICANT_UPLIFT')
            THEN 'OBSERVED_OCCURRENCE_RESPONSE'
    END AS periodic_occurrence_response,
    CASE
        WHEN v.seasonality_flag_v2_2 = 'Uplift seasonal'
         AND v.years_uplift_v2_2 > 0
         AND 2 * v.years_price_drop_uplift_v2_2 >= v.years_uplift_v2_2
            THEN 'POSSIBLY_PRICE_DROP_DRIVEN'
    END AS price_drop_flag_v2_2,
    CASE
        WHEN v.seasonality_flag_v2_2 = 'Occasion mission shift'
            THEN 'SUFFICIENT: share gain occurs in an abnormal low-traffic trading regime without absolute demand growth'
        WHEN v.seasonality_flag_v2_2 = 'Specifically seasonal'
            THEN 'SUFFICIENT: annual concentration is consistent across complete years'
        WHEN v.seasonality_flag_v2_2 = 'Uplift seasonal'
            THEN 'SUFFICIENT: recurring local demand uplift'
        WHEN v.recurrence_class = 'PERIODIC_OR_ONE_OFF'
            THEN 'OCCURRENCE-SPECIFIC: do not interpret as recurring seasonality'
        WHEN v.years_eligible_v2_2 < v.min_years
            THEN 'LIMITED: fewer than 2 eligible occurrences'
        WHEN v.years_valid_v2_2 < v.min_years
            THEN 'LIMITED: fewer than 2 valid occurrences'
        ELSE 'SUFFICIENT'
    END AS evidence_status_v2_2,
    CONCAT(
        'v2.1=', COALESCE(v.seasonality_flag, 'n/a'), '; v2.2=',
        COALESCE(v.seasonality_flag_v2_2, 'n/a'), '; specific ',
        CAST(v.years_specific_v2_2 AS STRING), '/', CAST(v.years_eligible_v2_2 AS STRING),
        '; uplift ', CAST(v.years_uplift_v2_2 AS STRING), '/', CAST(v.years_valid_v2_2 AS STRING),
        '; mission shift ', CAST(v.years_mission_mix_v2_2 AS STRING), '/', CAST(v.years_valid_v2_2 AS STRING),
        '; median share index ', COALESCE(CAST(ROUND(v.median_core_share_index_v2_2, 2) AS STRING), 'n/a'),
        '; median absolute index ', COALESCE(CAST(ROUND(v.median_core_absolute_basket_index_v2_2, 2) AS STRING), 'n/a')
    ) AS classification_reason_v2_2
FROM verdict v;


-- =====================================================================================
-- E  PRODUCT x PARENT OCCASION EVIDENCE
--     One row per TPNB x parent occasion. It combines event and build-up windows, adds
--     same-parent subclass support and separates annual seasonality from periodic events.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_2_product_occasion_evidence;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_2_product_occasion_evidence
USING PARQUET AS
WITH occasion_info AS (
    SELECT
        parent_occasion_key,
        MAX(occasion_name) AS parent_occasion_name,
        MAX(occasion_family) AS occasion_family,
        MAX(recurrence_handling) AS recurrence_handling,
        MAX(CASE
            WHEN recurrence_handling = 'PERIODIC_OR_ONE_OFF' THEN 'PERIODIC_OR_ONE_OFF'
            WHEN recurrence_handling = 'FIXED_ANNUAL_DATE' THEN 'ANNUAL_FIXED'
            WHEN recurrence_handling = 'RULE_BASED_ANNUAL_DATE' THEN 'ANNUAL_RULE_BASED'
            WHEN recurrence_handling = 'APPROXIMATE_BUSINESS_WINDOW' THEN 'ANNUAL_BUSINESS_WINDOW'
            WHEN recurrence_handling = 'EXPLICIT_YEARLY_DATE_REQUIRED' THEN 'ANNUAL_MOVING_OR_EXPLICIT'
            ELSE 'UNKNOWN' END) AS recurrence_class
    FROM lab_customer_value_analytics.seasonality_v2_occurrences
    WHERE window_type = 'EVENT'
    GROUP BY parent_occasion_key
),

tpnb_year AS (
    SELECT
        entity_id AS tpnb,
        parent_occasion_key,
        event_year,
        MAX(CASE WHEN is_eligible_year_v2_2 THEN 1 ELSE 0 END) AS eligible_year,
        MAX(CASE WHEN is_valid_year_v2_2 THEN 1 ELSE 0 END) AS valid_year,
        MAX(CASE WHEN is_specific_year_v2_2 THEN 1 ELSE 0 END) AS specific_year,
        MAX(CASE WHEN is_uplift_year_v2_2 THEN 1 ELSE 0 END) AS uplift_year,
        MAX(CASE WHEN is_mission_mix_year_v2_2 THEN 1 ELSE 0 END) AS mission_mix_year,
        MAX(CASE WHEN is_decline_year_v2_2 THEN 1 ELSE 0 END) AS decline_year,
        MAX(CASE WHEN periodic_occurrence_response IS NOT NULL THEN 1 ELSE 0 END) AS periodic_response_year,
        MAX(core_share_index) AS max_core_share_index,
        MAX(core_absolute_basket_index) AS max_core_absolute_basket_index,
        MAX_BY(window_type, COALESCE(core_share_index, CAST(-1 AS DOUBLE))) AS strongest_window_type
    FROM lab_customer_value_analytics.seasonality_v2_2_occasion_metrics
    WHERE entity_level = 'TPNB'
    GROUP BY entity_id, parent_occasion_key, event_year
),

tpnb_parent_counts AS (
    SELECT
        tpnb,
        parent_occasion_key,
        SUM(eligible_year) AS parent_years_eligible,
        SUM(valid_year) AS parent_years_valid,
        SUM(specific_year) AS parent_years_specific,
        SUM(uplift_year) AS parent_years_uplift,
        SUM(mission_mix_year) AS parent_years_mission_mix,
        SUM(decline_year) AS parent_years_decline,
        SUM(periodic_response_year) AS parent_periodic_response_years,
        MAX(max_core_share_index) AS parent_max_core_share_index,
        MAX(max_core_absolute_basket_index) AS parent_max_core_absolute_basket_index,
        MAX_BY(strongest_window_type, COALESCE(max_core_share_index, CAST(-1 AS DOUBLE))) AS strongest_signal_window_type
    FROM tpnb_year
    GROUP BY tpnb, parent_occasion_key
),

window_verdicts AS (
    SELECT
        entity_level,
        entity_id,
        parent_occasion_key,
        window_type,
        MAX(seasonality_flag_v2_2) AS window_verdict,
        MAX(median_core_share_index_v2_2) AS window_median_index,
        MAX(median_core_absolute_basket_index_v2_2) AS window_median_absolute_index
    FROM lab_customer_value_analytics.seasonality_v2_2_occasion_metrics
    GROUP BY entity_level, entity_id, parent_occasion_key, window_type
),

parent_verdicts AS (
    SELECT
        entity_level,
        entity_id,
        parent_occasion_key,
        MAX(CASE WHEN window_type = 'EVENT' THEN window_verdict END) AS event_verdict,
        MAX(CASE WHEN window_type = 'BUILDUP' THEN window_verdict END) AS buildup_verdict,
        MAX(CASE WHEN window_type = 'EVENT' THEN window_median_index END) AS event_median_index,
        MAX(CASE WHEN window_type = 'BUILDUP' THEN window_median_index END) AS buildup_median_index,
        MAX(CASE WHEN window_type = 'EVENT' THEN window_median_absolute_index END) AS event_median_absolute_index,
        MAX(CASE WHEN window_type = 'BUILDUP' THEN window_median_absolute_index END) AS buildup_median_absolute_index
    FROM window_verdicts
    GROUP BY entity_level, entity_id, parent_occasion_key
),

ranked_parent AS (
    SELECT
        p.*,
        CASE p.event_verdict
            WHEN 'Specifically seasonal' THEN 1
            WHEN 'Uplift seasonal' THEN 2
            WHEN 'Occasion-suppressed' THEN 3
            WHEN 'Occasion mission shift' THEN 4
            WHEN 'Regular' THEN 5
            WHEN 'Insufficient evidence' THEN 6
            ELSE 7 END AS event_rank,
        CASE p.buildup_verdict
            WHEN 'Specifically seasonal' THEN 1
            WHEN 'Uplift seasonal' THEN 2
            WHEN 'Occasion-suppressed' THEN 3
            WHEN 'Occasion mission shift' THEN 4
            WHEN 'Regular' THEN 5
            WHEN 'Insufficient evidence' THEN 6
            ELSE 7 END AS buildup_rank
    FROM parent_verdicts p
),

parent_resolved AS (
    SELECT
        p.*,
        CASE WHEN p.buildup_rank < p.event_rank THEN p.buildup_verdict ELSE p.event_verdict END AS confirmed_parent_verdict,
        CASE
            WHEN p.buildup_rank <= 2 AND p.event_rank <= 2 THEN 'BUILDUP_AND_EVENT'
            WHEN p.buildup_rank <= 2 THEN 'BUILDUP_ONLY'
            WHEN p.event_rank <= 2 AND p.buildup_verdict IS NOT NULL THEN 'EVENT_ONLY'
            WHEN p.event_rank <= 2 THEN 'EVENT'
            WHEN LEAST(p.event_rank, p.buildup_rank) = 4 THEN 'MISSION_SHIFT'
        END AS confirmed_timing_type
    FROM ranked_parent p
),

subclass_parent AS (
    SELECT
        entity_id AS subclass_entity_id,
        parent_occasion_key,
        confirmed_parent_verdict AS subclass_parent_verdict
    FROM parent_resolved
    WHERE entity_level = 'SUBCLASS'
),

tpnb_parent AS (
    SELECT
        p.entity_id AS tpnb,
        p.parent_occasion_key,
        p.confirmed_parent_verdict,
        p.confirmed_timing_type,
        p.event_verdict,
        p.buildup_verdict,
        p.event_median_index,
        p.buildup_median_index,
        p.event_median_absolute_index,
        p.buildup_median_absolute_index
    FROM parent_resolved p
    WHERE p.entity_level = 'TPNB'
),

joined AS (
    SELECT
        p.tpnb,
        p.parent_occasion_key,
        o.parent_occasion_name,
        o.occasion_family,
        o.recurrence_class,
        p.confirmed_parent_verdict,
        p.confirmed_timing_type,
        p.event_verdict,
        p.buildup_verdict,
        p.event_median_index,
        p.buildup_median_index,
        p.event_median_absolute_index,
        p.buildup_median_absolute_index,
        COALESCE(c.parent_years_eligible, 0) AS parent_years_eligible,
        COALESCE(c.parent_years_valid, 0) AS parent_years_valid,
        COALESCE(c.parent_years_specific, 0) AS parent_years_specific,
        COALESCE(c.parent_years_uplift, 0) AS parent_years_uplift,
        COALESCE(c.parent_years_mission_mix, 0) AS parent_years_mission_mix,
        COALESCE(c.parent_years_decline, 0) AS parent_years_decline,
        COALESCE(c.parent_periodic_response_years, 0) AS parent_periodic_response_years,
        c.parent_max_core_share_index,
        c.parent_max_core_absolute_basket_index,
        c.strongest_signal_window_type,
        d.hierarchy_division_name,
        d.hierarchy_subclass_code,
        d.hierarchy_subclass_name,
        d.subclass_entity_id,
        e.availability_pattern,
        e.total_baskets,
        e.total_sales,
        s.subclass_parent_verdict
    FROM tpnb_parent p
    LEFT JOIN tpnb_parent_counts c
      ON p.tpnb = c.tpnb
     AND p.parent_occasion_key = c.parent_occasion_key
    LEFT JOIN occasion_info o
      ON p.parent_occasion_key = o.parent_occasion_key
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_product_dim d
      ON TRY_CAST(p.tpnb AS BIGINT) = d.tpnb
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_entity_profile e
      ON e.entity_level = 'TPNB'
     AND e.entity_id = p.tpnb
    LEFT JOIN subclass_parent s
      ON d.subclass_entity_id = s.subclass_entity_id
     AND p.parent_occasion_key = s.parent_occasion_key
),

resolved AS (
    SELECT
        j.*,
        CASE
            WHEN j.recurrence_class = 'PERIODIC_OR_ONE_OFF'
             AND j.parent_periodic_response_years > 0
                THEN 'Observed occurrence response'
            WHEN j.confirmed_parent_verdict IN ('Specifically seasonal', 'Uplift seasonal',
                                                'Occasion-suppressed', 'Occasion mission shift')
                THEN j.confirmed_parent_verdict
            WHEN j.confirmed_parent_verdict = 'Insufficient evidence'
             AND j.parent_years_decline = 0
             AND j.parent_years_specific BETWEEN p.supported_single_min_positive_years
                                             AND p.supported_single_max_positive_years
             AND j.subclass_parent_verdict = 'Specifically seasonal'
             AND j.availability_pattern <> 'Year-round'
                THEN 'Supported specifically seasonal'
            WHEN j.confirmed_parent_verdict = 'Insufficient evidence'
             AND j.parent_years_decline = 0
             AND j.parent_years_uplift BETWEEN p.supported_single_min_positive_years
                                          AND p.supported_single_max_positive_years
             AND j.subclass_parent_verdict IN ('Specifically seasonal', 'Uplift seasonal')
             AND j.availability_pattern <> 'Year-round'
             AND (COALESCE(j.hierarchy_division_name, '') <> 'HOME & CLOTHING'
                  OR j.subclass_parent_verdict = 'Specifically seasonal')
                THEN 'Supported uplift seasonal'
            ELSE j.confirmed_parent_verdict
        END AS resolved_occasion_verdict
    FROM joined j
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_2_params p
)

SELECT
    r.*,
    CASE
        WHEN r.confirmed_parent_verdict IN ('Specifically seasonal', 'Uplift seasonal')
         AND r.subclass_parent_verdict IN ('Specifically seasonal', 'Uplift seasonal')
            THEN 'HIGH'
        WHEN r.confirmed_parent_verdict IN ('Specifically seasonal', 'Uplift seasonal')
            THEN 'MEDIUM'
        WHEN r.resolved_occasion_verdict IN ('Supported specifically seasonal', 'Supported uplift seasonal')
            THEN 'SUPPORTED'
        WHEN r.resolved_occasion_verdict = 'Observed occurrence response'
            THEN 'OCCURRENCE_ONLY'
        WHEN r.resolved_occasion_verdict = 'Occasion mission shift'
            THEN 'DIAGNOSTIC'
        ELSE 'NONE'
    END AS occasion_confidence,
    CASE
        WHEN r.confirmed_parent_verdict IN ('Specifically seasonal', 'Uplift seasonal')
            THEN 'TPNB_MULTI_OCCURRENCE'
        WHEN r.resolved_occasion_verdict IN ('Supported specifically seasonal', 'Supported uplift seasonal')
            THEN 'TPNB_SINGLE_OCCURRENCE_PLUS_SAME_PARENT_SUBCLASS'
        WHEN r.resolved_occasion_verdict = 'Observed occurrence response'
            THEN 'TPNB_SINGLE_PERIODIC_OCCURRENCE'
        WHEN r.resolved_occasion_verdict = 'Occasion mission shift'
            THEN 'ABNORMAL_TRADING_MISSION_SHIFT'
        ELSE 'NONE'
    END AS evidence_source_v2_2,
    CASE WHEN r.subclass_parent_verdict IN ('Specifically seasonal', 'Uplift seasonal') THEN 'Y' ELSE 'N' END
        AS same_parent_subclass_support_flag
FROM resolved r;


-- =====================================================================================
-- F  V2.2 TPNB SUMMARY
--     Keeps the v2.1 summary columns and appends candidate resolved evidence fields.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_2_tpnb_summary;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_2_tpnb_summary
USING PARQUET AS
WITH evidence AS (
    SELECT
        tpnb,
        SUM(CASE WHEN confirmed_parent_verdict = 'Specifically seasonal' THEN 1 ELSE 0 END) AS confirmed_specific_occasions,
        SUM(CASE WHEN confirmed_parent_verdict = 'Uplift seasonal' THEN 1 ELSE 0 END) AS confirmed_uplift_occasions,
        SUM(CASE WHEN resolved_occasion_verdict = 'Supported specifically seasonal' THEN 1 ELSE 0 END) AS supported_specific_occasions,
        SUM(CASE WHEN resolved_occasion_verdict = 'Supported uplift seasonal' THEN 1 ELSE 0 END) AS supported_uplift_occasions,
        SUM(CASE WHEN resolved_occasion_verdict = 'Occasion mission shift' THEN 1 ELSE 0 END) AS mission_shift_occasions,
        SUM(CASE WHEN resolved_occasion_verdict = 'Observed occurrence response' THEN 1 ELSE 0 END) AS periodic_response_occasions,
        SUM(CASE WHEN resolved_occasion_verdict = 'Occasion-suppressed' THEN 1 ELSE 0 END) AS suppressed_occasions_v2_2,
        SUM(CASE WHEN resolved_occasion_verdict = 'Regular' THEN 1 ELSE 0 END) AS regular_occasions_v2_2,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(CASE
            WHEN resolved_occasion_verdict = 'Supported specifically seasonal' THEN parent_occasion_name END)))
            AS supported_specific_occasion_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(CASE
            WHEN resolved_occasion_verdict = 'Supported uplift seasonal' THEN parent_occasion_name END)))
            AS supported_uplift_occasion_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(CASE
            WHEN resolved_occasion_verdict = 'Occasion mission shift' THEN parent_occasion_name END)))
            AS mission_shift_occasion_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(CASE
            WHEN resolved_occasion_verdict = 'Observed occurrence response' THEN parent_occasion_name END)))
            AS periodic_response_occasion_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(CASE
            WHEN resolved_occasion_verdict IN ('Specifically seasonal', 'Uplift seasonal',
                                               'Supported specifically seasonal', 'Supported uplift seasonal')
            THEN occasion_family END))) AS resolved_seasonal_family_list,
        MAX_BY(parent_occasion_name, CASE
            WHEN resolved_occasion_verdict = 'Specifically seasonal' THEN 6000000.0 + COALESCE(parent_max_core_share_index, 0)
            WHEN resolved_occasion_verdict = 'Uplift seasonal' THEN 5000000.0 + COALESCE(parent_max_core_share_index, 0)
            WHEN resolved_occasion_verdict = 'Supported specifically seasonal' THEN 4000000.0 + COALESCE(parent_max_core_share_index, 0)
            WHEN resolved_occasion_verdict = 'Supported uplift seasonal' THEN 3000000.0 + COALESCE(parent_max_core_share_index, 0)
            WHEN resolved_occasion_verdict = 'Occasion mission shift' THEN 2000000.0 + COALESCE(parent_max_core_share_index, 0)
            WHEN resolved_occasion_verdict = 'Observed occurrence response' THEN 1000000.0 + COALESCE(parent_max_core_share_index, 0)
        END) AS primary_occasion_v2_2,
        MAX_BY(resolved_occasion_verdict, CASE
            WHEN resolved_occasion_verdict = 'Specifically seasonal' THEN 6000000.0 + COALESCE(parent_max_core_share_index, 0)
            WHEN resolved_occasion_verdict = 'Uplift seasonal' THEN 5000000.0 + COALESCE(parent_max_core_share_index, 0)
            WHEN resolved_occasion_verdict = 'Supported specifically seasonal' THEN 4000000.0 + COALESCE(parent_max_core_share_index, 0)
            WHEN resolved_occasion_verdict = 'Supported uplift seasonal' THEN 3000000.0 + COALESCE(parent_max_core_share_index, 0)
            WHEN resolved_occasion_verdict = 'Occasion mission shift' THEN 2000000.0 + COALESCE(parent_max_core_share_index, 0)
            WHEN resolved_occasion_verdict = 'Observed occurrence response' THEN 1000000.0 + COALESCE(parent_max_core_share_index, 0)
        END) AS primary_occasion_verdict_v2_2,
        MAX_BY(evidence_source_v2_2, CASE
            WHEN resolved_occasion_verdict = 'Specifically seasonal' THEN 6000000.0 + COALESCE(parent_max_core_share_index, 0)
            WHEN resolved_occasion_verdict = 'Uplift seasonal' THEN 5000000.0 + COALESCE(parent_max_core_share_index, 0)
            WHEN resolved_occasion_verdict = 'Supported specifically seasonal' THEN 4000000.0 + COALESCE(parent_max_core_share_index, 0)
            WHEN resolved_occasion_verdict = 'Supported uplift seasonal' THEN 3000000.0 + COALESCE(parent_max_core_share_index, 0)
            WHEN resolved_occasion_verdict = 'Occasion mission shift' THEN 2000000.0 + COALESCE(parent_max_core_share_index, 0)
            WHEN resolved_occasion_verdict = 'Observed occurrence response' THEN 1000000.0 + COALESCE(parent_max_core_share_index, 0)
        END) AS primary_evidence_source_v2_2
    FROM lab_customer_value_analytics.seasonality_v2_2_product_occasion_evidence
    GROUP BY tpnb
),

labelled AS (
    SELECT
        e.*,
        CASE
            WHEN e.confirmed_specific_occasions > 0 THEN 'Specifically seasonal'
            WHEN e.confirmed_uplift_occasions > 0 THEN 'Uplift seasonal'
            WHEN e.supported_specific_occasions > 0 THEN 'Supported specifically seasonal'
            WHEN e.supported_uplift_occasions > 0 THEN 'Supported uplift seasonal'
            WHEN e.suppressed_occasions_v2_2 > 0 THEN 'Occasion-suppressed'
            WHEN e.regular_occasions_v2_2 > 0 OR e.mission_shift_occasions > 0 THEN 'Regular'
            ELSE 'Insufficient evidence'
        END AS resolved_product_seasonality_v2_2
    FROM evidence e
)

SELECT
    s.*,
    COALESCE(l.confirmed_specific_occasions, 0) AS confirmed_specific_occasions_v2_2,
    COALESCE(l.confirmed_uplift_occasions, 0) AS confirmed_uplift_occasions_v2_2,
    COALESCE(l.supported_specific_occasions, 0) AS supported_specific_occasions_v2_2,
    COALESCE(l.supported_uplift_occasions, 0) AS supported_uplift_occasions_v2_2,
    COALESCE(l.mission_shift_occasions, 0) AS mission_shift_occasions_v2_2,
    COALESCE(l.periodic_response_occasions, 0) AS periodic_response_occasions_v2_2,
    COALESCE(l.suppressed_occasions_v2_2, 0) AS suppressed_occasions_v2_2,
    COALESCE(l.regular_occasions_v2_2, 0) AS regular_occasions_v2_2,
    CASE WHEN COALESCE(l.supported_specific_occasions, 0) + COALESCE(l.supported_uplift_occasions, 0) > 0
         THEN 'Y' ELSE 'N' END AS supported_single_occurrence_flag_v2_2,
    CASE WHEN COALESCE(l.mission_shift_occasions, 0) > 0 THEN 'Y' ELSE 'N' END AS occasion_mission_shift_flag_v2_2,
    CASE WHEN COALESCE(l.periodic_response_occasions, 0) > 0 THEN 'Y' ELSE 'N' END AS periodic_event_response_flag_v2_2,
    COALESCE(l.resolved_product_seasonality_v2_2, 'Insufficient evidence') AS resolved_product_seasonality_v2_2,
    l.supported_specific_occasion_list,
    l.supported_uplift_occasion_list,
    l.mission_shift_occasion_list,
    l.periodic_response_occasion_list,
    l.resolved_seasonal_family_list,
    l.primary_occasion_v2_2,
    l.primary_occasion_verdict_v2_2,
    l.primary_evidence_source_v2_2
FROM lab_customer_value_analytics.seasonality_v2_tpnb_summary s
LEFT JOIN labelled l
  ON s.tpnb = l.tpnb;


-- =====================================================================================
-- G  RESPONDER PRODUCTS FOR A FUTURE TIMING REBUILD
--     Do not overwrite the v2.1 timing tables until the diagnostics pass. This table is
--     the candidate input that statements 17-24 should use after sign-off.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_2_responder_products;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_2_responder_products
USING PARQUET AS
SELECT DISTINCT
    tpnb,
    parent_occasion_key AS occasion_key,
    resolved_occasion_verdict,
    evidence_source_v2_2,
    occasion_confidence
FROM lab_customer_value_analytics.seasonality_v2_2_product_occasion_evidence
WHERE resolved_occasion_verdict IN (
    'Specifically seasonal',
    'Uplift seasonal',
    'Supported specifically seasonal',
    'Supported uplift seasonal'
);

-- =====================================================================================
-- PART C - DOMINANCE, LAUNCH-RAMP AND PERIODIC-SEPARATION REFINEMENT (v2.3)
-- =====================================================================================
-- =====================================================================================
-- SEASONALITY THEMES v2.3 REFINEMENT OVERLAY
-- Spark SQL / Databricks / Kyuubi compatible. No QUALIFY.
--
-- Purpose
-- -------
-- Refine the v2.2 candidate without rereading sales.invoice_line.
--
-- What is retained from v2.2
--   * the abnormal-low-traffic mission-shift guard;
--   * confirmed multi-occurrence TPNB verdicts;
--   * periodic events kept outside recurring seasonality;
--   * price-derived promotion evidence remains explicitly inferred.
--
-- What is tightened
--   * single-occurrence support must be material for the product AND the subclass;
--   * the supported parent must be one of the product's and subclass's dominant parents;
--   * launch-ramp uplift is rejected unless the subclass is specifically seasonal;
--   * supported evidence remains distinct from confirmed TPNB evidence;
--   * confirmed seasonal results receive HIGH / MEDIUM / REVIEW confidence rather than
--     being silently removed when the remaining cause may be an unseen promotion.
--
-- Prerequisite
-- ------------
-- Run seasonality_themes_v2_2_candidate_overlay.sql first.
-- This script creates only seasonality_v2_3_* tables and does not overwrite v2.1/v2.2.
-- =====================================================================================


-- =====================================================================================
-- A  PARAMETERS
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_3_params;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_3_params
USING PARQUET AS
SELECT
    CAST(0.40 AS DOUBLE) AS support_product_parent_share_min,
        -- candidate parent must explain at least 40% of the product's positive incremental baskets
    CAST(0.40 AS DOUBLE) AS support_subclass_parent_share_min,
        -- same parent must explain at least 40% of the subclass's positive incremental baskets
    CAST(2 AS INT) AS support_product_parent_rank_max,
        -- at most the product's first two material parents can be supported
    CAST(2 AS INT) AS support_subclass_parent_rank_max,
        -- at most the subclass's first two material parents can back a product
    CAST(28 AS INT) AS launch_ramp_guard_days,
        -- an uplift inside the first 28 days of a newly observed range is treated as launch ramp
    CAST(0.25 AS DOUBLE) AS confirmed_parent_share_high_min,
        -- a confirmed parent explaining at least 25% of signal can be HIGH/MEDIUM confidence
    CAST(3 AS INT) AS confirmed_review_family_count_min,
        -- year-round products responding across 3+ unrelated families are marked REVIEW
    CAST(5.0 AS DOUBLE) AS periodic_z_min,
        -- one-off/periodic events need a stronger one-occurrence significance threshold
    CAST(100.0 AS DOUBLE) AS periodic_incremental_baskets_min,
    CAST(1000.0 AS DOUBLE) AS periodic_incremental_sales_min;


-- =====================================================================================
-- B  PARENT SIGNAL FEATURES
--     One row per entity x parent occasion. Signal size is positive incremental baskets,
--     summed over positive event/build-up rows and years. It is used for parent dominance,
--     not as a replacement for the verdict itself.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_3_parent_signal_features;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_3_parent_signal_features
USING PARQUET AS
WITH positive_rows AS (
    SELECT
        m.entity_level,
        m.entity_id,
        m.parent_occasion_key,
        m.occasion_family,
        m.event_year,
        m.window_type,
        m.pre_start_date,
        m.year_first_sale_date,
        m.core_baskets,
        m.core_expected_baskets,
        m.core_incremental_sales,
        m.core_share_index,
        m.is_specific_year_v2_2,
        m.is_uplift_year_v2_2,
        m.is_decline_year_v2_2,
        CASE
            WHEN m.is_specific_year_v2_2 OR m.is_uplift_year_v2_2
            THEN GREATEST(CAST(m.core_baskets AS DOUBLE) - COALESCE(m.core_expected_baskets, CAST(0 AS DOUBLE)),
                          CAST(0 AS DOUBLE))
            ELSE CAST(0 AS DOUBLE)
        END AS positive_incremental_baskets,
        CASE
            WHEN m.is_specific_year_v2_2 OR m.is_uplift_year_v2_2
            THEN GREATEST(COALESCE(m.core_incremental_sales, CAST(0 AS DOUBLE)), CAST(0 AS DOUBLE))
            ELSE CAST(0 AS DOUBLE)
        END AS positive_incremental_sales,
        CASE WHEN m.is_specific_year_v2_2 OR m.is_uplift_year_v2_2 THEN 1 ELSE 0 END AS positive_row,
        CASE
            WHEN (m.is_specific_year_v2_2 OR m.is_uplift_year_v2_2)
             AND m.year_first_sale_date IS NOT NULL
            THEN DATEDIFF(m.pre_start_date, m.year_first_sale_date)
        END AS days_from_first_sale_to_signal
    FROM lab_customer_value_analytics.seasonality_v2_2_occasion_metrics m
),

parent_rollup AS (
    SELECT
        entity_level,
        entity_id,
        parent_occasion_key,
        MAX(occasion_family) AS occasion_family,
        COUNT(DISTINCT CASE WHEN positive_row = 1 THEN event_year END) AS positive_years,
        SUM(positive_incremental_baskets) AS parent_positive_incremental_baskets,
        SUM(positive_incremental_sales) AS parent_positive_incremental_sales,
        MAX(CASE WHEN positive_row = 1 THEN core_share_index END) AS parent_max_positive_share_index,
        MIN(CASE WHEN positive_row = 1 THEN days_from_first_sale_to_signal END) AS min_days_from_first_sale_to_signal,
        SUM(CASE WHEN is_specific_year_v2_2 THEN 1 ELSE 0 END) AS positive_specific_rows,
        SUM(CASE WHEN is_uplift_year_v2_2 THEN 1 ELSE 0 END) AS positive_uplift_rows,
        SUM(CASE WHEN is_decline_year_v2_2 THEN 1 ELSE 0 END) AS decline_rows
    FROM positive_rows
    GROUP BY entity_level, entity_id, parent_occasion_key
),

entity_totals AS (
    SELECT
        entity_level,
        entity_id,
        SUM(parent_positive_incremental_baskets) AS entity_positive_incremental_baskets,
        SUM(parent_positive_incremental_sales) AS entity_positive_incremental_sales,
        SUM(CASE WHEN parent_positive_incremental_baskets > 0 THEN 1 ELSE 0 END) AS positive_parent_count,
        COUNT(DISTINCT CASE WHEN parent_positive_incremental_baskets > 0 THEN occasion_family END) AS positive_family_count
    FROM parent_rollup
    GROUP BY entity_level, entity_id
),

ranked AS (
    SELECT
        p.*,
        t.entity_positive_incremental_baskets,
        t.entity_positive_incremental_sales,
        t.positive_parent_count,
        t.positive_family_count,
        p.parent_positive_incremental_baskets
            / NULLIF(t.entity_positive_incremental_baskets, 0) AS parent_signal_share,
        DENSE_RANK() OVER (
            PARTITION BY p.entity_level, p.entity_id
            ORDER BY p.parent_positive_incremental_baskets DESC,
                     p.parent_positive_incremental_sales DESC,
                     p.parent_occasion_key
        ) AS parent_signal_rank
    FROM parent_rollup p
    JOIN entity_totals t
      ON p.entity_level = t.entity_level
     AND p.entity_id = t.entity_id
)

SELECT * FROM ranked;


-- =====================================================================================
-- C  REFINED PRODUCT x PARENT OCCASION EVIDENCE
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_3_product_occasion_evidence;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_3_product_occasion_evidence
USING PARQUET AS
WITH product_features AS (
    SELECT *
    FROM lab_customer_value_analytics.seasonality_v2_3_parent_signal_features
    WHERE entity_level = 'TPNB'
),

subclass_features AS (
    SELECT
        entity_id AS subclass_entity_id,
        parent_occasion_key,
        parent_positive_incremental_baskets AS subclass_parent_positive_incremental_baskets,
        parent_positive_incremental_sales AS subclass_parent_positive_incremental_sales,
        parent_signal_share AS subclass_parent_signal_share,
        parent_signal_rank AS subclass_parent_signal_rank,
        positive_parent_count AS subclass_positive_parent_count,
        positive_family_count AS subclass_positive_family_count
    FROM lab_customer_value_analytics.seasonality_v2_3_parent_signal_features
    WHERE entity_level = 'SUBCLASS'
),

joined AS (
    SELECT
        e.*,
        pf.parent_positive_incremental_baskets,
        pf.parent_positive_incremental_sales,
        pf.entity_positive_incremental_baskets,
        pf.parent_signal_share AS product_parent_signal_share,
        pf.parent_signal_rank AS product_parent_signal_rank,
        pf.positive_parent_count AS product_positive_parent_count,
        pf.positive_family_count AS product_positive_family_count,
        pf.min_days_from_first_sale_to_signal,
        sf.subclass_parent_positive_incremental_baskets,
        sf.subclass_parent_positive_incremental_sales,
        sf.subclass_parent_signal_share,
        sf.subclass_parent_signal_rank,
        sf.subclass_positive_parent_count,
        sf.subclass_positive_family_count,
        CASE
            WHEN e.availability_pattern = 'Launched during period'
             AND pf.min_days_from_first_sale_to_signal BETWEEN 0 AND p.launch_ramp_guard_days
            THEN 'Y' ELSE 'N'
        END AS launch_ramp_flag,
        p.support_product_parent_share_min,
        p.support_subclass_parent_share_min,
        p.support_product_parent_rank_max,
        p.support_subclass_parent_rank_max,
        p.confirmed_parent_share_high_min,
        p.confirmed_review_family_count_min
    FROM lab_customer_value_analytics.seasonality_v2_2_product_occasion_evidence e
    LEFT JOIN product_features pf
      ON e.tpnb = pf.entity_id
     AND e.parent_occasion_key = pf.parent_occasion_key
    LEFT JOIN subclass_features sf
      ON e.subclass_entity_id = sf.subclass_entity_id
     AND e.parent_occasion_key = sf.parent_occasion_key
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_3_params p
),

evaluated AS (
    SELECT
        j.*,
        CASE
            WHEN j.resolved_occasion_verdict = 'Supported specifically seasonal'
             AND j.subclass_parent_verdict = 'Specifically seasonal'
             AND COALESCE(j.product_parent_signal_share, 0) >= j.support_product_parent_share_min
             AND COALESCE(j.subclass_parent_signal_share, 0) >= j.support_subclass_parent_share_min
             AND COALESCE(j.product_parent_signal_rank, 999) <= j.support_product_parent_rank_max
             AND COALESCE(j.subclass_parent_signal_rank, 999) <= j.support_subclass_parent_rank_max
             AND j.parent_years_decline = 0
                THEN 'Supported specifically seasonal'
            WHEN j.resolved_occasion_verdict = 'Supported uplift seasonal'
             AND j.subclass_parent_verdict IN ('Specifically seasonal', 'Uplift seasonal')
             AND COALESCE(j.product_parent_signal_share, 0) >= j.support_product_parent_share_min
             AND COALESCE(j.subclass_parent_signal_share, 0) >= j.support_subclass_parent_share_min
             AND COALESCE(j.product_parent_signal_rank, 999) = 1
             AND COALESCE(j.subclass_parent_signal_rank, 999) = 1
             AND j.parent_years_decline = 0
             AND (j.launch_ramp_flag = 'N' OR j.subclass_parent_verdict = 'Specifically seasonal')
                THEN 'Supported uplift seasonal'
            WHEN j.resolved_occasion_verdict IN ('Supported specifically seasonal', 'Supported uplift seasonal')
                THEN 'Insufficient evidence'
            ELSE j.resolved_occasion_verdict
        END AS resolved_occasion_verdict_v2_3
    FROM joined j
)

SELECT
    e.*,
    CASE
        WHEN e.resolved_occasion_verdict_v2_3 IN ('Specifically seasonal', 'Uplift seasonal')
         AND e.subclass_parent_verdict IN ('Specifically seasonal', 'Uplift seasonal')
         AND COALESCE(e.product_parent_signal_share, 0) >= e.confirmed_parent_share_high_min
            THEN 'HIGH'
        WHEN e.resolved_occasion_verdict_v2_3 IN ('Specifically seasonal', 'Uplift seasonal')
         AND e.availability_pattern = 'Year-round'
         AND COALESCE(e.product_positive_family_count, 0) >= e.confirmed_review_family_count_min
            THEN 'REVIEW'
        WHEN e.resolved_occasion_verdict_v2_3 IN ('Specifically seasonal', 'Uplift seasonal')
            THEN 'MEDIUM'
        WHEN e.resolved_occasion_verdict_v2_3 IN ('Supported specifically seasonal', 'Supported uplift seasonal')
            THEN 'SUPPORTED_STRICT'
        WHEN e.resolved_occasion_verdict_v2_3 = 'Occasion mission shift'
            THEN 'DIAGNOSTIC'
        WHEN e.resolved_occasion_verdict_v2_3 = 'Observed occurrence response'
            THEN 'OCCURRENCE_ONLY'
        ELSE 'NONE'
    END AS occasion_confidence_v2_3,
    CASE
        WHEN e.resolved_occasion_verdict_v2_3 IN ('Specifically seasonal', 'Uplift seasonal')
            THEN 'TPNB_MULTI_OCCURRENCE'
        WHEN e.resolved_occasion_verdict_v2_3 IN ('Supported specifically seasonal', 'Supported uplift seasonal')
            THEN 'TPNB_SINGLE_OCCURRENCE_PLUS_DOMINANT_SAME_PARENT_SUBCLASS'
        WHEN e.resolved_occasion_verdict_v2_3 = 'Occasion mission shift'
            THEN 'ABNORMAL_TRADING_MISSION_SHIFT'
        WHEN e.resolved_occasion_verdict_v2_3 = 'Observed occurrence response'
            THEN 'TPNB_SINGLE_PERIODIC_OCCURRENCE'
        ELSE 'NONE'
    END AS evidence_source_v2_3,
    CASE
        WHEN e.resolved_occasion_verdict IN ('Supported specifically seasonal', 'Supported uplift seasonal')
         AND e.resolved_occasion_verdict_v2_3 = 'Insufficient evidence'
        THEN CONCAT_WS('; ',
             CASE WHEN COALESCE(e.product_parent_signal_share, 0) < e.support_product_parent_share_min
                  THEN 'PRODUCT_PARENT_NOT_MATERIAL' END,
             CASE WHEN COALESCE(e.subclass_parent_signal_share, 0) < e.support_subclass_parent_share_min
                  THEN 'SUBCLASS_PARENT_NOT_MATERIAL' END,
             CASE WHEN COALESCE(e.product_parent_signal_rank, 999) > e.support_product_parent_rank_max
                  THEN 'PRODUCT_PARENT_NOT_DOMINANT' END,
             CASE WHEN COALESCE(e.subclass_parent_signal_rank, 999) > e.support_subclass_parent_rank_max
                  THEN 'SUBCLASS_PARENT_NOT_DOMINANT' END,
             CASE WHEN e.launch_ramp_flag = 'Y' AND e.subclass_parent_verdict <> 'Specifically seasonal'
                  THEN 'POSSIBLE_RANGE_LAUNCH_RAMP' END)
    END AS support_rejection_reason_v2_3
FROM evaluated e;


-- =====================================================================================
-- D  REFINED TPNB SUMMARY
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_3_tpnb_summary;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_3_tpnb_summary
USING PARQUET AS
WITH evidence AS (
    SELECT
        tpnb,
        SUM(CASE WHEN resolved_occasion_verdict_v2_3 = 'Specifically seasonal' THEN 1 ELSE 0 END) AS confirmed_specific,
        SUM(CASE WHEN resolved_occasion_verdict_v2_3 = 'Uplift seasonal' THEN 1 ELSE 0 END) AS confirmed_uplift,
        SUM(CASE WHEN resolved_occasion_verdict_v2_3 = 'Supported specifically seasonal' THEN 1 ELSE 0 END) AS supported_specific,
        SUM(CASE WHEN resolved_occasion_verdict_v2_3 = 'Supported uplift seasonal' THEN 1 ELSE 0 END) AS supported_uplift,
        SUM(CASE WHEN resolved_occasion_verdict_v2_3 = 'Occasion mission shift' THEN 1 ELSE 0 END) AS mission_shift,
        SUM(CASE WHEN resolved_occasion_verdict_v2_3 = 'Occasion-suppressed' THEN 1 ELSE 0 END) AS suppressed,
        SUM(CASE WHEN resolved_occasion_verdict_v2_3 = 'Regular' THEN 1 ELSE 0 END) AS regular,
        SUM(CASE WHEN occasion_confidence_v2_3 = 'REVIEW' THEN 1 ELSE 0 END) AS review_parent_count,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(CASE
            WHEN resolved_occasion_verdict_v2_3 = 'Supported specifically seasonal' THEN parent_occasion_name END)))
            AS supported_specific_occasion_list_v2_3,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(CASE
            WHEN resolved_occasion_verdict_v2_3 = 'Supported uplift seasonal' THEN parent_occasion_name END)))
            AS supported_uplift_occasion_list_v2_3,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(CASE
            WHEN resolved_occasion_verdict_v2_3 = 'Occasion mission shift' THEN parent_occasion_name END)))
            AS mission_shift_occasion_list_v2_3,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(CASE
            WHEN occasion_confidence_v2_3 = 'REVIEW' THEN parent_occasion_name END)))
            AS review_occasion_list_v2_3,
        MAX_BY(parent_occasion_name,
            CASE resolved_occasion_verdict_v2_3
                WHEN 'Specifically seasonal' THEN 6000000.0 + COALESCE(parent_positive_incremental_baskets, 0)
                WHEN 'Uplift seasonal' THEN 5000000.0 + COALESCE(parent_positive_incremental_baskets, 0)
                WHEN 'Supported specifically seasonal' THEN 4000000.0 + COALESCE(parent_positive_incremental_baskets, 0)
                WHEN 'Supported uplift seasonal' THEN 3000000.0 + COALESCE(parent_positive_incremental_baskets, 0)
                WHEN 'Occasion mission shift' THEN 2000000.0 + COALESCE(parent_positive_incremental_baskets, 0)
                ELSE COALESCE(parent_positive_incremental_baskets, 0)
            END) AS primary_occasion_v2_3,
        MAX_BY(resolved_occasion_verdict_v2_3,
            CASE resolved_occasion_verdict_v2_3
                WHEN 'Specifically seasonal' THEN 6000000.0 + COALESCE(parent_positive_incremental_baskets, 0)
                WHEN 'Uplift seasonal' THEN 5000000.0 + COALESCE(parent_positive_incremental_baskets, 0)
                WHEN 'Supported specifically seasonal' THEN 4000000.0 + COALESCE(parent_positive_incremental_baskets, 0)
                WHEN 'Supported uplift seasonal' THEN 3000000.0 + COALESCE(parent_positive_incremental_baskets, 0)
                WHEN 'Occasion mission shift' THEN 2000000.0 + COALESCE(parent_positive_incremental_baskets, 0)
                ELSE COALESCE(parent_positive_incremental_baskets, 0)
            END) AS primary_occasion_verdict_v2_3,
        MAX_BY(occasion_confidence_v2_3,
            CASE resolved_occasion_verdict_v2_3
                WHEN 'Specifically seasonal' THEN 6000000.0 + COALESCE(parent_positive_incremental_baskets, 0)
                WHEN 'Uplift seasonal' THEN 5000000.0 + COALESCE(parent_positive_incremental_baskets, 0)
                WHEN 'Supported specifically seasonal' THEN 4000000.0 + COALESCE(parent_positive_incremental_baskets, 0)
                WHEN 'Supported uplift seasonal' THEN 3000000.0 + COALESCE(parent_positive_incremental_baskets, 0)
                WHEN 'Occasion mission shift' THEN 2000000.0 + COALESCE(parent_positive_incremental_baskets, 0)
                ELSE COALESCE(parent_positive_incremental_baskets, 0)
            END) AS primary_occasion_confidence_v2_3
    FROM lab_customer_value_analytics.seasonality_v2_3_product_occasion_evidence
    GROUP BY tpnb
),

resolved AS (
    SELECT
        b.*,
        COALESCE(e.confirmed_specific, 0) AS confirmed_specific_occasions_v2_3,
        COALESCE(e.confirmed_uplift, 0) AS confirmed_uplift_occasions_v2_3,
        COALESCE(e.supported_specific, 0) AS supported_specific_occasions_v2_3,
        COALESCE(e.supported_uplift, 0) AS supported_uplift_occasions_v2_3,
        COALESCE(e.mission_shift, 0) AS mission_shift_occasions_v2_3,
        COALESCE(e.review_parent_count, 0) AS review_parent_count_v2_3,
        e.supported_specific_occasion_list_v2_3,
        e.supported_uplift_occasion_list_v2_3,
        e.mission_shift_occasion_list_v2_3,
        e.review_occasion_list_v2_3,
        e.primary_occasion_v2_3,
        e.primary_occasion_verdict_v2_3,
        e.primary_occasion_confidence_v2_3,
        CASE
            WHEN COALESCE(e.confirmed_specific, 0) > 0 THEN 'Specifically seasonal'
            WHEN COALESCE(e.confirmed_uplift, 0) > 0 THEN 'Uplift seasonal'
            WHEN COALESCE(e.supported_specific, 0) > 0 THEN 'Supported specifically seasonal'
            WHEN COALESCE(e.supported_uplift, 0) > 0 THEN 'Supported uplift seasonal'
            WHEN COALESCE(e.suppressed, 0) > 0 THEN 'Occasion-suppressed'
            WHEN COALESCE(e.regular, 0) > 0 OR COALESCE(e.mission_shift, 0) > 0 THEN 'Regular'
            ELSE 'Insufficient evidence'
        END AS resolved_product_seasonality_v2_3
    FROM lab_customer_value_analytics.seasonality_v2_2_tpnb_summary b
    LEFT JOIN evidence e
      ON b.tpnb = e.tpnb
)

SELECT
    r.*,
    CASE WHEN r.supported_specific_occasions_v2_3 + r.supported_uplift_occasions_v2_3 > 0
         THEN 'Y' ELSE 'N' END AS supported_single_occurrence_flag_v2_3,
    CASE WHEN r.mission_shift_occasions_v2_3 > 0 THEN 'Y' ELSE 'N' END AS occasion_mission_shift_flag_v2_3,
    CASE WHEN r.review_parent_count_v2_3 > 0 THEN 'Y' ELSE 'N' END AS confirmed_review_required_flag_v2_3
FROM resolved r;


-- =====================================================================================
-- E  PERIODIC / ONE-OFF EVENT RESPONSE
--     Occurrence-specific evidence only. It is deliberately not rolled into recurring
--     product seasonality. Metrics are event/core metrics, not lifetime product totals.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_3_periodic_response;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_3_periodic_response
USING PARQUET AS
WITH periodic AS (
    SELECT
        m.entity_id AS tpnb,
        m.parent_occasion_key,
        MAX(e.parent_occasion_name) AS parent_occasion_name,
        m.event_year,
        MAX(m.event_start_date) AS event_start_date,
        MAX(m.event_end_date) AS event_end_date,
        SUM(m.core_baskets) AS core_baskets,
        SUM(m.core_expected_baskets) AS core_expected_baskets,
        SUM(CAST(m.core_baskets AS DOUBLE) - COALESCE(m.core_expected_baskets, CAST(0 AS DOUBLE)))
            AS incremental_baskets,
        SUM(m.core_sales) AS core_sales,
        SUM(m.core_incremental_sales) AS incremental_sales,
        MAX(m.core_share_index) AS max_core_share_index,
        MAX(m.core_z) AS max_core_z
    FROM lab_customer_value_analytics.seasonality_v2_2_occasion_metrics m
    JOIN lab_customer_value_analytics.seasonality_v2_2_product_occasion_evidence e
      ON m.entity_id = e.tpnb
     AND m.parent_occasion_key = e.parent_occasion_key
    WHERE m.entity_level = 'TPNB'
      AND e.recurrence_class = 'PERIODIC_OR_ONE_OFF'
      AND m.eligibility_status = 'ELIGIBLE'
    GROUP BY m.entity_id, m.parent_occasion_key, m.event_year
)

SELECT
    p.*,
    p.core_baskets / NULLIF(p.core_expected_baskets, 0) AS core_share_index,
    CASE
        WHEN p.max_core_z >= prm.periodic_z_min
         AND p.incremental_baskets >= prm.periodic_incremental_baskets_min
         AND p.incremental_sales >= prm.periodic_incremental_sales_min
            THEN 'MATERIAL_OBSERVED_RESPONSE'
        ELSE 'NO_MATERIAL_RESPONSE'
    END AS periodic_response_status
FROM periodic p
CROSS JOIN lab_customer_value_analytics.seasonality_v2_3_params prm;

-- =====================================================================================
-- PART D - STRICT PRODUCTION POLICY AND REVIEW POPULATION (v2.4, Kyuubi-fixed)
-- =====================================================================================
-- =====================================================================================
-- SEASONALITY THEMES v2.4 PRODUCTION POLICY OVERLAY
-- Spark SQL / Databricks / Kyuubi compatible. No QUALIFY.
--
-- Purpose
-- -------
-- Turn the v2.3 evidence into explicit production policies without rereading
-- sales.invoice_line and without overwriting v2.1-v2.3 tables.
--
-- This overlay does four things:
--   1) keeps v2.3 confirmed, supported, mission-shift and periodic evidence separate;
--   2) tests a narrow seasonal-subclass-shape rescue for credible one-season products;
--   3) provides STRICT, BALANCED and BROAD automation scenarios;
--   4) creates review and responder populations for the next timing rebuild.
--
-- Prerequisite
-- ------------
-- Run seasonality_themes_v2_3_refinement_overlay.sql first.
-- =====================================================================================


-- =====================================================================================
-- A  PARAMETERS
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_4_params;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_4_params
USING PARQUET AS
SELECT
    CAST(0.25 AS DOUBLE) AS shape_rescue_product_share_min,
    CAST(0.25 AS DOUBLE) AS shape_rescue_subclass_share_min,
    CAST(2 AS INT)       AS shape_rescue_parent_rank_max,
    CAST(1.40 AS DOUBLE) AS shape_rescue_concentration_min,
    CAST(0.50 AS DOUBLE) AS year_round_override_parent_share_min,
    CAST(2 AS INT)       AS year_round_override_family_count_max,
    CAST(5 AS INT)       AS review_sample_rows_per_stratum;


-- =====================================================================================
-- B  SUBCLASS SEASONAL SHAPE BY PARENT OCCASION
--     A narrow subclass can support a one-season product when its observed availability
--     or 52-week profile is seasonal in the same part of the year as the parent occasion.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_4_subclass_parent_shape;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_4_subclass_parent_shape
USING PARQUET AS
WITH parent_month AS (
    SELECT
        parent_occasion_key,
        CAST(PERCENTILE_APPROX(MONTH(event_start_date), 0.5) AS INT) AS typical_event_month
    FROM lab_customer_value_analytics.seasonality_v2_occurrences
    WHERE window_type = 'EVENT'
    GROUP BY parent_occasion_key
),

parents AS (
    SELECT
        p.parent_occasion_key,
        p.typical_event_month,
        CASE
            WHEN p.typical_event_month IN (12, 1, 2) THEN 'Winter'
            WHEN p.typical_event_month IN (3, 4, 5)  THEN 'Spring'
            WHEN p.typical_event_month IN (6, 7, 8)  THEN 'Summer'
            WHEN p.typical_event_month IN (9, 10, 11) THEN 'Autumn'
        END AS occasion_season
    FROM parent_month p
),

subclasses AS (
    SELECT
        entity_id AS subclass_entity_id,
        availability_pattern AS subclass_availability_pattern,
        time_of_year_pattern AS subclass_time_of_year_pattern,
        peak_season AS subclass_peak_season,
        seasonal_concentration_index AS subclass_seasonal_concentration_index,
        best_8_week_share_pct AS subclass_best_8_week_share_pct,
        total_baskets AS subclass_total_baskets
    FROM lab_customer_value_analytics.seasonality_v2_entity_profile
    WHERE entity_level = 'SUBCLASS'
)

SELECT
    s.*,
    p.parent_occasion_key,
    p.typical_event_month,
    p.occasion_season,
    CASE WHEN s.subclass_peak_season = p.occasion_season THEN 'Y' ELSE 'N' END AS subclass_peak_matches_occasion,
    CASE
        WHEN s.subclass_availability_pattern = 'Recurring seasonal' THEN 'Y'
        WHEN s.subclass_seasonal_concentration_index >= prm.shape_rescue_concentration_min
         AND s.subclass_peak_season = p.occasion_season THEN 'Y'
        ELSE 'N'
    END AS subclass_has_matching_seasonal_shape
FROM subclasses s
CROSS JOIN parents p
CROSS JOIN lab_customer_value_analytics.seasonality_v2_4_params prm;


-- =====================================================================================
-- C  PRODUCT x PARENT OCCASION PRODUCTION POLICY
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_4_product_occasion_policy;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_4_product_occasion_policy
USING PARQUET AS
WITH joined AS (
    SELECT
        -- e already contains hierarchy_division_name, hierarchy_subclass_code and
        -- hierarchy_subclass_name. Re-selecting them from product_dim would create duplicate
        -- column names inside alias j and make references such as j.hierarchy_division_name
        -- ambiguous in Spark/Kyuubi.
        e.*,
        d.product_description,
        d.hierarchy_department_name,
        d.hierarchy_section_name,
        d.hierarchy_class_name,
        sh.subclass_availability_pattern,
        sh.subclass_time_of_year_pattern,
        sh.subclass_peak_season,
        sh.subclass_seasonal_concentration_index,
        sh.subclass_best_8_week_share_pct,
        sh.occasion_season,
        sh.subclass_peak_matches_occasion,
        sh.subclass_has_matching_seasonal_shape,
        prm.shape_rescue_product_share_min,
        prm.shape_rescue_subclass_share_min,
        prm.shape_rescue_parent_rank_max,
        prm.year_round_override_parent_share_min,
        prm.year_round_override_family_count_max
    FROM lab_customer_value_analytics.seasonality_v2_3_product_occasion_evidence e
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_product_dim d
      ON TRY_CAST(e.tpnb AS BIGINT) = d.tpnb
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_4_subclass_parent_shape sh
      ON e.subclass_entity_id = sh.subclass_entity_id
     AND e.parent_occasion_key = sh.parent_occasion_key
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_4_params prm
),

evaluated AS (
    SELECT
        j.*,
        CASE
            WHEN j.resolved_occasion_verdict IN ('Supported specifically seasonal', 'Supported uplift seasonal')
             AND j.resolved_occasion_verdict_v2_3 = 'Insufficient evidence'
             AND j.subclass_parent_verdict IN ('Specifically seasonal', 'Uplift seasonal')
             AND COALESCE(j.product_parent_signal_share, 0) >= j.shape_rescue_product_share_min
             AND COALESCE(j.subclass_parent_signal_share, 0) >= j.shape_rescue_subclass_share_min
             AND COALESCE(j.product_parent_signal_rank, 999) <= j.shape_rescue_parent_rank_max
             AND COALESCE(j.subclass_parent_signal_rank, 999) <= j.shape_rescue_parent_rank_max
             AND j.parent_years_decline = 0
             AND j.subclass_has_matching_seasonal_shape = 'Y'
             AND (j.hierarchy_division_name <> 'HOME & CLOTHING'
                  OR j.subclass_parent_verdict = 'Specifically seasonal')
            THEN 'Y' ELSE 'N'
        END AS seasonal_shape_rescue_flag
    FROM joined j
),

resolved AS (
    SELECT
        e.*,
        CASE
            WHEN e.seasonal_shape_rescue_flag = 'Y'
                THEN e.resolved_occasion_verdict
            ELSE e.resolved_occasion_verdict_v2_3
        END AS resolved_occasion_verdict_v2_4,
        CASE
            WHEN e.seasonal_shape_rescue_flag = 'Y' THEN 'SUPPORTED_SHAPE'
            ELSE e.occasion_confidence_v2_3
        END AS occasion_confidence_v2_4,
        CASE
            WHEN e.seasonal_shape_rescue_flag = 'Y'
                THEN 'TPNB_SINGLE_OCCURRENCE_PLUS_SEASONAL_SUBCLASS_SHAPE'
            ELSE e.evidence_source_v2_3
        END AS evidence_source_v2_4
    FROM evaluated e
),

policy AS (
    SELECT
        r.*,
        CASE
            WHEN r.resolved_occasion_verdict_v2_4 IN ('Supported specifically seasonal', 'Supported uplift seasonal')
                THEN 'AUTO_ACCEPT_SUPPORTED'
            WHEN r.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Uplift seasonal')
             AND r.occasion_confidence_v2_4 = 'HIGH'
                THEN 'AUTO_ACCEPT_CONFIRMED_HIGH'
            WHEN r.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Uplift seasonal')
             AND r.occasion_confidence_v2_4 = 'MEDIUM'
                THEN 'AUTO_ACCEPT_CONFIRMED_MEDIUM_CANDIDATE'
            WHEN r.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Uplift seasonal')
             AND r.occasion_confidence_v2_4 = 'REVIEW'
                THEN 'MANUAL_REVIEW_CONFIRMED'
            WHEN r.resolved_occasion_verdict_v2_4 = 'Occasion mission shift'
                THEN 'DIAGNOSTIC_MISSION_SHIFT'
            WHEN r.resolved_occasion_verdict_v2_4 = 'Observed occurrence response'
                THEN 'OCCURRENCE_ONLY_PERIODIC'
            ELSE 'NO_ACCEPTED_SEASONAL_EVIDENCE'
        END AS production_status,
        CASE
            WHEN r.resolved_occasion_verdict_v2_4 IN ('Supported specifically seasonal', 'Supported uplift seasonal') THEN TRUE
            WHEN r.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Uplift seasonal')
             AND r.occasion_confidence_v2_4 = 'HIGH' THEN TRUE
            ELSE FALSE
        END AS accepted_strict,
        CASE
            WHEN r.resolved_occasion_verdict_v2_4 IN ('Supported specifically seasonal', 'Supported uplift seasonal') THEN TRUE
            WHEN r.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Uplift seasonal')
             AND r.occasion_confidence_v2_4 = 'HIGH' THEN TRUE
            WHEN r.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Uplift seasonal')
             AND r.occasion_confidence_v2_4 = 'MEDIUM'
             AND r.availability_pattern <> 'Year-round' THEN TRUE
            WHEN r.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Uplift seasonal')
             AND r.occasion_confidence_v2_4 = 'MEDIUM'
             AND r.availability_pattern = 'Year-round'
             AND r.subclass_parent_verdict IN ('Specifically seasonal', 'Uplift seasonal')
             AND COALESCE(r.product_parent_signal_share, 0) >= r.year_round_override_parent_share_min
             AND COALESCE(r.product_positive_family_count, 0) <= r.year_round_override_family_count_max THEN TRUE
            ELSE FALSE
        END AS accepted_balanced,
        CASE
            WHEN r.resolved_occasion_verdict_v2_4 IN ('Supported specifically seasonal', 'Supported uplift seasonal') THEN TRUE
            WHEN r.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Uplift seasonal')
             AND r.occasion_confidence_v2_4 IN ('HIGH', 'MEDIUM') THEN TRUE
            ELSE FALSE
        END AS accepted_broad,
        CASE
            WHEN r.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Uplift seasonal')
             AND r.occasion_confidence_v2_4 = 'REVIEW'
                THEN 'YEAR_ROUND_MULTI_FAMILY_OR_WEAK_PARENT_ATTRIBUTION'
            WHEN r.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Uplift seasonal')
             AND r.availability_pattern = 'Year-round'
             AND r.subclass_parent_verdict NOT IN ('Specifically seasonal', 'Uplift seasonal')
                THEN 'YEAR_ROUND_WITHOUT_SUBCLASS_CORROBORATION'
            WHEN r.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Uplift seasonal')
             AND COALESCE(r.product_parent_signal_share, 0) < r.year_round_override_parent_share_min
                THEN 'WEAK_PRODUCT_PARENT_DOMINANCE'
        END AS production_review_reason
    FROM resolved r
)

SELECT * FROM policy;


-- =====================================================================================
-- D  TPNB SUMMARY UNDER THREE PRODUCTION SCENARIOS
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_4_tpnb_summary;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_4_tpnb_summary
USING PARQUET AS
WITH rolled AS (
    SELECT
        tpnb,
        SUM(CASE WHEN accepted_strict AND resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Supported specifically seasonal') THEN 1 ELSE 0 END) AS strict_specific,
        SUM(CASE WHEN accepted_strict AND resolved_occasion_verdict_v2_4 IN ('Uplift seasonal', 'Supported uplift seasonal') THEN 1 ELSE 0 END) AS strict_uplift,
        SUM(CASE WHEN accepted_balanced AND resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Supported specifically seasonal') THEN 1 ELSE 0 END) AS balanced_specific,
        SUM(CASE WHEN accepted_balanced AND resolved_occasion_verdict_v2_4 IN ('Uplift seasonal', 'Supported uplift seasonal') THEN 1 ELSE 0 END) AS balanced_uplift,
        SUM(CASE WHEN accepted_broad AND resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Supported specifically seasonal') THEN 1 ELSE 0 END) AS broad_specific,
        SUM(CASE WHEN accepted_broad AND resolved_occasion_verdict_v2_4 IN ('Uplift seasonal', 'Supported uplift seasonal') THEN 1 ELSE 0 END) AS broad_uplift,
        SUM(CASE WHEN production_status = 'MANUAL_REVIEW_CONFIRMED' THEN 1 ELSE 0 END) AS review_occasions,
        SUM(CASE WHEN resolved_occasion_verdict_v2_4 = 'Occasion mission shift' THEN 1 ELSE 0 END) AS mission_shift_occasions,
        SUM(CASE WHEN resolved_occasion_verdict_v2_4 = 'Occasion-suppressed' THEN 1 ELSE 0 END) AS suppressed_occasions,
        SUM(CASE WHEN resolved_occasion_verdict_v2_4 = 'Regular' THEN 1 ELSE 0 END) AS regular_occasions,
        SUM(CASE WHEN seasonal_shape_rescue_flag = 'Y' THEN 1 ELSE 0 END) AS shape_rescue_occasions,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(CASE WHEN accepted_strict THEN parent_occasion_name END))) AS accepted_occasion_list_strict,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(CASE WHEN accepted_balanced THEN parent_occasion_name END))) AS accepted_occasion_list_balanced,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(CASE WHEN production_status = 'MANUAL_REVIEW_CONFIRMED' THEN parent_occasion_name END))) AS review_occasion_list_v2_4,
        MAX_BY(CASE WHEN accepted_strict THEN parent_occasion_name END,
               CASE WHEN accepted_strict THEN 1000000.0 + COALESCE(parent_positive_incremental_baskets, 0) END) AS primary_occasion_strict,
        MAX_BY(CASE WHEN accepted_balanced THEN parent_occasion_name END,
               CASE WHEN accepted_balanced THEN 1000000.0 + COALESCE(parent_positive_incremental_baskets, 0) END) AS primary_occasion_balanced,
        MAX_BY(CASE WHEN accepted_broad THEN parent_occasion_name END,
               CASE WHEN accepted_broad THEN 1000000.0 + COALESCE(parent_positive_incremental_baskets, 0) END) AS primary_occasion_broad
    FROM lab_customer_value_analytics.seasonality_v2_4_product_occasion_policy
    GROUP BY tpnb
)

SELECT
    b.*,
    COALESCE(r.strict_specific, 0) AS strict_specific_occasions,
    COALESCE(r.strict_uplift, 0) AS strict_uplift_occasions,
    COALESCE(r.balanced_specific, 0) AS balanced_specific_occasions,
    COALESCE(r.balanced_uplift, 0) AS balanced_uplift_occasions,
    COALESCE(r.broad_specific, 0) AS broad_specific_occasions,
    COALESCE(r.broad_uplift, 0) AS broad_uplift_occasions,
    COALESCE(r.review_occasions, 0) AS review_occasions_v2_4,
    COALESCE(r.mission_shift_occasions, 0) AS mission_shift_occasions_v2_4,
    COALESCE(r.shape_rescue_occasions, 0) AS shape_rescue_occasions_v2_4,
    r.accepted_occasion_list_strict,
    r.accepted_occasion_list_balanced,
    r.review_occasion_list_v2_4,
    r.primary_occasion_strict,
    r.primary_occasion_balanced,
    r.primary_occasion_broad,
    CASE
        WHEN COALESCE(r.strict_specific, 0) > 0 THEN 'Specifically seasonal'
        WHEN COALESCE(r.strict_uplift, 0) > 0 THEN 'Uplift seasonal'
        WHEN COALESCE(r.review_occasions, 0) > 0 THEN 'Review required'
        WHEN COALESCE(r.suppressed_occasions, 0) > 0 THEN 'Occasion-suppressed'
        WHEN COALESCE(r.regular_occasions, 0) > 0 OR COALESCE(r.mission_shift_occasions, 0) > 0 THEN 'Regular'
        ELSE 'Insufficient evidence'
    END AS production_seasonality_strict,
    CASE
        WHEN COALESCE(r.balanced_specific, 0) > 0 THEN 'Specifically seasonal'
        WHEN COALESCE(r.balanced_uplift, 0) > 0 THEN 'Uplift seasonal'
        WHEN COALESCE(r.review_occasions, 0) > 0 THEN 'Review required'
        WHEN COALESCE(r.suppressed_occasions, 0) > 0 THEN 'Occasion-suppressed'
        WHEN COALESCE(r.regular_occasions, 0) > 0 OR COALESCE(r.mission_shift_occasions, 0) > 0 THEN 'Regular'
        ELSE 'Insufficient evidence'
    END AS production_seasonality_balanced,
    CASE
        WHEN COALESCE(r.broad_specific, 0) > 0 THEN 'Specifically seasonal'
        WHEN COALESCE(r.broad_uplift, 0) > 0 THEN 'Uplift seasonal'
        WHEN COALESCE(r.review_occasions, 0) > 0 THEN 'Review required'
        WHEN COALESCE(r.suppressed_occasions, 0) > 0 THEN 'Occasion-suppressed'
        WHEN COALESCE(r.regular_occasions, 0) > 0 OR COALESCE(r.mission_shift_occasions, 0) > 0 THEN 'Regular'
        ELSE 'Insufficient evidence'
    END AS production_seasonality_broad
FROM lab_customer_value_analytics.seasonality_v2_3_tpnb_summary b
LEFT JOIN rolled r
  ON b.tpnb = r.tpnb;


-- =====================================================================================
-- E  RESPONDER POPULATIONS FOR A LATER TIMING REBUILD
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_4_responder_products_strict;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_4_responder_products_strict
USING PARQUET AS
SELECT DISTINCT
    tpnb,
    parent_occasion_key,
    parent_occasion_name,
    resolved_occasion_verdict_v2_4,
    occasion_confidence_v2_4,
    evidence_source_v2_4
FROM lab_customer_value_analytics.seasonality_v2_4_product_occasion_policy
WHERE accepted_strict;

DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_4_responder_products_balanced;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_4_responder_products_balanced
USING PARQUET AS
SELECT DISTINCT
    tpnb,
    parent_occasion_key,
    parent_occasion_name,
    resolved_occasion_verdict_v2_4,
    occasion_confidence_v2_4,
    evidence_source_v2_4
FROM lab_customer_value_analytics.seasonality_v2_4_product_occasion_policy
WHERE accepted_balanced;


-- =====================================================================================
-- F  DETERMINISTIC REVIEW SAMPLE
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_4_review_sample;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_4_review_sample
USING PARQUET AS
WITH candidates AS (
    SELECT
        p.*,
        COALESCE(v.expected_label, 'UNLABELLED') AS expected_label,
        ROW_NUMBER() OVER (
            PARTITION BY
                p.production_status,
                COALESCE(p.hierarchy_division_name, 'UNKNOWN'),
                p.parent_occasion_key
            ORDER BY HASH(p.tpnb, p.parent_occasion_key)
        ) AS sample_rn
    FROM lab_customer_value_analytics.seasonality_v2_4_product_occasion_policy p
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_validation_labels v
      ON p.tpnb = v.tpnb
    WHERE p.production_status IN (
        'AUTO_ACCEPT_SUPPORTED',
        'AUTO_ACCEPT_CONFIRMED_HIGH',
        'AUTO_ACCEPT_CONFIRMED_MEDIUM_CANDIDATE',
        'MANUAL_REVIEW_CONFIRMED'
    )
)

SELECT c.*
FROM candidates c
CROSS JOIN lab_customer_value_analytics.seasonality_v2_4_params prm
WHERE c.sample_rn <= prm.review_sample_rows_per_stratum;

-- =====================================================================================
-- PART E - GUARDED ANNUAL POLICY (v2.5)
-- =====================================================================================
-- =====================================================================================
-- SEASONALITY THEMES v2.5 GUARDED ANNUAL PRODUCTION POLICY
-- Spark SQL / Databricks / Kyuubi compatible. No QUALIFY.
--
-- Purpose
-- -------
-- Apply one final conservative guard to the v2.4 STRICT population before rebuilding
-- customer timing and the final Themes output. This does not reread sales.invoice_line and
-- does not overwrite v2.1-v2.4 tables.
--
-- Main safeguards
--   1) periodic / one-off events remain in their separate occurrence-response model;
--   2) explicit seasonal hierarchy identities cannot be credited to a conflicting occasion;
--   3) same-season shape rescues with competing families need rank-1 support;
--   4) minor annual occasions need stronger product + subclass seasonal evidence;
--   5) weak-dominance major-event rows are held unless corroborated.
--
-- Prerequisite
--   seasonality_themes_v2_4_production_policy_kyuubi_fixed.sql
-- =====================================================================================


-- =====================================================================================
-- A  PARAMETERS
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_5_params;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_5_params
USING PARQUET AS
SELECT
    CAST(0.40 AS DOUBLE) AS minor_uplift_product_share_min,
    CAST(0.40 AS DOUBLE) AS minor_uplift_subclass_share_min,
    CAST(0.25 AS DOUBLE) AS minor_specific_product_share_min,
    CAST(0.50 AS DOUBLE) AS minor_dominant_product_share_min,
    CAST(2 AS INT)       AS minor_positive_family_count_max,
    CAST(0.40 AS DOUBLE) AS major_weak_product_share_min,
    CAST(1 AS INT)       AS shape_conflict_rank_required;


-- =====================================================================================
-- B  PARENT OCCASION METADATA
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_5_parent_metadata;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_5_parent_metadata
USING PARQUET AS
SELECT
    parent_occasion_key,
    MAX(occasion_name)       AS parent_occasion_name,
    MAX(occasion_family)     AS occasion_family,
    MAX(is_major_occasion)   AS is_major_occasion,
    CAST(PERCENTILE_APPROX(MONTH(event_start_date), 0.5) AS INT) AS typical_event_month
FROM lab_customer_value_analytics.seasonality_v2_occurrences
WHERE window_type = 'EVENT'
GROUP BY parent_occasion_key;


-- =====================================================================================
-- C  FINAL GUARDED PRODUCT x PARENT-OCCASION POLICY
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_5_product_occasion_policy;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_5_product_occasion_policy
USING PARQUET AS
WITH base AS (
    SELECT
        p.*,
        COALESCE(m.is_major_occasion, 'N') AS is_major_occasion_v2_5,
        UPPER(CONCAT_WS(
            ' ',
            COALESCE(p.hierarchy_department_name, ''),
            COALESCE(p.hierarchy_section_name, ''),
            COALESCE(p.hierarchy_class_name, ''),
            COALESCE(p.hierarchy_subclass_name, '')
        )) AS hierarchy_text
    FROM lab_customer_value_analytics.seasonality_v2_4_product_occasion_policy p
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_5_parent_metadata m
      ON p.parent_occasion_key = m.parent_occasion_key
),

anchored AS (
    SELECT
        b.*,
        CASE
            WHEN b.hierarchy_text LIKE '%HALLOWEEN%' THEN 'HALLOWEEN'
            WHEN b.hierarchy_text LIKE '%EASTER%'
              OR b.hierarchy_text LIKE '%HOT CROSS%'
              OR b.hierarchy_text LIKE '%CREME EGG%'
              OR b.hierarchy_text LIKE '%MINI EGG%'
              OR b.hierarchy_text LIKE '%SIMNEL%' THEN 'EASTER_SUNDAY'
            WHEN b.hierarchy_text LIKE '%VALENTINE%' THEN 'VALENTINES_GALENTINES'
            WHEN b.hierarchy_text LIKE '%MOTHER% DAY%' OR b.hierarchy_text LIKE '%MOTHERS DAY%' THEN 'MOTHERS_DAY'
            WHEN b.hierarchy_text LIKE '%FATHER% DAY%' OR b.hierarchy_text LIKE '%FATHERS DAY%' THEN 'FATHERS_DAY'
            WHEN b.hierarchy_text LIKE '%HAGGIS%' THEN 'BURNS_NIGHT'
            WHEN b.hierarchy_text LIKE '%DIWALI%' THEN 'DIWALI'
            WHEN b.hierarchy_text LIKE '%LUNAR NEW YEAR%'
              OR b.hierarchy_text LIKE '%CHINESE NEW YEAR%' THEN 'LUNAR_NEW_YEAR'
            WHEN b.hierarchy_text LIKE '%HANUKKAH%' THEN 'HANUKKAH'
        END AS explicit_anchor_parent_key,
        CASE
            WHEN b.hierarchy_text LIKE '%CHRISTMAS%'
              OR b.hierarchy_text LIKE '%XMAS%'
              OR b.hierarchy_text LIKE '%MINCE PIE%'
              OR b.hierarchy_text LIKE '%ADVENT%'
              OR b.hierarchy_text LIKE '%PANETTONE%'
              OR b.hierarchy_text LIKE '%STOLLEN%'
              OR b.hierarchy_text LIKE '%PIGS IN BLANKETS%'
              OR b.hierarchy_text LIKE '%CHRISTMAS CHEESE%'
              OR b.hierarchy_text LIKE '%XMAS GIFT%'
              OR b.hierarchy_text LIKE '%XMAS SAUSAGE%'
              OR b.hierarchy_text LIKE '%SEASONAL TWISTWRAP%'
                THEN 'CHRISTMAS_NEW_YEAR'
            WHEN b.hierarchy_text LIKE '%HALLOWEEN%' THEN 'HALLOWEEN_BONFIRE'
            WHEN b.hierarchy_text LIKE '%EASTER%'
              OR b.hierarchy_text LIKE '%HOT CROSS%'
              OR b.hierarchy_text LIKE '%CREME EGG%'
              OR b.hierarchy_text LIKE '%MINI EGG%'
              OR b.hierarchy_text LIKE '%SIMNEL%' THEN 'EASTER'
            WHEN b.hierarchy_text LIKE '%VALENTINE%'
              OR b.hierarchy_text LIKE '%MOTHER% DAY%'
              OR b.hierarchy_text LIKE '%MOTHERS DAY%'
              OR b.hierarchy_text LIKE '%FATHER% DAY%'
              OR b.hierarchy_text LIKE '%FATHERS DAY%' THEN 'GIFTING_CELEBRATIONS'
            WHEN b.hierarchy_text LIKE '%HAGGIS%' THEN 'NATIONAL_DAYS'
            WHEN b.hierarchy_text LIKE '%DIWALI%'
              OR b.hierarchy_text LIKE '%LUNAR NEW YEAR%'
              OR b.hierarchy_text LIKE '%CHINESE NEW YEAR%'
              OR b.hierarchy_text LIKE '%HANUKKAH%' THEN 'WORLD_RELIGIOUS_CULTURAL'
        END AS explicit_anchor_family
    FROM base b
),

strict_shape_conflicts AS (
    SELECT
        tpnb,
        occasion_season,
        COUNT(DISTINCT occasion_family) AS strict_same_season_family_count
    FROM anchored
    WHERE accepted_strict
      AND recurrence_class <> 'PERIODIC_OR_ONE_OFF'
      AND occasion_season IS NOT NULL
    GROUP BY tpnb, occasion_season
),

features AS (
    SELECT
        a.*,
        COALESCE(c.strict_same_season_family_count, 0) AS strict_same_season_family_count,
        CASE
            WHEN a.explicit_anchor_parent_key IS NOT NULL
             AND a.parent_occasion_key <> a.explicit_anchor_parent_key THEN 'Y'
            WHEN a.explicit_anchor_family IS NOT NULL
             AND a.occasion_family <> a.explicit_anchor_family THEN 'Y'
            ELSE 'N'
        END AS explicit_anchor_conflict_flag,
        CASE
            WHEN a.explicit_anchor_parent_key IS NOT NULL
             AND a.parent_occasion_key = a.explicit_anchor_parent_key THEN 'Y'
            WHEN a.explicit_anchor_parent_key IS NULL
             AND a.explicit_anchor_family IS NOT NULL
             AND a.occasion_family = a.explicit_anchor_family THEN 'Y'
            ELSE 'N'
        END AS explicit_anchor_match_flag
    FROM anchored a
    LEFT JOIN strict_shape_conflicts c
      ON a.tpnb = c.tpnb
     AND a.occasion_season = c.occasion_season
),

evaluated AS (
    SELECT
        f.*,
        CASE
            WHEN NOT f.accepted_strict THEN FALSE
            WHEN f.recurrence_class = 'PERIODIC_OR_ONE_OFF' THEN FALSE
            WHEN f.explicit_anchor_conflict_flag = 'Y' THEN FALSE
            WHEN f.seasonal_shape_rescue_flag = 'Y'
             AND f.strict_same_season_family_count > 1
             AND f.explicit_anchor_match_flag <> 'Y'
             AND NOT (
                 COALESCE(f.product_parent_signal_rank, 999) <= p.shape_conflict_rank_required
                 AND COALESCE(f.subclass_parent_signal_rank, 999) <= p.shape_conflict_rank_required
             ) THEN FALSE
            WHEN f.explicit_anchor_match_flag = 'Y' THEN TRUE
            WHEN f.is_major_occasion_v2_5 = 'Y'
             AND f.production_review_reason IS NULL THEN TRUE
            WHEN f.is_major_occasion_v2_5 = 'Y'
             AND f.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Supported specifically seasonal')
             AND COALESCE(f.product_parent_signal_share, 0) >= p.major_weak_product_share_min THEN TRUE
            WHEN f.is_major_occasion_v2_5 <> 'Y'
             AND f.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Supported specifically seasonal')
             AND f.subclass_has_matching_seasonal_shape = 'Y'
             AND COALESCE(f.product_parent_signal_share, 0) >= p.minor_specific_product_share_min
             AND COALESCE(f.product_positive_family_count, 0) <= p.minor_positive_family_count_max THEN TRUE
            WHEN f.is_major_occasion_v2_5 <> 'Y'
             AND f.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Supported specifically seasonal')
             AND f.production_review_reason IS NULL
             AND COALESCE(f.product_parent_signal_share, 0) >= p.minor_dominant_product_share_min
             AND COALESCE(f.product_positive_family_count, 0) <= p.minor_positive_family_count_max THEN TRUE
            WHEN f.is_major_occasion_v2_5 <> 'Y'
             AND f.resolved_occasion_verdict_v2_4 IN ('Uplift seasonal', 'Supported uplift seasonal')
             AND f.subclass_has_matching_seasonal_shape = 'Y'
             AND f.production_review_reason IS NULL
             AND COALESCE(f.product_parent_signal_share, 0) >= p.minor_uplift_product_share_min
             AND COALESCE(f.subclass_parent_signal_share, 0) >= p.minor_uplift_subclass_share_min
             AND COALESCE(f.product_positive_family_count, 0) <= p.minor_positive_family_count_max THEN TRUE
            ELSE FALSE
        END AS accepted_guarded,
        CASE
            WHEN NOT f.accepted_strict THEN 'NOT_IN_V2_4_STRICT'
            WHEN f.recurrence_class = 'PERIODIC_OR_ONE_OFF' THEN 'PERIODIC_SEPARATE'
            WHEN f.explicit_anchor_conflict_flag = 'Y' THEN 'HOLD_EXPLICIT_HIERARCHY_CONFLICT'
            WHEN f.seasonal_shape_rescue_flag = 'Y'
             AND f.strict_same_season_family_count > 1
             AND f.explicit_anchor_match_flag <> 'Y'
             AND NOT (
                 COALESCE(f.product_parent_signal_rank, 999) <= p.shape_conflict_rank_required
                 AND COALESCE(f.subclass_parent_signal_rank, 999) <= p.shape_conflict_rank_required
             ) THEN 'HOLD_SAME_SEASON_SHAPE_CONFLICT'
            WHEN f.explicit_anchor_match_flag = 'Y' THEN 'ACCEPT_EXPLICIT_HIERARCHY_MATCH'
            WHEN f.is_major_occasion_v2_5 = 'Y'
             AND f.production_review_reason IS NULL THEN 'ACCEPT_MAJOR_STRONG'
            WHEN f.is_major_occasion_v2_5 = 'Y'
             AND f.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Supported specifically seasonal')
             AND COALESCE(f.product_parent_signal_share, 0) >= p.major_weak_product_share_min
                THEN 'ACCEPT_MAJOR_SPECIFIC'
            WHEN f.is_major_occasion_v2_5 <> 'Y'
             AND f.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Supported specifically seasonal')
             AND f.subclass_has_matching_seasonal_shape = 'Y'
             AND COALESCE(f.product_parent_signal_share, 0) >= p.minor_specific_product_share_min
             AND COALESCE(f.product_positive_family_count, 0) <= p.minor_positive_family_count_max
                THEN 'ACCEPT_MINOR_SPECIFIC_SHAPE'
            WHEN f.is_major_occasion_v2_5 <> 'Y'
             AND f.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Supported specifically seasonal')
             AND f.production_review_reason IS NULL
             AND COALESCE(f.product_parent_signal_share, 0) >= p.minor_dominant_product_share_min
             AND COALESCE(f.product_positive_family_count, 0) <= p.minor_positive_family_count_max
                THEN 'ACCEPT_MINOR_SPECIFIC_DOMINANT'
            WHEN f.is_major_occasion_v2_5 <> 'Y'
             AND f.resolved_occasion_verdict_v2_4 IN ('Uplift seasonal', 'Supported uplift seasonal')
             AND f.subclass_has_matching_seasonal_shape = 'Y'
             AND f.production_review_reason IS NULL
             AND COALESCE(f.product_parent_signal_share, 0) >= p.minor_uplift_product_share_min
             AND COALESCE(f.subclass_parent_signal_share, 0) >= p.minor_uplift_subclass_share_min
             AND COALESCE(f.product_positive_family_count, 0) <= p.minor_positive_family_count_max
                THEN 'ACCEPT_MINOR_UPLIFT_SHAPE'
            WHEN f.is_major_occasion_v2_5 = 'Y' THEN 'HOLD_MAJOR_WEAK_OR_UNCORROBORATED'
            ELSE 'HOLD_MINOR_INSUFFICIENT_OCCASION_EVIDENCE'
        END AS guarded_policy_reason
    FROM features f
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_5_params p
)

SELECT * FROM evaluated;


-- =====================================================================================
-- D  GUARDED TPNB SUMMARY
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_5_tpnb_summary;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_5_tpnb_summary
USING PARQUET AS
WITH rolled AS (
    SELECT
        tpnb,
        SUM(CASE WHEN accepted_guarded
                  AND resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Supported specifically seasonal')
                 THEN 1 ELSE 0 END) AS guarded_specific_occasions,
        SUM(CASE WHEN accepted_guarded
                  AND resolved_occasion_verdict_v2_4 IN ('Uplift seasonal', 'Supported uplift seasonal')
                 THEN 1 ELSE 0 END) AS guarded_uplift_occasions,
        SUM(CASE WHEN accepted_strict AND NOT accepted_guarded THEN 1 ELSE 0 END) AS held_from_v2_4_strict_occasions,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(CASE WHEN accepted_guarded THEN parent_occasion_name END)))
            AS guarded_occasion_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(CASE WHEN accepted_strict AND NOT accepted_guarded THEN parent_occasion_name END)))
            AS held_occasion_list,
        MAX_BY(
            CASE WHEN accepted_guarded THEN parent_occasion_name END,
            CASE WHEN accepted_guarded THEN 1000000.0 + COALESCE(parent_positive_incremental_baskets, 0) END
        ) AS primary_occasion_guarded
    FROM lab_customer_value_analytics.seasonality_v2_5_product_occasion_policy
    GROUP BY tpnb
)

SELECT
    s.*,
    COALESCE(r.guarded_specific_occasions, 0) AS guarded_specific_occasions,
    COALESCE(r.guarded_uplift_occasions, 0) AS guarded_uplift_occasions,
    COALESCE(r.held_from_v2_4_strict_occasions, 0) AS held_from_v2_4_strict_occasions,
    r.guarded_occasion_list,
    r.held_occasion_list,
    r.primary_occasion_guarded,
    CASE
        WHEN COALESCE(r.guarded_specific_occasions, 0) > 0 THEN 'Specifically seasonal'
        WHEN COALESCE(r.guarded_uplift_occasions, 0) > 0 THEN 'Uplift seasonal'
        WHEN COALESCE(r.held_from_v2_4_strict_occasions, 0) > 0 THEN 'Review required'
        WHEN s.production_seasonality_strict IN ('Regular', 'Occasion-suppressed') THEN s.production_seasonality_strict
        ELSE 'Insufficient evidence'
    END AS production_seasonality_guarded
FROM lab_customer_value_analytics.seasonality_v2_4_tpnb_summary s
LEFT JOIN rolled r
  ON s.tpnb = r.tpnb;


-- =====================================================================================
-- E  FINAL ANNUAL RESPONDER POPULATION CANDIDATE
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_5_responder_products_annual;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_5_responder_products_annual
USING PARQUET AS
SELECT DISTINCT
    tpnb,
    parent_occasion_key,
    parent_occasion_name,
    occasion_family,
    resolved_occasion_verdict_v2_4 AS resolved_occasion_verdict,
    occasion_confidence_v2_4       AS occasion_confidence,
    evidence_source_v2_4           AS evidence_source,
    guarded_policy_reason
FROM lab_customer_value_analytics.seasonality_v2_5_product_occasion_policy
WHERE accepted_guarded;

-- =====================================================================================
-- PART F - FINAL ANNUAL OCCASION ATTRIBUTION (v2.6)
-- =====================================================================================
-- =====================================================================================
-- SEASONALITY THEMES v2.6 FINAL ANNUAL ATTRIBUTION POLICY
-- Spark SQL / Databricks / Kyuubi compatible. No QUALIFY.
--
-- Purpose
-- -------
-- Resolve the remaining qualitative errors in v2.5 without changing the core uplift model.
-- v2.5 already performs very strongly on the labelled validation set, but review showed
-- that shape-only evidence can still credit products to a nearby minor occasion, e.g.
-- Halloween products to Diwali, autumn bulbs to Back to School, or generic cards to a
-- sporting event. v2.6 therefore adds a transparent occasion-affinity layer.
--
-- Principles
--   * Major annual occasions keep the conservative v2.5 guarded decision.
--   * Minor occasions require an explicit product/hierarchy affinity to that occasion.
--   * Strong product identity cannot be credited to a conflicting occasion family.
--   * Periodic / one-off events stay in the separate periodic-response model.
--   * No read of sales.invoice_line; no overwrite of v2.1-v2.5 tables.
--
-- Prerequisite
--   seasonality_v2_5_guarded_annual_policy.sql
-- =====================================================================================


-- =====================================================================================
-- A  PARAMETERS
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_6_annual_params;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_6_annual_params
USING PARQUET AS
SELECT
    CAST(0.25 AS DOUBLE) AS soft_affinity_product_share_min,
    CAST(2 AS INT)       AS soft_affinity_product_rank_max,
    CAST(2 AS INT)       AS soft_affinity_subclass_rank_max;


-- =====================================================================================
-- B  OCCASION-AFFINITY RULES
--    HARD: an explicit or highly specific identity for the occasion.
--    SOFT: a plausible category/misson; still needs strong product + subclass ranking.
--
--    The rule table is deliberately small and editable. Add business-approved mappings
--    here rather than weakening the statistical thresholds for every occasion.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_6_affinity_rules;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_6_affinity_rules
USING PARQUET AS
SELECT *
FROM VALUES
    -- Halloween
    ('HALLOWEEN', 'ANY_TEXT', 'HALLOWEEN',     'HARD', 'Explicit Halloween identity'),
    ('HALLOWEEN', 'ANY_TEXT', 'JAFFAWEEN',     'HARD', 'Explicit Halloween identity'),
    ('HALLOWEEN', 'ANY_TEXT', 'TRICK OR TREAT','HARD', 'Explicit Halloween identity'),
    ('HALLOWEEN', 'ANY_TEXT', 'HAUNTED',       'HARD', 'Explicit Halloween identity'),
    ('HALLOWEEN', 'ANY_TEXT', 'SPOOKY',        'HARD', 'Explicit Halloween identity'),
    ('HALLOWEEN', 'ANY_TEXT', 'WITCH',         'HARD', 'Explicit Halloween identity'),
    ('HALLOWEEN', 'ANY_TEXT', 'SKELET',        'HARD', 'Explicit Halloween identity'),
    ('HALLOWEEN', 'ANY_TEXT', 'DRACUL',        'HARD', 'Explicit Halloween identity'),
    ('HALLOWEEN', 'ANY_TEXT', 'VAMPIRE',       'HARD', 'Explicit Halloween identity'),
    ('HALLOWEEN', 'HIERARCHY_TEXT', 'SQUASH/PUMPKIN', 'SOFT', 'Pumpkin mission'),
    ('HALLOWEEN', 'HIERARCHY_TEXT', 'HALLOWEEN',       'HARD', 'Halloween hierarchy'),

    -- Bonfire / Diwali: fireworks may legitimately belong to either occurrence
    ('BONFIRE_NIGHT', 'ANY_TEXT', 'BONFIRE',   'HARD', 'Explicit Bonfire identity'),
    ('BONFIRE_NIGHT', 'ANY_TEXT', 'FIREWORK',  'SOFT', 'Fireworks mission'),
    ('BONFIRE_NIGHT', 'ANY_TEXT', 'SPARKLER',  'SOFT', 'Fireworks mission'),
    ('DIWALI', 'ANY_TEXT', 'DIWALI',           'HARD', 'Explicit Diwali identity'),
    ('DIWALI', 'ANY_TEXT', 'FIREWORK',          'SOFT', 'Fireworks may be Diwali-related'),
    ('DIWALI', 'ANY_TEXT', 'SPARKLER',          'SOFT', 'Fireworks may be Diwali-related'),
    ('DIWALI', 'ANY_TEXT', 'GLOWSTICK',         'SOFT', 'Celebration-light mission'),
    ('DIWALI', 'ANY_TEXT', 'GLOW STICK',        'SOFT', 'Celebration-light mission'),

    -- Narrow religious / cultural events
    ('BURNS_NIGHT', 'ANY_TEXT', 'HAGGIS',       'HARD', 'Explicit Burns Night mission'),
    ('PASSOVER', 'ANY_TEXT', 'PASSOVER',        'HARD', 'Explicit Passover identity'),
    ('PASSOVER', 'ANY_TEXT', 'MATZO',           'HARD', 'Explicit Passover identity'),
    ('PASSOVER', 'ANY_TEXT', 'MATZOS',          'HARD', 'Explicit Passover identity'),
    ('PASSOVER', 'HIERARCHY_TEXT', 'KOSHER PASSOVER', 'HARD', 'Passover hierarchy'),
    ('LUNAR_NEW_YEAR', 'ANY_TEXT', 'LUNAR NEW YEAR',   'HARD', 'Explicit Lunar New Year identity'),
    ('LUNAR_NEW_YEAR', 'ANY_TEXT', 'CHINESE NEW YEAR', 'HARD', 'Explicit Lunar New Year identity'),
    ('HANUKKAH', 'ANY_TEXT', 'HANUKKAH',        'HARD', 'Explicit Hanukkah identity'),

    -- Food moments
    ('PANCAKE_DAY', 'ANY_TEXT', 'PANCAKE',      'HARD', 'Explicit Pancake Day mission'),

    -- Back to School / University: exclude coincidental September ranges such as bulbs
    ('BACK_TO_SCHOOL_UNIVERSITY', 'ANY_TEXT', 'BACK TO SCHOOL',       'HARD', 'Explicit Back to School identity'),
    ('BACK_TO_SCHOOL_UNIVERSITY', 'ANY_TEXT', 'SCHOOL UNIFORM',       'HARD', 'School mission'),
    ('BACK_TO_SCHOOL_UNIVERSITY', 'ANY_TEXT', 'STATIONERY',           'HARD', 'School mission'),
    ('BACK_TO_SCHOOL_UNIVERSITY', 'ANY_TEXT', 'PENCIL',               'HARD', 'School mission'),
    ('BACK_TO_SCHOOL_UNIVERSITY', 'ANY_TEXT', 'NOTEBOOK',             'HARD', 'School mission'),
    ('BACK_TO_SCHOOL_UNIVERSITY', 'ANY_TEXT', 'RULER',                'HARD', 'School mission'),
    ('BACK_TO_SCHOOL_UNIVERSITY', 'ANY_TEXT', 'LUNCH BOX',            'HARD', 'School mission'),
    ('BACK_TO_SCHOOL_UNIVERSITY', 'ANY_TEXT', 'LUNCH BAG',            'HARD', 'School mission'),
    ('BACK_TO_SCHOOL_UNIVERSITY', 'ANY_TEXT', 'PENCIL CASE',          'HARD', 'School mission'),
    ('BACK_TO_SCHOOL_UNIVERSITY', 'HIERARCHY_TEXT', 'ON THE GO LUNCH','HARD', 'School lunch mission'),
    ('BACK_TO_SCHOOL_UNIVERSITY', 'HIERARCHY_TEXT', 'WRITING INSTRUMENTS', 'HARD', 'School mission'),
    ('BACK_TO_SCHOOL_UNIVERSITY', 'HIERARCHY_TEXT', 'COLOURING INSTRUMENTS', 'SOFT', 'School mission'),
    ('BACK_TO_SCHOOL_UNIVERSITY', 'HIERARCHY_TEXT', 'KIDS TIGHTS',    'SOFT', 'School clothing mission'),

    -- World Book Day
    ('WORLD_BOOK_DAY_UK_IRELAND', 'ANY_TEXT', 'WORLD BOOK DAY', 'HARD', 'Explicit World Book Day identity'),
    ('WORLD_BOOK_DAY_UK_IRELAND', 'HIERARCHY_TEXT', 'BOOK',      'SOFT', 'Book mission'),
    ('WORLD_BOOK_DAY_UK_IRELAND', 'HIERARCHY_TEXT', 'DRESS UP',  'SOFT', 'World Book Day costume mission'),
    ('WORLD_BOOK_DAY_UK_IRELAND', 'ANY_TEXT', 'COSTUME',         'SOFT', 'World Book Day costume mission'),

    -- Annual sport with a well-established grocery mission
    ('WIMBLEDON', 'ANY_TEXT', 'WIMBLEDON',             'HARD', 'Explicit Wimbledon identity'),
    ('WIMBLEDON', 'HIERARCHY_TEXT', 'STRAWBERRIES',    'SOFT', 'Established Wimbledon food mission'),
    ('WIMBLEDON', 'ANY_TEXT', 'TENNIS',                'SOFT', 'Tennis mission'),

    -- National day examples where explicit identity is required
    ('ST_PATRICKS_DAY', 'ANY_TEXT', 'ST PATRICK',      'HARD', 'Explicit St Patricks identity'),
    ('ST_DAVIDS_DAY',   'ANY_TEXT', 'ST DAVID',        'HARD', 'Explicit St Davids identity'),
    ('ST_GEORGES_DAY',  'ANY_TEXT', 'ST GEORGE',       'HARD', 'Explicit St Georges identity'),
    ('ST_ANDREWS_DAY',  'ANY_TEXT', 'ST ANDREW',       'HARD', 'Explicit St Andrews identity'),

    -- Annual sport / awareness events: only explicit identity should auto-accept
    ('THE_OPEN_CHAMPIONSHIP', 'ANY_TEXT', 'THE OPEN CHAMPIONSHIP', 'HARD', 'Explicit event identity'),
    ('THE_OPEN_CHAMPIONSHIP', 'ANY_TEXT', 'GOLF',                  'SOFT', 'Golf mission'),
    ('GRAND_NATIONAL', 'ANY_TEXT', 'GRAND NATIONAL',               'HARD', 'Explicit event identity')
AS r(parent_occasion_key, match_scope, match_pattern, affinity_strength, rule_note);


-- =====================================================================================
-- C  FINAL ANNUAL PRODUCT x PARENT POLICY
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_6_product_occasion_policy;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_6_product_occasion_policy
USING PARQUET AS
WITH base AS (
    SELECT
        p.*,
        UPPER(COALESCE(p.product_description, '')) AS product_text_v2_6,
        UPPER(CONCAT_WS(
            ' ',
            COALESCE(p.hierarchy_department_name, ''),
            COALESCE(p.hierarchy_section_name, ''),
            COALESCE(p.hierarchy_class_name, ''),
            COALESCE(p.hierarchy_subclass_name, '')
        )) AS hierarchy_text_v2_6,
        UPPER(CONCAT_WS(
            ' ',
            COALESCE(p.product_description, ''),
            COALESCE(p.hierarchy_department_name, ''),
            COALESCE(p.hierarchy_section_name, ''),
            COALESCE(p.hierarchy_class_name, ''),
            COALESCE(p.hierarchy_subclass_name, '')
        )) AS any_text_v2_6
    FROM lab_customer_value_analytics.seasonality_v2_5_product_occasion_policy p
),

-- Strong product identity. This is used only to prevent clearly conflicting attribution.
identity AS (
    SELECT
        b.*,
        CASE
            WHEN b.any_text_v2_6 LIKE '%HALLOWEEN%'
              OR b.any_text_v2_6 LIKE '%JAFFAWEEN%'
              OR b.any_text_v2_6 LIKE '%TRICK OR TREAT%'
              OR b.any_text_v2_6 LIKE '%HAUNTED%'
              OR b.any_text_v2_6 LIKE '%SPOOKY%'
              OR b.any_text_v2_6 LIKE '%WITCH%'
              OR b.any_text_v2_6 LIKE '%SKELET%'
              OR b.any_text_v2_6 LIKE '%DRACUL%'
              OR b.any_text_v2_6 LIKE '%VAMPIRE%' THEN 'HALLOWEEN'
            WHEN b.any_text_v2_6 LIKE '%EASTER%'
              OR b.any_text_v2_6 LIKE '%HOT CROSS%'
              OR b.any_text_v2_6 LIKE '%CREME EGG%'
              OR b.any_text_v2_6 LIKE '%MINI EGG%'
              OR b.any_text_v2_6 LIKE '%SIMNEL%' THEN 'EASTER_SUNDAY'
            WHEN b.any_text_v2_6 LIKE '%VALENTINE%' THEN 'VALENTINES_GALENTINES'
            WHEN b.any_text_v2_6 LIKE '%MOTHER% DAY%' OR b.any_text_v2_6 LIKE '%MOTHERS DAY%' THEN 'MOTHERS_DAY'
            WHEN b.any_text_v2_6 LIKE '%FATHER% DAY%' OR b.any_text_v2_6 LIKE '%FATHERS DAY%' THEN 'FATHERS_DAY'
            WHEN b.any_text_v2_6 LIKE '%HAGGIS%' THEN 'BURNS_NIGHT'
            WHEN b.any_text_v2_6 LIKE '%DIWALI%' THEN 'DIWALI'
            WHEN b.any_text_v2_6 LIKE '%PASSOVER%' OR b.any_text_v2_6 LIKE '%MATZO%' THEN 'PASSOVER'
            WHEN b.any_text_v2_6 LIKE '%LUNAR NEW YEAR%' OR b.any_text_v2_6 LIKE '%CHINESE NEW YEAR%' THEN 'LUNAR_NEW_YEAR'
            WHEN b.any_text_v2_6 LIKE '%HANUKKAH%' THEN 'HANUKKAH'
            WHEN b.any_text_v2_6 LIKE '%PANCAKE%' THEN 'PANCAKE_DAY'
        END AS identity_anchor_parent_v2_6,
        CASE
            WHEN b.any_text_v2_6 LIKE '%CHRISTMAS%'
              OR b.any_text_v2_6 LIKE '%XMAS%'
              OR b.any_text_v2_6 LIKE '%ADVENT%'
              OR b.any_text_v2_6 LIKE '%MINCE PIE%'
              OR b.any_text_v2_6 LIKE '%PANETTONE%'
              OR b.any_text_v2_6 LIKE '%STOLLEN%'
              OR b.any_text_v2_6 LIKE '%PIGS IN BLANKETS%' THEN 'CHRISTMAS_NEW_YEAR'
            WHEN b.any_text_v2_6 LIKE '%HALLOWEEN%'
              OR b.any_text_v2_6 LIKE '%JAFFAWEEN%'
              OR b.any_text_v2_6 LIKE '%TRICK OR TREAT%'
              OR b.any_text_v2_6 LIKE '%HAUNTED%'
              OR b.any_text_v2_6 LIKE '%SPOOKY%'
              OR b.any_text_v2_6 LIKE '%WITCH%'
              OR b.any_text_v2_6 LIKE '%SKELET%'
              OR b.any_text_v2_6 LIKE '%DRACUL%'
              OR b.any_text_v2_6 LIKE '%VAMPIRE%' THEN 'HALLOWEEN_BONFIRE'
            WHEN b.any_text_v2_6 LIKE '%EASTER%'
              OR b.any_text_v2_6 LIKE '%HOT CROSS%'
              OR b.any_text_v2_6 LIKE '%CREME EGG%'
              OR b.any_text_v2_6 LIKE '%MINI EGG%'
              OR b.any_text_v2_6 LIKE '%SIMNEL%' THEN 'EASTER'
            WHEN b.any_text_v2_6 LIKE '%VALENTINE%'
              OR b.any_text_v2_6 LIKE '%MOTHER% DAY%'
              OR b.any_text_v2_6 LIKE '%MOTHERS DAY%'
              OR b.any_text_v2_6 LIKE '%FATHER% DAY%'
              OR b.any_text_v2_6 LIKE '%FATHERS DAY%' THEN 'GIFTING_CELEBRATIONS'
            WHEN b.any_text_v2_6 LIKE '%HAGGIS%' THEN 'NATIONAL_DAYS'
            WHEN b.any_text_v2_6 LIKE '%DIWALI%'
              OR b.any_text_v2_6 LIKE '%PASSOVER%'
              OR b.any_text_v2_6 LIKE '%MATZO%'
              OR b.any_text_v2_6 LIKE '%LUNAR NEW YEAR%'
              OR b.any_text_v2_6 LIKE '%CHINESE NEW YEAR%'
              OR b.any_text_v2_6 LIKE '%HANUKKAH%' THEN 'WORLD_RELIGIOUS_CULTURAL'
        END AS identity_anchor_family_v2_6
    FROM base b
),

rule_matches AS (
    SELECT /*+ BROADCAST(r) */
        i.tpnb,
        i.parent_occasion_key,
        MAX(CASE WHEN r.affinity_strength = 'HARD' THEN 1 ELSE 0 END) AS hard_affinity_match,
        MAX(CASE WHEN r.affinity_strength = 'SOFT' THEN 1 ELSE 0 END) AS soft_affinity_match,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(r.match_pattern))) AS matched_affinity_patterns
    FROM identity i
    JOIN lab_customer_value_analytics.seasonality_v2_6_affinity_rules r
      ON i.parent_occasion_key = r.parent_occasion_key
     AND (
            (r.match_scope = 'ANY_TEXT'       AND i.any_text_v2_6       LIKE CONCAT('%', r.match_pattern, '%'))
         OR (r.match_scope = 'PRODUCT_TEXT'   AND i.product_text_v2_6   LIKE CONCAT('%', r.match_pattern, '%'))
         OR (r.match_scope = 'HIERARCHY_TEXT' AND i.hierarchy_text_v2_6 LIKE CONCAT('%', r.match_pattern, '%'))
     )
    GROUP BY i.tpnb, i.parent_occasion_key
),

features AS (
    SELECT
        i.*,
        COALESCE(r.hard_affinity_match, 0) AS hard_affinity_match,
        COALESCE(r.soft_affinity_match, 0) AS soft_affinity_match,
        r.matched_affinity_patterns,
        CASE
            WHEN i.identity_anchor_parent_v2_6 IS NOT NULL
             AND i.parent_occasion_key <> i.identity_anchor_parent_v2_6 THEN 'Y'
            WHEN i.identity_anchor_family_v2_6 IS NOT NULL
             AND i.occasion_family <> i.identity_anchor_family_v2_6 THEN 'Y'
            ELSE 'N'
        END AS identity_conflict_flag_v2_6,
        CASE
            WHEN i.identity_anchor_parent_v2_6 IS NOT NULL
             AND i.parent_occasion_key = i.identity_anchor_parent_v2_6 THEN 'Y'
            WHEN i.identity_anchor_parent_v2_6 IS NULL
             AND i.identity_anchor_family_v2_6 IS NOT NULL
             AND i.occasion_family = i.identity_anchor_family_v2_6 THEN 'Y'
            ELSE 'N'
        END AS identity_match_flag_v2_6
    FROM identity i
    LEFT JOIN rule_matches r
      ON i.tpnb = r.tpnb
     AND i.parent_occasion_key = r.parent_occasion_key
),

evaluated AS (
    SELECT
        f.*,
        CASE
            WHEN NOT f.accepted_guarded THEN FALSE
            WHEN f.recurrence_class = 'PERIODIC_OR_ONE_OFF' THEN FALSE
            WHEN f.identity_conflict_flag_v2_6 = 'Y' THEN FALSE
            WHEN f.is_major_occasion_v2_5 = 'Y' THEN TRUE
            WHEN f.identity_match_flag_v2_6 = 'Y' OR f.hard_affinity_match = 1 THEN TRUE
            WHEN f.soft_affinity_match = 1
             AND COALESCE(f.product_parent_signal_share, 0) >= p.soft_affinity_product_share_min
             AND COALESCE(f.product_parent_signal_rank, 999) <= p.soft_affinity_product_rank_max
             AND COALESCE(f.subclass_parent_signal_rank, 999) <= p.soft_affinity_subclass_rank_max THEN TRUE
            ELSE FALSE
        END AS accepted_final_annual,
        CASE
            WHEN NOT f.accepted_guarded THEN 'NOT_IN_V2_5_GUARDED'
            WHEN f.recurrence_class = 'PERIODIC_OR_ONE_OFF' THEN 'PERIODIC_SEPARATE'
            WHEN f.identity_conflict_flag_v2_6 = 'Y' THEN 'HOLD_CONFLICTING_PRODUCT_IDENTITY'
            WHEN f.is_major_occasion_v2_5 = 'Y' THEN 'ACCEPT_MAJOR_V2_5_GUARDED'
            WHEN f.identity_match_flag_v2_6 = 'Y' THEN 'ACCEPT_MINOR_EXPLICIT_IDENTITY'
            WHEN f.hard_affinity_match = 1 THEN 'ACCEPT_MINOR_HARD_AFFINITY'
            WHEN f.soft_affinity_match = 1
             AND COALESCE(f.product_parent_signal_share, 0) >= p.soft_affinity_product_share_min
             AND COALESCE(f.product_parent_signal_rank, 999) <= p.soft_affinity_product_rank_max
             AND COALESCE(f.subclass_parent_signal_rank, 999) <= p.soft_affinity_subclass_rank_max
                THEN 'ACCEPT_MINOR_SOFT_AFFINITY'
            WHEN f.is_major_occasion_v2_5 <> 'Y' THEN 'HOLD_MINOR_NO_APPROVED_AFFINITY'
            ELSE 'HOLD_OTHER'
        END AS final_annual_policy_reason
    FROM features f
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_6_annual_params p
)

SELECT * FROM evaluated;


-- =====================================================================================
-- D  FINAL ANNUAL RESPONDER TABLE
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_6_responder_products_annual;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_6_responder_products_annual
USING PARQUET AS
SELECT
    tpnb,
    parent_occasion_key,
    parent_occasion_name,
    occasion_family,
    resolved_occasion_verdict_v2_4 AS occasion_verdict,
    evidence_source_v2_4           AS evidence_source,
    final_annual_policy_reason,
    matched_affinity_patterns,
    product_parent_signal_share,
    subclass_parent_signal_share,
    product_parent_signal_rank,
    subclass_parent_signal_rank,
    total_baskets,
    total_sales
FROM lab_customer_value_analytics.seasonality_v2_6_product_occasion_policy
WHERE accepted_final_annual;


-- =====================================================================================
-- E  TPNB SUMMARY FOR FINAL-POLICY COMPARISON
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_6_tpnb_summary;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_6_tpnb_summary
USING PARQUET AS
WITH accepted AS (
    SELECT
        tpnb,
        COUNT(*) AS accepted_annual_occasion_count,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(parent_occasion_name))) AS accepted_annual_occasion_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(occasion_family))) AS accepted_annual_family_list,
        MAX(CASE WHEN resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Supported specifically seasonal') THEN 1 ELSE 0 END)
            AS has_specific,
        MAX(CASE WHEN resolved_occasion_verdict_v2_4 IN ('Uplift seasonal', 'Supported uplift seasonal') THEN 1 ELSE 0 END)
            AS has_uplift
    FROM lab_customer_value_analytics.seasonality_v2_6_product_occasion_policy
    WHERE accepted_final_annual
    GROUP BY tpnb
),
held AS (
    SELECT
        tpnb,
        COUNT(*) AS held_annual_occasion_count,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(parent_occasion_name))) AS held_annual_occasion_list
    FROM lab_customer_value_analytics.seasonality_v2_6_product_occasion_policy
    WHERE accepted_guarded
      AND recurrence_class <> 'PERIODIC_OR_ONE_OFF'
      AND NOT accepted_final_annual
    GROUP BY tpnb
)
SELECT
    s.*,
    COALESCE(a.accepted_annual_occasion_count, 0) AS accepted_annual_occasion_count_v2_6,
    COALESCE(a.accepted_annual_occasion_list, '') AS accepted_annual_occasion_list_v2_6,
    COALESCE(a.accepted_annual_family_list, '')   AS accepted_annual_family_list_v2_6,
    COALESCE(h.held_annual_occasion_count, 0)     AS held_annual_occasion_count_v2_6,
    COALESCE(h.held_annual_occasion_list, '')     AS held_annual_occasion_list_v2_6,
    CASE
        WHEN COALESCE(a.has_specific, 0) = 1 THEN 'Specifically seasonal'
        WHEN COALESCE(a.has_uplift, 0) = 1 THEN 'Uplift seasonal'
        WHEN s.production_seasonality_guarded = 'Occasion-suppressed' THEN 'Occasion-suppressed'
        WHEN s.production_seasonality_guarded = 'Regular' OR COALESCE(s.mission_shift_occasions_v2_4, 0) > 0 THEN 'Regular'
        WHEN COALESCE(h.held_annual_occasion_count, 0) > 0 THEN 'Review required'
        ELSE 'Insufficient evidence'
    END AS resolved_product_seasonality_v2_6
FROM lab_customer_value_analytics.seasonality_v2_5_tpnb_summary s
LEFT JOIN accepted a ON s.tpnb = a.tpnb
LEFT JOIN held h     ON s.tpnb = h.tpnb;

-- =====================================================================================
-- PART G - FINAL ANNUAL SHOPPING TIMING, WEIGHTS AND ONE-ROW-PER-TPNB THEME OUTPUT
-- =====================================================================================
-- =====================================================================================
-- SEASONALITY THEMES v2.6 FINAL ANNUAL TIMING AND THEME BUILD
-- Spark SQL / Databricks / Kyuubi compatible. No QUALIFY.
--
-- Prerequisites:
--   * seasonality_themes_v2_1.sql statements 0-15 (base analytical tables)
--   * seasonality_v2_6_final_annual_attribution.sql (approved annual responders)
--
-- Scope:
--   * Annual / recurring occasions only.
--   * Periodic and one-off events remain in the separate v2.6 periodic-confidence tables.
--   * Promotion dates below are PRICE-DROP-INFERRED from average price paid; they are not
--     the certified Tesco campaign calendar.
--   * Range landing below is inferred from the observed sales spell, not a certified
--     ranging or merchandising source.
--
-- Creates:
--   seasonality_v2_6_price_drop_periods
--   seasonality_v2_6_timing_daily
--   seasonality_v2_6_product_timing
--   seasonality_v2_6_occasion_daily_curve
--   seasonality_v2_6_occasion_timing
--   seasonality_v2_6_occasion_weights
--   seasonality_v2_6_product_occasion_profile
--   seasonality_v2_6_theme_output
-- =====================================================================================

-- 17  PRICE-DROP PERIODS  (inferred from price, for products flagged seasonal anywhere)
--     No price drops table is used. A day counts as promoted when the product's average
--     price paid that day (sales / units) is at least 10% below its reference price
--     (promo_price_change_max_pct). Reference price = 80th percentile of its daily
--     prices in that calendar month and the two before it, so a long price drop does not
--     become the normal price, and price rises carry through.
--     Promoted days at most promo_max_gap_days apart form one period. A period needs at
--     least promo_min_days promoted days.
--     Caveat: if sales.invoice_line amounts are before Clubcard and multi-buy discounts,
--     those price drops are invisible here. A price drops table can replace this
--     statement if it produces the same columns.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_6_price_drop_periods;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_6_price_drop_periods
USING PARQUET AS
WITH responders AS (
    SELECT DISTINCT tpnb
    FROM lab_customer_value_analytics.seasonality_v2_6_responder_products_annual
),

daily_price AS (
    SELECT /*+ BROADCAST(r) */
        d.entity_id                                 AS tpnb,
        d.sale_date,
        d.sales,
        d.units,
        d.sales / NULLIF(d.units, 0)                AS unit_price,
        YEAR(d.sale_date) * 12 + MONTH(d.sale_date) AS month_index
    FROM lab_customer_value_analytics.seasonality_v2_entity_daily d
    JOIN responders r
      ON d.entity_id = r.tpnb
    WHERE d.entity_level = 'TPNB'
      AND d.units > 0
      AND d.sales > 0
),

monthly_prices AS (
    SELECT tpnb, month_index, COLLECT_LIST(unit_price) AS prices
    FROM daily_price
    GROUP BY tpnb, month_index
),

reference_lists AS (
    SELECT
        tpnb,
        month_index,
        SORT_ARRAY(FLATTEN(COLLECT_LIST(prices) OVER (
            PARTITION BY tpnb ORDER BY month_index RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
        ))) AS reference_prices
    FROM monthly_prices
),

reference_price AS (
    SELECT
        tpnb,
        month_index,
        ELEMENT_AT(reference_prices, CAST(CEIL(0.8 * SIZE(reference_prices)) AS INT)) AS reference_price
    FROM reference_lists
),

price_drop_days AS (
    SELECT
        d.tpnb,
        d.sale_date,
        d.sales,
        d.units,
        r.reference_price,
        p.promo_max_gap_days
    FROM daily_price d
    JOIN reference_price r
      ON d.tpnb = r.tpnb
     AND d.month_index = r.month_index
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_params p
    WHERE d.unit_price <= r.reference_price * (1.0 + p.promo_price_change_max_pct / 100.0)
),

promo_gaps AS (
    SELECT
        d.*,
        DATEDIFF(d.sale_date, LAG(d.sale_date) OVER (PARTITION BY d.tpnb ORDER BY d.sale_date)) AS days_since_previous
    FROM price_drop_days d
),

promo_runs AS (
    SELECT
        g.*,
        SUM(CASE WHEN g.days_since_previous IS NULL OR g.days_since_previous > g.promo_max_gap_days THEN 1 ELSE 0 END)
            OVER (PARTITION BY g.tpnb ORDER BY g.sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS price_drop_period_no
    FROM promo_gaps g
),

periods AS (
    SELECT
        tpnb,
        price_drop_period_no,
        MIN(sale_date)                               AS price_drop_start,
        MAX(sale_date)                               AS price_drop_end,
        COUNT(*)                                     AS price_drop_days,
        DATEDIFF(MAX(sale_date), MIN(sale_date)) + 1 AS period_days,
        SUM(sales)                                   AS price_drop_sales,
        SUM(units)                                   AS price_drop_units,
        100.0 * (1.0 - SUM(sales) / NULLIF(SUM(units * reference_price), 0)) AS avg_observed_discount_pct
    FROM promo_runs
    GROUP BY tpnb, price_drop_period_no
)

SELECT pr.*
FROM periods pr
CROSS JOIN lab_customer_value_analytics.seasonality_v2_params p
WHERE pr.price_drop_days >= p.promo_min_days;


-- =====================================================================================
-- 18  TIMING DAILY  (product x occasion x year x day, products flagged seasonal only)
--     Products: TPNBs flagged Specifically or Uplift seasonal for the occasion or for its
--     build-up. Days: every day of each completed year from search_days before the event
--     to the end of the post-event window. search_days = the occasion's
--     buildup_search_days, or its pre-event days + default_search_days.
--       expected_baskets = market items that day x the product's mean share of items on
--                          that weekday in its baseline. 0 when it had no baseline, and
--                          0 on days it was not on sale (outside its on-sale runs).
--       excess_baskets   = baskets - expected_baskets. 0 on days inside another major
--                          occasion's windows (event or build-up), except days of this
--                          occasion's own window.
--     Baseline: the nearest 28 usable days before the search range (7-day gap), skipping
--     low-traffic days and every major occasion's windows. The nearest 28 after the
--     post-event window are added when the before side finds fewer than
--     min_baseline_days. Days the product was not on sale do not count.
--     Shopping start / midpoint / end = the day after which the running total of extra
--     demand stays at or above timing_start_pct / timing_mid_pct / timing_end_pct
--     (10% / 50% / 90%) of the year's total. Peak = day with the largest excess.
--     Shuffles: the day grid and the products' daily sales meet in one join. The
--     baseline, running totals and dates are windows on the same partitions.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_6_timing_daily;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_6_timing_daily
USING PARQUET AS
WITH prm AS (
    SELECT * FROM lab_customer_value_analytics.seasonality_v2_params
),

coverage AS (
    SELECT MIN(data_start_date) AS data_start_date
    FROM lab_customer_value_analytics.seasonality_v2_calendar_days
),

timing_occurrences AS (
    SELECT
        o.occurrence_id,
        o.occasion_key,
        o.event_year,
        o.event_start_date,
        o.event_end_date,
        o.lead_days,
        o.lag_days,
        COALESCE(g.buildup_search_days, o.lead_days + p.default_search_days) AS search_days,
        DATE_SUB(o.event_start_date, COALESCE(g.buildup_search_days, o.lead_days + p.default_search_days)) AS range_start,
        DATE_ADD(o.event_end_date, o.lag_days) AS range_end,
        p.baseline_max_scan_days AS scan_days
    FROM lab_customer_value_analytics.seasonality_v2_occurrences o
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_occasion_config g
      ON o.occasion_key = g.occasion_key
    CROSS JOIN prm p
    CROSS JOIN coverage cv
    WHERE o.is_completed = 'Y'
      AND o.window_type = 'EVENT'
      AND DATE_SUB(o.event_start_date, COALESCE(g.buildup_search_days, o.lead_days + p.default_search_days)) >= cv.data_start_date
),

-- every day inside a major occasion's windows, with the occasions (parents) it belongs to
major_day_parents AS (
    SELECT t.major_date, COLLECT_SET(o.parent_occasion_key) AS parent_keys
    FROM lab_customer_value_analytics.seasonality_v2_occurrences o
    LATERAL VIEW EXPLODE(SEQUENCE(o.pre_start_date, o.post_end_date)) t AS major_date
    WHERE o.is_major_occasion = 'Y'
    GROUP BY t.major_date
),

calendar_roles AS (
    SELECT t.occurrence_id, 'BEFORE' AS side, x.role_date
    FROM timing_occurrences t
    LATERAL VIEW EXPLODE(SEQUENCE(DATE_SUB(t.range_start, 7 + t.scan_days), DATE_SUB(t.range_start, 8))) x AS role_date
    UNION ALL
    SELECT t.occurrence_id, 'AFTER', x.role_date
    FROM timing_occurrences t
    LATERAL VIEW EXPLODE(SEQUENCE(DATE_ADD(t.range_end, 8), DATE_ADD(t.range_end, 7 + t.scan_days))) x AS role_date
    UNION ALL
    SELECT t.occurrence_id, 'RANGE', x.role_date
    FROM timing_occurrences t
    LATERAL VIEW EXPLODE(SEQUENCE(t.range_start, t.range_end)) x AS role_date
),

calendar_flags AS (
    SELECT /*+ BROADCAST(t, m) */
        r.occurrence_id,
        r.side,
        r.role_date,
        c.weekday,
        c.all_items,
        c.is_low_traffic_day,
        COALESCE(SIZE(m.parent_keys) > 0, FALSE)                                AS in_major_window,
        COALESCE(SIZE(FILTER(m.parent_keys, k -> k <> t.occasion_key)) > 0, FALSE)
            AND r.role_date < DATE_SUB(t.event_start_date, t.lead_days)            AS in_other_major_window
    FROM calendar_roles r
    JOIN timing_occurrences t
      ON r.occurrence_id = t.occurrence_id
    JOIN lab_customer_value_analytics.seasonality_v2_calendar_days c
      ON r.role_date = c.cal_date
    LEFT JOIN major_day_parents m
      ON r.role_date = m.major_date
),

base_ranked AS (
    SELECT
        f.*,
        ROW_NUMBER() OVER (
            PARTITION BY f.occurrence_id, f.side
            ORDER BY CASE WHEN f.side = 'BEFORE' THEN DATEDIFF(DATE '2100-01-01', f.role_date)
                          ELSE DATEDIFF(f.role_date, DATE '1900-01-01') END
        ) AS nearest_rank
    FROM calendar_flags f
    WHERE f.side IN ('BEFORE', 'AFTER')
      AND f.is_low_traffic_day = 'N'
      AND NOT f.in_major_window
),

base_before_counts AS (
    SELECT occurrence_id,
           SUM(CASE WHEN side = 'BEFORE' AND nearest_rank <= 28 THEN 1 ELSE 0 END) AS baseline_days_before
    FROM base_ranked
    GROUP BY occurrence_id
),

day_roles AS (
    SELECT occurrence_id, 'RANGE' AS role, role_date, weekday, all_items, in_other_major_window
    FROM calendar_flags
    WHERE side = 'RANGE'
    UNION ALL
    SELECT b.occurrence_id, 'BASE', b.role_date, b.weekday, b.all_items, b.in_other_major_window
    FROM base_ranked b
    JOIN base_before_counts n
      ON b.occurrence_id = n.occurrence_id
    CROSS JOIN prm p
    WHERE b.nearest_rank <= 28
      AND (b.side = 'BEFORE' OR n.baseline_days_before < p.min_baseline_days)
),

usable_occurrences AS (
    SELECT t.*
    FROM timing_occurrences t
    JOIN (SELECT occurrence_id, COUNT(*) AS baseline_days FROM day_roles WHERE role = 'BASE' GROUP BY occurrence_id) b
      ON t.occurrence_id = b.occurrence_id
    CROSS JOIN prm p
    WHERE b.baseline_days >= p.min_baseline_days
),

responders AS (
    SELECT DISTINCT tpnb, parent_occasion_key AS occasion_key
    FROM lab_customer_value_analytics.seasonality_v2_6_responder_products_annual
),

responder_tpnbs AS (
    SELECT DISTINCT tpnb FROM responders
),

grid AS (
    SELECT /*+ BROADCAST(u, d) */
        r.tpnb,
        u.occurrence_id,
        u.occasion_key,
        u.event_year,
        u.event_start_date,
        u.event_end_date,
        u.lead_days,
        u.lag_days,
        u.search_days,
        d.role,
        d.role_date,
        d.weekday,
        d.all_items,
        d.in_other_major_window
    FROM responders r
    JOIN usable_occurrences u
      ON r.occasion_key = u.occasion_key
    JOIN day_roles d
      ON u.occurrence_id = d.occurrence_id
),

product_daily AS (
    SELECT /*+ BROADCAST(r) */
        d.entity_id AS tpnb,
        d.sale_date,
        d.baskets,
        d.sales,
        d.units
    FROM lab_customer_value_analytics.seasonality_v2_entity_daily d
    JOIN responder_tpnbs r
      ON d.entity_id = r.tpnb
    WHERE d.entity_level = 'TPNB'
),

spells AS (
    SELECT /*+ BROADCAST(r) */
        s.entity_id AS tpnb,
        COLLECT_LIST(NAMED_STRUCT('s', s.spell_start, 'e', s.spell_end)) AS spells
    FROM lab_customer_value_analytics.seasonality_v2_availability_spells s
    JOIN responder_tpnbs r
      ON s.entity_id = r.tpnb
    WHERE s.entity_level = 'TPNB'
    GROUP BY s.entity_id
),

promos AS (
    SELECT tpnb, COLLECT_LIST(NAMED_STRUCT('s', price_drop_start, 'e', price_drop_end)) AS promos
    FROM lab_customer_value_analytics.seasonality_v2_6_price_drop_periods
    GROUP BY tpnb
),

flagged AS (
    SELECT /*+ BROADCAST(s, pr) */
        g.*,
        COALESCE(a.baskets, 0)                    AS baskets,
        COALESCE(a.sales, CAST(0 AS DOUBLE))      AS sales,
        COALESCE(a.units, CAST(0 AS DOUBLE))      AS units,
        COALESCE(EXISTS(s.spells, sp -> g.role_date BETWEEN sp.s AND sp.e), FALSE) AS on_sale,
        COALESCE(EXISTS(pr.promos, x -> g.role_date BETWEEN x.s AND x.e), FALSE)  AS on_price_drop,
        COALESCE(COALESCE(a.baskets, 0) / NULLIF(g.all_items, 0), CAST(0 AS DOUBLE)) AS item_share
    FROM grid g
    LEFT JOIN product_daily a
      ON g.tpnb = a.tpnb
     AND g.role_date = a.sale_date
    LEFT JOIN spells s
      ON g.tpnb = s.tpnb
    LEFT JOIN promos pr
      ON g.tpnb = pr.tpnb
),

with_baseline AS (
    SELECT
        f.*,
        SUM(CASE WHEN f.role = 'BASE' AND f.on_sale AND f.weekday = 1 THEN 1 ELSE 0 END) OVER w AS bn1,
        SUM(CASE WHEN f.role = 'BASE' AND f.on_sale AND f.weekday = 2 THEN 1 ELSE 0 END) OVER w AS bn2,
        SUM(CASE WHEN f.role = 'BASE' AND f.on_sale AND f.weekday = 3 THEN 1 ELSE 0 END) OVER w AS bn3,
        SUM(CASE WHEN f.role = 'BASE' AND f.on_sale AND f.weekday = 4 THEN 1 ELSE 0 END) OVER w AS bn4,
        SUM(CASE WHEN f.role = 'BASE' AND f.on_sale AND f.weekday = 5 THEN 1 ELSE 0 END) OVER w AS bn5,
        SUM(CASE WHEN f.role = 'BASE' AND f.on_sale AND f.weekday = 6 THEN 1 ELSE 0 END) OVER w AS bn6,
        SUM(CASE WHEN f.role = 'BASE' AND f.on_sale AND f.weekday = 7 THEN 1 ELSE 0 END) OVER w AS bn7,
        SUM(CASE WHEN f.role = 'BASE' AND f.on_sale AND f.weekday = 1 THEN f.item_share ELSE CAST(0 AS DOUBLE) END) OVER w AS bs1,
        SUM(CASE WHEN f.role = 'BASE' AND f.on_sale AND f.weekday = 2 THEN f.item_share ELSE CAST(0 AS DOUBLE) END) OVER w AS bs2,
        SUM(CASE WHEN f.role = 'BASE' AND f.on_sale AND f.weekday = 3 THEN f.item_share ELSE CAST(0 AS DOUBLE) END) OVER w AS bs3,
        SUM(CASE WHEN f.role = 'BASE' AND f.on_sale AND f.weekday = 4 THEN f.item_share ELSE CAST(0 AS DOUBLE) END) OVER w AS bs4,
        SUM(CASE WHEN f.role = 'BASE' AND f.on_sale AND f.weekday = 5 THEN f.item_share ELSE CAST(0 AS DOUBLE) END) OVER w AS bs5,
        SUM(CASE WHEN f.role = 'BASE' AND f.on_sale AND f.weekday = 6 THEN f.item_share ELSE CAST(0 AS DOUBLE) END) OVER w AS bs6,
        SUM(CASE WHEN f.role = 'BASE' AND f.on_sale AND f.weekday = 7 THEN f.item_share ELSE CAST(0 AS DOUBLE) END) OVER w AS bs7,
        SUM(CASE WHEN f.role = 'BASE' AND f.on_sale THEN f.baskets ELSE 0 END) OVER w AS base_baskets,
        SUM(CASE WHEN f.role = 'BASE' AND f.on_sale THEN f.sales ELSE CAST(0 AS DOUBLE) END) OVER w AS base_sales
    FROM flagged f
    WINDOW w AS (PARTITION BY f.tpnb, f.occurrence_id)
),

expected AS (
    SELECT
        b.*,
        b.bn1 + b.bn2 + b.bn3 + b.bn4 + b.bn5 + b.bn6 + b.bn7 AS baseline_days_available,
        CASE
            WHEN NOT b.on_sale THEN CAST(0 AS DOUBLE)
            WHEN b.bn1 + b.bn2 + b.bn3 + b.bn4 + b.bn5 + b.bn6 + b.bn7 = 0 THEN CAST(0 AS DOUBLE)
            WHEN ELEMENT_AT(ARRAY(b.bn1, b.bn2, b.bn3, b.bn4, b.bn5, b.bn6, b.bn7), b.weekday) > 0
                THEN b.all_items * ELEMENT_AT(ARRAY(b.bs1, b.bs2, b.bs3, b.bs4, b.bs5, b.bs6, b.bs7), b.weekday)
                     / NULLIF(ELEMENT_AT(ARRAY(b.bn1, b.bn2, b.bn3, b.bn4, b.bn5, b.bn6, b.bn7), b.weekday), 0)
            ELSE b.all_items * (b.bs1 + b.bs2 + b.bs3 + b.bs4 + b.bs5 + b.bs6 + b.bs7)
                 / NULLIF(b.bn1 + b.bn2 + b.bn3 + b.bn4 + b.bn5 + b.bn6 + b.bn7, 0)
        END AS expected_baskets,
        COALESCE(b.base_sales / NULLIF(b.base_baskets, 0), CAST(0 AS DOUBLE)) AS base_sales_per_basket
    FROM with_baseline b
    WHERE b.role = 'RANGE'
),

excess AS (
    SELECT
        e.*,
        CASE WHEN e.in_other_major_window THEN CAST(0 AS DOUBLE)
             ELSE e.baskets - e.expected_baskets END AS excess_baskets,
        CASE WHEN e.in_other_major_window THEN CAST(0 AS DOUBLE)
             ELSE e.sales - e.expected_baskets * e.base_sales_per_basket END AS incremental_sales
    FROM expected e
),

running AS (
    SELECT
        x.*,
        SUM(x.excess_baskets) OVER (
            PARTITION BY x.tpnb, x.occurrence_id ORDER BY x.role_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_excess_baskets,
        SUM(x.excess_baskets) OVER (
            PARTITION BY x.tpnb, x.occurrence_id ORDER BY x.role_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_excess_baskets,
        MAX(CASE WHEN NOT x.in_other_major_window THEN x.excess_baskets END) OVER (
            PARTITION BY x.tpnb, x.occurrence_id) AS peak_excess_baskets
    FROM excess x
),

dated AS (
    SELECT
        r.*,
        CASE WHEN r.total_excess_baskets > 0 THEN COALESCE(
            DATE_ADD(MAX(CASE WHEN r.cum_excess_baskets < p.timing_start_pct * r.total_excess_baskets THEN r.role_date END) OVER w2, 1),
            MIN(r.role_date) OVER w2) END AS shopping_start_date,
        CASE WHEN r.total_excess_baskets > 0 THEN COALESCE(
            DATE_ADD(MAX(CASE WHEN r.cum_excess_baskets < p.timing_mid_pct * r.total_excess_baskets THEN r.role_date END) OVER w2, 1),
            MIN(r.role_date) OVER w2) END AS shopping_mid_date,
        CASE WHEN r.total_excess_baskets > 0 THEN COALESCE(
            DATE_ADD(MAX(CASE WHEN r.cum_excess_baskets < p.timing_end_pct * r.total_excess_baskets THEN r.role_date END) OVER w2, 1),
            MIN(r.role_date) OVER w2) END AS shopping_end_date,
        MIN(CASE WHEN NOT r.in_other_major_window AND r.excess_baskets = r.peak_excess_baskets
                 THEN r.role_date END) OVER w2 AS peak_date
    FROM running r
    CROSS JOIN prm p
    WINDOW w2 AS (PARTITION BY r.tpnb, r.occurrence_id)
)

SELECT
    tpnb,
    occasion_key,
    occurrence_id,
    event_year,
    event_start_date,
    event_end_date,
    lead_days,
    lag_days,
    search_days,
    role_date,
    DATEDIFF(event_start_date, role_date) AS days_before_event,
    weekday,
    all_items,
    in_other_major_window,
    on_sale,
    on_price_drop,
    baskets,
    sales,
    units,
    baseline_days_available,
    expected_baskets,
    base_sales_per_basket,
    excess_baskets,
    incremental_sales,
    cum_excess_baskets,
    total_excess_baskets,
    shopping_start_date,
    shopping_mid_date,
    shopping_end_date,
    peak_date,
    COALESCE(role_date >= shopping_start_date AND role_date <= event_end_date, FALSE) AS in_shopping_window
FROM dated;


-- =====================================================================================
-- 19  PRODUCT TIMING  (one row per product x occasion x year)
--     When customers buy (start, midpoint, end, peak), when the product lands on the
--     shelf, and when its price drop starts, as dates and as days before the event.
--     range_landing: start of the on-sale run in progress on the shopping start date (or,
--       with no extra demand, during the occasion's window), if it began within
--       range_landing_max_days of the event (else it is on sale all year) and more than 28
--       days after the data starts (else it may have been on sale before the data begins).
--     price drop: price-drop periods overlapping the shopping window (start to event end).
--     customers_ahead_of_price_drop_days = price drop start - shopping start
--       (positive = customers started buying that many days before the price drop).
--     price_drop_alignment: Customers start first / Price drop starts first / Aligned (within
--       timing_align_days) / No price drop / No extra demand.
--     start_at_search_limit = Y when demand was already elevated at the first day
--       searched, so shopping started at least that early.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_6_product_timing;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_6_product_timing
USING PARQUET AS
WITH yearly AS (
    SELECT
        tpnb,
        occasion_key,
        occurrence_id,
        event_year,
        MAX(event_start_date)        AS event_start_date,
        MAX(event_end_date)          AS event_end_date,
        MAX(lead_days)               AS lead_days,
        MAX(search_days)             AS search_days,
        MAX(baseline_days_available) AS baseline_days_available,
        SUM(baskets)                 AS range_baskets,
        SUM(expected_baskets)        AS range_expected_baskets,
        MAX(total_excess_baskets)    AS range_excess_baskets,
        SUM(sales)                   AS range_sales,
        SUM(incremental_sales)       AS range_incremental_sales,
        MAX(shopping_start_date)     AS shopping_start_date,
        MAX(shopping_mid_date)       AS shopping_mid_date,
        MAX(shopping_end_date)       AS shopping_end_date,
        MAX(peak_date)               AS peak_date,
        MIN(CASE WHEN NOT in_other_major_window THEN role_date END) AS first_counted_date,
        SUM(CASE WHEN in_shopping_window AND NOT in_other_major_window THEN sales ELSE CAST(0 AS DOUBLE) END)
            AS window_sales,
        SUM(CASE WHEN in_shopping_window AND NOT in_other_major_window AND on_price_drop THEN sales ELSE CAST(0 AS DOUBLE) END)
            AS window_price_drop_sales
    FROM lab_customer_value_analytics.seasonality_v2_6_timing_daily
    GROUP BY tpnb, occasion_key, occurrence_id, event_year
),

coverage AS (
    SELECT MIN(data_start_date) AS data_start_date
    FROM lab_customer_value_analytics.seasonality_v2_calendar_days
),

occasion_names AS (
    SELECT occasion_key, MAX(occasion_name) AS occasion_name, MAX(occasion_family) AS occasion_family
    FROM lab_customer_value_analytics.seasonality_v2_occurrences
    WHERE window_type = 'EVENT'
    GROUP BY occasion_key
),

spells AS (
    SELECT s.entity_id AS tpnb, COLLECT_LIST(NAMED_STRUCT('s', s.spell_start, 'e', s.spell_end)) AS spells
    FROM lab_customer_value_analytics.seasonality_v2_availability_spells s
    LEFT SEMI JOIN (SELECT DISTINCT tpnb FROM yearly) y
      ON s.entity_id = y.tpnb
    WHERE s.entity_level = 'TPNB'
    GROUP BY s.entity_id
),

landing AS (
    SELECT /*+ BROADCAST(s) */
        y.tpnb,
        y.occurrence_id,
        ARRAY_MIN(TRANSFORM(
            FILTER(s.spells, sp -> sp.s <= COALESCE(y.shopping_start_date, y.event_end_date)
                                   AND sp.e >= COALESCE(y.shopping_start_date, DATE_SUB(y.event_start_date, y.lead_days))),
            sp -> sp.s)) AS covering_spell_start
    FROM yearly y
    LEFT JOIN spells s
      ON y.tpnb = s.tpnb
),

promo_overlap AS (
    SELECT
        y.tpnb,
        y.occurrence_id,
        MIN(p.price_drop_start) AS price_drop_start_date,
        MAX(p.price_drop_end)   AS price_drop_end_date,
        SUM(p.price_drop_sales * p.avg_observed_discount_pct) / NULLIF(SUM(p.price_drop_sales), 0) AS price_drop_avg_observed_discount_pct
    FROM yearly y
    JOIN lab_customer_value_analytics.seasonality_v2_6_price_drop_periods p
      ON y.tpnb = p.tpnb
    WHERE y.shopping_start_date IS NOT NULL
      AND p.price_drop_start <= y.event_end_date
      AND p.price_drop_end >= y.shopping_start_date
    GROUP BY y.tpnb, y.occurrence_id
),

post_event_promo AS (
    SELECT y.tpnb, y.occurrence_id, MIN(p.price_drop_start) AS post_event_price_drop_start_date
    FROM yearly y
    JOIN lab_customer_value_analytics.seasonality_v2_6_price_drop_periods p
      ON y.tpnb = p.tpnb
    WHERE p.price_drop_start > y.event_end_date
      AND p.price_drop_start <= DATE_ADD(y.event_end_date, 14)
    GROUP BY y.tpnb, y.occurrence_id
)

SELECT
    y.tpnb,
    y.occasion_key,
    n.occasion_name,
    n.occasion_family,
    y.occurrence_id,
    y.event_year,
    y.event_start_date,
    y.event_end_date,
    y.search_days,
    y.baseline_days_available,
    y.range_baskets,
    y.range_expected_baskets,
    y.range_excess_baskets,
    y.range_baskets / NULLIF(y.range_expected_baskets, 0) AS range_demand_index,
    y.range_sales,
    y.range_incremental_sales,
    CASE WHEN y.range_excess_baskets > 0 THEN 'Y' ELSE 'N' END AS has_extra_demand,
    y.shopping_start_date,
    y.shopping_mid_date,
    y.shopping_end_date,
    y.peak_date,
    DATEDIFF(y.event_start_date, y.shopping_start_date)  AS shopping_start_days_before,
    DATEDIFF(y.event_start_date, y.shopping_mid_date)    AS shopping_mid_days_before,
    DATEDIFF(y.event_start_date, y.shopping_end_date)    AS shopping_end_days_before,
    DATEDIFF(y.event_start_date, y.peak_date)            AS peak_days_before,
    DATEDIFF(y.shopping_end_date, y.shopping_start_date) + 1 AS shopping_window_days,
    CASE WHEN y.shopping_start_date = y.first_counted_date THEN 'Y' ELSE 'N' END AS start_at_search_limit,
    CASE WHEN l.covering_spell_start >= DATE_SUB(y.event_start_date, p.range_landing_max_days)
          AND l.covering_spell_start > DATE_ADD(cv.data_start_date, 28)
         THEN l.covering_spell_start END AS observed_sales_spell_start_date,
    DATEDIFF(y.event_start_date,
             CASE WHEN l.covering_spell_start >= DATE_SUB(y.event_start_date, p.range_landing_max_days)
                   AND l.covering_spell_start > DATE_ADD(cv.data_start_date, 28)
                  THEN l.covering_spell_start END) AS observed_range_start_days_before,
    po.price_drop_start_date,
    po.price_drop_end_date,
    DATEDIFF(y.event_start_date, po.price_drop_start_date) AS price_drop_start_days_before,
    po.price_drop_avg_observed_discount_pct,
    DATEDIFF(po.price_drop_start_date, y.shopping_start_date) AS customers_ahead_of_price_drop_days,
    CASE
        WHEN y.shopping_start_date IS NULL THEN 'No extra demand'
        WHEN po.price_drop_start_date IS NULL THEN 'No price drop'
        WHEN DATEDIFF(po.price_drop_start_date, y.shopping_start_date) > p.timing_align_days THEN 'Customers start first'
        WHEN DATEDIFF(y.shopping_start_date, po.price_drop_start_date) > p.timing_align_days THEN 'Price drop starts first'
        ELSE 'Aligned'
    END AS price_drop_alignment,
    y.window_sales,
    y.window_price_drop_sales,
    100.0 * (y.window_price_drop_sales / NULLIF(y.window_sales, 0)) AS window_price_drop_share_pct,
    pe.post_event_price_drop_start_date,
    'PRICE_INFERRED_FROM_AVERAGE_PRICE_PAID' AS price_drop_timing_source,
    'OBSERVED_SALES_SPELL' AS range_timing_source
FROM yearly y
CROSS JOIN lab_customer_value_analytics.seasonality_v2_params p
CROSS JOIN coverage cv
LEFT JOIN occasion_names n
  ON y.occasion_key = n.occasion_key
LEFT JOIN landing l
  ON y.tpnb = l.tpnb
 AND y.occurrence_id = l.occurrence_id
LEFT JOIN promo_overlap po
  ON y.tpnb = po.tpnb
 AND y.occurrence_id = po.occurrence_id
LEFT JOIN post_event_promo pe
  ON y.tpnb = pe.tpnb
 AND y.occurrence_id = pe.occurrence_id;


-- =====================================================================================
-- 20  OCCASION DAILY CURVE  (occasion x year x day, all its seasonal products together)
--     For charts: demand index (baskets / expected), extra demand and its running share,
--     and how much of the occasion's sales were on price drop each day.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_6_occasion_daily_curve;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_6_occasion_daily_curve
USING PARQUET AS
WITH daily AS (
    SELECT
        occasion_key,
        event_year,
        role_date,
        MAX(event_start_date)  AS event_start_date,
        MAX(event_end_date)    AS event_end_date,
        MAX(days_before_event) AS days_before_event,
        MAX(CASE WHEN in_other_major_window THEN 1 ELSE 0 END) AS in_other_major_window_flag,
        COUNT(*)                                           AS products,
        SUM(CASE WHEN on_sale THEN 1 ELSE 0 END)           AS products_on_sale,
        SUM(CASE WHEN on_price_drop THEN 1 ELSE 0 END)          AS products_on_price_drop,
        SUM(baskets)                                       AS baskets,
        SUM(expected_baskets)                              AS expected_baskets,
        SUM(excess_baskets)                                AS excess_baskets,
        SUM(sales)                                         AS sales,
        SUM(CASE WHEN on_price_drop THEN sales ELSE CAST(0 AS DOUBLE) END) AS price_drop_sales,
        SUM(incremental_sales)                             AS incremental_sales
    FROM lab_customer_value_analytics.seasonality_v2_6_timing_daily
    GROUP BY occasion_key, event_year, role_date
)

SELECT
    d.occasion_key,
    d.event_year,
    d.role_date,
    d.event_start_date,
    d.event_end_date,
    d.days_before_event,
    CASE WHEN d.in_other_major_window_flag = 1 THEN 'Y' ELSE 'N' END AS in_other_major_window,
    d.products,
    d.products_on_sale,
    d.products_on_price_drop,
    100.0 * (d.products_on_price_drop / NULLIF(d.products, 0)) AS products_on_price_drop_pct,
    d.baskets,
    d.expected_baskets,
    d.baskets / NULLIF(d.expected_baskets, 0) AS demand_index,
    d.excess_baskets,
    SUM(d.excess_baskets) OVER (
        PARTITION BY d.occasion_key, d.event_year ORDER BY d.role_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_excess_baskets,
    SUM(d.excess_baskets) OVER (
        PARTITION BY d.occasion_key, d.event_year ORDER BY d.role_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_excess_baskets,
    d.sales,
    d.price_drop_sales,
    100.0 * (d.price_drop_sales / NULLIF(d.sales, 0)) AS price_drop_sales_share_pct,
    d.incremental_sales
FROM daily d;


-- =====================================================================================
-- 21  OCCASION TIMING  (one row per occasion x year: our timing vs the customers')
--     Customers: start / midpoint / end / peak of the occasion's extra demand, from the
--       daily curve (all its seasonal products together, by baskets), with the same
--       "stays above 10% / 50% / 90%" rule as statement 18.
--     Us: our fixed pre-event window, the calibrated build-up start, when seasonal ranges
--       land (sales-weighted median over products with a seasonal range) and when
--       price drops start (sales-weighted median over products promoted in their window).
--     customers_ahead_of_price_drop_days = price drop start - customers' start.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_6_occasion_timing;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_6_occasion_timing
USING PARQUET AS
WITH prm AS (
    SELECT * FROM lab_customer_value_analytics.seasonality_v2_params
),

curve AS (
    SELECT * FROM lab_customer_value_analytics.seasonality_v2_6_occasion_daily_curve
),

dates AS (
    SELECT
        c.occasion_key,
        c.event_year,
        MAX(c.event_start_date)      AS event_start_date,
        MAX(c.event_end_date)        AS event_end_date,
        MAX(c.products)              AS products,
        MAX(c.total_excess_baskets)  AS total_excess_baskets,
        SUM(c.incremental_sales)     AS total_incremental_sales,
        SUM(c.sales)                 AS total_sales,
        CASE WHEN MAX(c.total_excess_baskets) > 0 THEN COALESCE(
            DATE_ADD(MAX(CASE WHEN c.cum_excess_baskets < p.timing_start_pct * c.total_excess_baskets THEN c.role_date END), 1),
            MIN(c.role_date)) END AS shopping_start_date,
        CASE WHEN MAX(c.total_excess_baskets) > 0 THEN COALESCE(
            DATE_ADD(MAX(CASE WHEN c.cum_excess_baskets < p.timing_mid_pct * c.total_excess_baskets THEN c.role_date END), 1),
            MIN(c.role_date)) END AS shopping_mid_date,
        CASE WHEN MAX(c.total_excess_baskets) > 0 THEN COALESCE(
            DATE_ADD(MAX(CASE WHEN c.cum_excess_baskets < p.timing_end_pct * c.total_excess_baskets THEN c.role_date END), 1),
            MIN(c.role_date)) END AS shopping_end_date,
        MAX(CASE WHEN c.in_other_major_window = 'N' THEN c.excess_baskets END) AS peak_excess_baskets,
        MIN(CASE WHEN c.in_other_major_window = 'N' THEN c.role_date END)      AS first_counted_date
    FROM curve c
    CROSS JOIN prm p
    GROUP BY c.occasion_key, c.event_year
),

peak AS (
    SELECT c.occasion_key, c.event_year, MIN(c.role_date) AS peak_date
    FROM curve c
    JOIN dates d
      ON c.occasion_key = d.occasion_key
     AND c.event_year = d.event_year
    WHERE c.in_other_major_window = 'N'
      AND c.excess_baskets = d.peak_excess_baskets
    GROUP BY c.occasion_key, c.event_year
),

window_promo AS (
    SELECT
        c.occasion_key,
        c.event_year,
        SUM(c.sales)       AS window_sales,
        SUM(c.price_drop_sales) AS window_price_drop_sales
    FROM curve c
    JOIN dates d
      ON c.occasion_key = d.occasion_key
     AND c.event_year = d.event_year
    WHERE c.in_other_major_window = 'N'
      AND c.role_date >= d.shopping_start_date
      AND c.role_date <= d.event_end_date
    GROUP BY c.occasion_key, c.event_year
),

landing_ranked AS (
    SELECT
        occasion_key,
        event_year,
        observed_range_start_days_before AS value_days,
        SUM(range_sales) OVER (
            PARTITION BY occasion_key, event_year ORDER BY observed_range_start_days_before, tpnb
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_weight,
        SUM(range_sales) OVER (
            PARTITION BY occasion_key, event_year ORDER BY observed_range_start_days_before, tpnb
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_weight
    FROM lab_customer_value_analytics.seasonality_v2_6_product_timing
    WHERE observed_range_start_days_before IS NOT NULL
      AND range_sales > 0
),

landing AS (
    SELECT occasion_key, event_year,
           MIN(CASE WHEN cum_weight >= 0.5 * total_weight THEN value_days END) AS observed_range_start_days_before,
           COUNT(*) AS products_with_observed_seasonal_range
    FROM landing_ranked
    GROUP BY occasion_key, event_year
),

promo_ranked AS (
    SELECT
        occasion_key,
        event_year,
        price_drop_start_days_before AS value_days,
        SUM(window_sales) OVER (
            PARTITION BY occasion_key, event_year ORDER BY price_drop_start_days_before, tpnb
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_weight,
        SUM(window_sales) OVER (
            PARTITION BY occasion_key, event_year ORDER BY price_drop_start_days_before, tpnb
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_weight
    FROM lab_customer_value_analytics.seasonality_v2_6_product_timing
    WHERE price_drop_start_days_before IS NOT NULL
      AND window_sales > 0
),

promo AS (
    SELECT occasion_key, event_year,
           MIN(CASE WHEN cum_weight >= 0.5 * total_weight THEN value_days END) AS price_drop_start_days_before,
           COUNT(*) AS products_with_price_drop
    FROM promo_ranked
    GROUP BY occasion_key, event_year
),

product_counts AS (
    SELECT
        occasion_key,
        event_year,
        SUM(CASE WHEN has_extra_demand = 'Y' THEN 1 ELSE 0 END)                       AS products_with_extra_demand,
        SUM(CASE WHEN price_drop_alignment = 'Customers start first' THEN 1 ELSE 0 END)    AS products_customers_before_price_drop,
        SUM(CASE WHEN price_drop_alignment = 'Price drop starts first' THEN 1 ELSE 0 END)   AS products_price_drop_first,
        SUM(CASE WHEN price_drop_alignment = 'Aligned' THEN 1 ELSE 0 END)                  AS products_price_drop_aligned,
        SUM(CASE WHEN price_drop_alignment = 'No price drop' THEN 1 ELSE 0 END)             AS products_no_price_drop
    FROM lab_customer_value_analytics.seasonality_v2_6_product_timing
    GROUP BY occasion_key, event_year
),

occasion_info AS (
    SELECT
        occasion_key,
        MAX(occasion_name)       AS occasion_name,
        MAX(occasion_family)     AS occasion_family,
        MAX(is_major_occasion)   AS is_major_occasion,
        MAX(lead_days)           AS event_window_days_before,
        MAX(buildup_lead_days)   AS buildup_start_days_before
    FROM lab_customer_value_analytics.seasonality_v2_occurrences
    WHERE window_type = 'EVENT'
    GROUP BY occasion_key
),

joined AS (
    SELECT
        d.occasion_key,
        i.occasion_name,
        i.occasion_family,
        i.is_major_occasion,
        d.event_year,
        d.event_start_date,
        d.event_end_date,
        i.event_window_days_before,
        i.buildup_start_days_before,
        d.products,
        COALESCE(pc.products_with_extra_demand, 0) AS products_with_extra_demand,
        d.total_excess_baskets,
        d.total_incremental_sales,
        d.total_sales,
        d.shopping_start_date,
        d.shopping_mid_date,
        d.shopping_end_date,
        pk.peak_date,
        DATEDIFF(d.event_start_date, d.shopping_start_date) AS shopping_start_days_before,
        DATEDIFF(d.event_start_date, d.shopping_mid_date)   AS shopping_mid_days_before,
        DATEDIFF(d.event_start_date, d.shopping_end_date)   AS shopping_end_days_before,
        DATEDIFF(d.event_start_date, pk.peak_date)          AS peak_days_before,
        DATEDIFF(d.shopping_end_date, d.shopping_start_date) + 1 AS shopping_window_days,
        CASE WHEN d.shopping_start_date = d.first_counted_date THEN 'Y' ELSE 'N' END AS start_at_search_limit,
        l.observed_range_start_days_before,
        COALESCE(l.products_with_observed_seasonal_range, 0) AS products_with_observed_seasonal_range,
        pr.price_drop_start_days_before,
        COALESCE(pr.products_with_price_drop, 0)     AS products_with_price_drop,
        COALESCE(pc.products_customers_before_price_drop, 0)    AS products_customers_before_price_drop,
        COALESCE(pc.products_price_drop_first, 0)    AS products_price_drop_first,
        COALESCE(pc.products_price_drop_aligned, 0)            AS products_price_drop_aligned,
        COALESCE(pc.products_no_price_drop, 0)       AS products_no_price_drop,
        w.window_sales,
        w.window_price_drop_sales
    FROM dates d
    LEFT JOIN occasion_info i  ON d.occasion_key = i.occasion_key
    LEFT JOIN peak pk          ON d.occasion_key = pk.occasion_key AND d.event_year = pk.event_year
    LEFT JOIN window_promo w   ON d.occasion_key = w.occasion_key  AND d.event_year = w.event_year
    LEFT JOIN landing l        ON d.occasion_key = l.occasion_key  AND d.event_year = l.event_year
    LEFT JOIN promo pr         ON d.occasion_key = pr.occasion_key AND d.event_year = pr.event_year
    LEFT JOIN product_counts pc ON d.occasion_key = pc.occasion_key AND d.event_year = pc.event_year
)

SELECT
    j.*,
    j.shopping_start_days_before - j.price_drop_start_days_before AS customers_ahead_of_price_drop_days,
    CASE
        WHEN j.shopping_start_date IS NULL THEN 'No extra demand'
        WHEN j.price_drop_start_days_before IS NULL THEN 'No price drop'
        WHEN j.shopping_start_days_before - j.price_drop_start_days_before > p.timing_align_days THEN 'Customers start first'
        WHEN j.price_drop_start_days_before - j.shopping_start_days_before > p.timing_align_days THEN 'Price drop starts first'
        ELSE 'Aligned'
    END AS price_drop_alignment,
    100.0 * (j.window_price_drop_sales / NULLIF(j.window_sales, 0)) AS window_price_drop_share_pct,
    100.0 * (j.products_with_price_drop / NULLIF(j.products_with_extra_demand, 0)) AS products_with_price_drop_pct,
    'PRICE_INFERRED_FROM_AVERAGE_PRICE_PAID' AS price_drop_timing_source,
    'OBSERVED_SALES_SPELL' AS range_timing_source
FROM joined j
CROSS JOIN prm p;


-- =====================================================================================
-- 22  OCCASION WEIGHTS  (one row per occasion: how important it is)
--     annual_incremental_sales = average extra sales per occurrence (all its seasonal
--       products, whole search range to the end of the event) x recurrence_per_year.
--     occasion_weight = its share of all occasions' annual incremental sales.
--     importance_tier: occasions ranked by that value. Tier 1 = the ones that make up the
--       first 50% of the total, Tier 2 = the next 30%, Tier 3 = the rest.
--     Typical timing = median over the years measured.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_6_occasion_weights;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_6_occasion_weights
USING PARQUET AS
WITH occasions AS (
    SELECT
        occasion_key,
        MAX(occasion_name)            AS occasion_name,
        MAX(occasion_family)          AS occasion_family,
        MAX(is_major_occasion)        AS is_major_occasion,
        MAX(calendar_years_available) AS calendar_years_available,
        MAX(lead_days)                AS event_window_days_before,
        MAX(buildup_lead_days)        AS buildup_start_days_before
    FROM lab_customer_value_analytics.seasonality_v2_occurrences o
    LEFT SEMI JOIN (
        SELECT DISTINCT parent_occasion_key
        FROM lab_customer_value_analytics.seasonality_v2_6_responder_products_annual
    ) r
      ON o.occasion_key = r.parent_occasion_key
    WHERE o.window_type = 'EVENT'
    GROUP BY o.occasion_key
),

flagged_products AS (
    SELECT
        r.parent_occasion_key AS occasion_key,
        COUNT(DISTINCT r.tpnb) AS flagged_products,
        COUNT(DISTINCT p.hierarchy_subclass_code) AS flagged_subclasses
    FROM lab_customer_value_analytics.seasonality_v2_6_responder_products_annual r
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_product_dim p
      ON TRY_CAST(r.tpnb AS BIGINT) = p.tpnb
    GROUP BY r.parent_occasion_key
),

timing_summary AS (
    SELECT
        occasion_key,
        COUNT(*)                                            AS years_measured,
        AVG(total_incremental_sales)                        AS incremental_sales_per_occurrence,
        PERCENTILE_APPROX(shopping_start_days_before, 0.5)  AS shopping_start_days_before,
        PERCENTILE_APPROX(shopping_mid_days_before, 0.5)    AS shopping_mid_days_before,
        PERCENTILE_APPROX(shopping_end_days_before, 0.5)    AS shopping_end_days_before,
        PERCENTILE_APPROX(peak_days_before, 0.5)            AS peak_days_before,
        PERCENTILE_APPROX(observed_range_start_days_before, 0.5)   AS observed_range_start_days_before,
        PERCENTILE_APPROX(price_drop_start_days_before, 0.5)     AS price_drop_start_days_before,
        PERCENTILE_APPROX(customers_ahead_of_price_drop_days, 0.5) AS customers_ahead_of_price_drop_days,
        100.0 * (SUM(window_price_drop_sales) / NULLIF(SUM(window_sales), 0)) AS window_price_drop_share_pct
    FROM lab_customer_value_analytics.seasonality_v2_6_occasion_timing
    GROUP BY occasion_key
),

market AS (
    SELECT AVG(year_sales) AS avg_annual_market_sales
    FROM (
        SELECT cal_year, SUM(all_sales) AS year_sales
        FROM lab_customer_value_analytics.seasonality_v2_calendar_days
        GROUP BY cal_year
        HAVING MIN(cal_date) = MAKE_DATE(cal_year, 1, 1)
           AND MAX(cal_date) = MAKE_DATE(cal_year, 12, 31)
    ) y
),

valued AS (
    SELECT
        o.*,
        COALESCE(g.recurrence_per_year, CAST(1.0 AS DOUBLE)) AS recurrence_per_year,
        COALESCE(f.flagged_products, 0)   AS flagged_products,
        COALESCE(f.flagged_subclasses, 0) AS flagged_subclasses,
        COALESCE(t.years_measured, 0)     AS years_measured,
        t.incremental_sales_per_occurrence,
        GREATEST(COALESCE(t.incremental_sales_per_occurrence, CAST(0 AS DOUBLE))
                 * COALESCE(g.recurrence_per_year, CAST(1.0 AS DOUBLE)), CAST(0 AS DOUBLE)) AS annual_incremental_sales,
        t.shopping_start_days_before,
        t.shopping_mid_days_before,
        t.shopping_end_days_before,
        t.peak_days_before,
        t.observed_range_start_days_before,
        t.price_drop_start_days_before,
        t.customers_ahead_of_price_drop_days,
        t.window_price_drop_share_pct,
        m.avg_annual_market_sales
    FROM occasions o
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_occasion_config g
      ON o.occasion_key = g.occasion_key
    LEFT JOIN flagged_products f
      ON o.occasion_key = f.occasion_key
    LEFT JOIN timing_summary t
      ON o.occasion_key = t.occasion_key
    CROSS JOIN market m
),

weighted AS (
    SELECT
        v.*,
        v.annual_incremental_sales / NULLIF(SUM(v.annual_incremental_sales) OVER (), 0) AS occasion_weight,
        100.0 * (v.annual_incremental_sales / NULLIF(v.avg_annual_market_sales, 0))      AS share_of_annual_sales_pct
    FROM valued v
),

ranked AS (
    SELECT
        w.*,
        RANK() OVER (ORDER BY w.annual_incremental_sales DESC) AS importance_rank,
        SUM(w.occasion_weight) OVER (
            ORDER BY w.annual_incremental_sales DESC, w.occasion_key
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS weight_before
    FROM weighted w
)

SELECT
    r.occasion_key,
    r.occasion_name,
    r.occasion_family,
    r.is_major_occasion,
    r.recurrence_per_year,
    r.calendar_years_available,
    r.years_measured,
    r.flagged_products,
    r.flagged_subclasses,
    r.incremental_sales_per_occurrence,
    r.annual_incremental_sales,
    r.occasion_weight,
    r.share_of_annual_sales_pct,
    r.importance_rank,
    CASE
        WHEN r.annual_incremental_sales <= 0 THEN 'No measured uplift'
        WHEN COALESCE(r.weight_before, CAST(0 AS DOUBLE)) < 0.5 THEN 'Tier 1'
        WHEN COALESCE(r.weight_before, CAST(0 AS DOUBLE)) < 0.8 THEN 'Tier 2'
        ELSE 'Tier 3'
    END AS importance_tier,
    CASE
        WHEN r.calendar_years_available < 2 THEN 'Dates for fewer than 2 years: no product can be flagged yet'
        WHEN r.flagged_products = 0 THEN 'No product flagged for this occasion'
    END AS weight_note,
    r.event_window_days_before,
    r.buildup_start_days_before,
    r.shopping_start_days_before,
    r.shopping_mid_days_before,
    r.shopping_end_days_before,
    r.peak_days_before,
    r.observed_range_start_days_before,
    r.price_drop_start_days_before,
    r.customers_ahead_of_price_drop_days,
    r.window_price_drop_share_pct
FROM ranked r;


-- =====================================================================================

-- =====================================================================================
-- 23  FINAL ANNUAL PRODUCT-OCCASION PROFILE
--     One row per accepted TPNB x annual parent occasion. The raw evidence verdict is
--     retained, while production_occasion_verdict normalises supported evidence into the
--     three business labels used downstream.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_6_product_occasion_profile;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_6_product_occasion_profile
USING PARQUET AS
WITH prm AS (
    SELECT * FROM lab_customer_value_analytics.seasonality_v2_params
),

accepted AS (
    SELECT
        p.tpnb,
        p.parent_occasion_key AS occasion_key,
        p.parent_occasion_name AS occasion_name,
        p.occasion_family,
        p.resolved_occasion_verdict_v2_4 AS evidence_occasion_verdict,
        CASE
            WHEN p.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Supported specifically seasonal')
                THEN 'Specifically seasonal'
            WHEN p.resolved_occasion_verdict_v2_4 IN ('Uplift seasonal', 'Supported uplift seasonal')
                THEN 'Uplift seasonal'
        END AS production_occasion_verdict,
        p.event_verdict,
        p.buildup_verdict,
        p.event_median_index,
        p.buildup_median_index,
        CASE
            WHEN p.confirmed_timing_type = 'BUILDUP_AND_EVENT' THEN 'Build-up and event'
            WHEN p.confirmed_timing_type = 'BUILDUP_ONLY' THEN 'Build-up only'
            WHEN p.confirmed_timing_type = 'EVENT_ONLY' THEN 'Event only'
            WHEN p.confirmed_timing_type = 'EVENT' THEN 'Event'
            WHEN p.strongest_signal_window_type = 'BUILDUP' THEN 'Build-up only'
            ELSE 'Event'
        END AS timing_type,
        p.evidence_source_v2_4 AS evidence_source,
        p.final_annual_policy_reason,
        p.matched_affinity_patterns,
        p.product_parent_signal_share,
        p.subclass_parent_signal_share,
        p.product_parent_signal_rank,
        p.subclass_parent_signal_rank
    FROM lab_customer_value_analytics.seasonality_v2_6_product_occasion_policy p
    WHERE p.accepted_final_annual
),

timing AS (
    SELECT
        tpnb,
        occasion_key,
        COUNT(*) AS years_measured,
        SUM(CASE WHEN has_extra_demand = 'Y' THEN 1 ELSE 0 END) AS years_with_extra_demand,
        SUM(CASE WHEN start_at_search_limit = 'Y' THEN 1 ELSE 0 END) AS years_start_at_search_limit,
        PERCENTILE_APPROX(shopping_start_days_before, 0.5) AS shopping_start_days_before,
        PERCENTILE_APPROX(shopping_mid_days_before, 0.5) AS shopping_mid_days_before,
        PERCENTILE_APPROX(shopping_end_days_before, 0.5) AS shopping_end_days_before,
        PERCENTILE_APPROX(peak_days_before, 0.5) AS peak_days_before,
        PERCENTILE_APPROX(shopping_window_days, 0.5) AS shopping_window_days,
        PERCENTILE_APPROX(observed_range_start_days_before, 0.5) AS observed_range_start_days_before,
        SUM(CASE WHEN price_drop_start_date IS NOT NULL THEN 1 ELSE 0 END) AS years_with_observed_price_drop,
        PERCENTILE_APPROX(price_drop_start_days_before, 0.5) AS price_drop_start_days_before,
        PERCENTILE_APPROX(customers_ahead_of_price_drop_days, 0.5) AS customers_ahead_of_price_drop_days,
        100.0 * (SUM(window_price_drop_sales) / NULLIF(SUM(window_sales), 0)) AS window_price_drop_share_pct,
        SUM(CASE WHEN post_event_price_drop_start_date IS NOT NULL THEN 1 ELSE 0 END) AS years_with_post_event_price_drop,
        AVG(range_incremental_sales) AS incremental_sales_per_occurrence
    FROM lab_customer_value_analytics.seasonality_v2_6_product_timing
    GROUP BY tpnb, occasion_key
),

combined AS (
    SELECT
        a.*,
        t.years_measured,
        t.years_with_extra_demand,
        t.years_start_at_search_limit,
        t.shopping_start_days_before,
        t.shopping_mid_days_before,
        t.shopping_end_days_before,
        t.peak_days_before,
        t.shopping_window_days,
        w.event_window_days_before,
        w.shopping_start_days_before AS occasion_shopping_start_days_before,
        CASE
            WHEN t.shopping_start_days_before IS NULL OR w.shopping_start_days_before IS NULL THEN NULL
            WHEN t.shopping_start_days_before - w.shopping_start_days_before > p.timing_align_days
                THEN 'Earlier than the occasion portfolio'
            WHEN w.shopping_start_days_before - t.shopping_start_days_before > p.timing_align_days
                THEN 'Later than the occasion portfolio'
            ELSE 'In line with the occasion portfolio'
        END AS timing_vs_occasion,
        t.observed_range_start_days_before,
        CASE
            WHEN t.observed_range_start_days_before IS NOT NULL
             AND w.shopping_start_days_before - t.observed_range_start_days_before > p.timing_align_days
                THEN 'Y' ELSE 'N'
        END AS observed_range_starts_after_customer_flag,
        t.years_with_observed_price_drop,
        t.price_drop_start_days_before,
        t.customers_ahead_of_price_drop_days,
        CASE
            WHEN COALESCE(t.years_with_extra_demand, 0) = 0 THEN 'No extra demand'
            WHEN t.years_with_observed_price_drop = 0 THEN 'No visible price drop'
            WHEN t.customers_ahead_of_price_drop_days > p.timing_align_days THEN 'Customers start first'
            WHEN t.customers_ahead_of_price_drop_days < -p.timing_align_days THEN 'Price drop starts first'
            ELSE 'Aligned'
        END AS price_drop_alignment,
        t.window_price_drop_share_pct,
        t.years_with_post_event_price_drop,
        t.incremental_sales_per_occurrence,
        w.recurrence_per_year,
        t.incremental_sales_per_occurrence * w.recurrence_per_year AS annual_incremental_sales,
        w.annual_incremental_sales AS occasion_annual_incremental_sales,
        w.occasion_weight AS occasion_importance_weight,
        w.importance_tier AS occasion_importance_tier,
        'PRICE_INFERRED_FROM_AVERAGE_PRICE_PAID' AS price_drop_timing_source,
        'OBSERVED_SALES_SPELL' AS range_timing_source
    FROM accepted a
    CROSS JOIN prm p
    LEFT JOIN timing t
      ON a.tpnb = t.tpnb
     AND a.occasion_key = t.occasion_key
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_6_occasion_weights w
      ON a.occasion_key = w.occasion_key
)

SELECT
    c.*,
    GREATEST(COALESCE(c.annual_incremental_sales, CAST(0 AS DOUBLE)), CAST(0 AS DOUBLE))
        / NULLIF(SUM(GREATEST(COALESCE(c.annual_incremental_sales, CAST(0 AS DOUBLE)), CAST(0 AS DOUBLE)))
                 OVER (PARTITION BY c.tpnb), 0) AS product_occasion_weight,
    100.0 * (c.annual_incremental_sales / NULLIF(c.occasion_annual_incremental_sales, 0))
        AS share_of_occasion_incremental_pct
FROM combined c;


-- =====================================================================================
-- 24  FINAL v2.6 THEME OUTPUT  (one row per TPNB)
--     Annual recurring occasions only. Periodic occurrence responses remain separate.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_6_theme_output;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_6_theme_output
USING PARQUET AS
WITH profile AS (
    SELECT * FROM lab_customer_value_analytics.seasonality_v2_6_product_occasion_profile
),

ranked AS (
    SELECT
        p.*,
        ROW_NUMBER() OVER (
            PARTITION BY p.tpnb
            ORDER BY p.annual_incremental_sales DESC NULLS LAST,
                     p.occasion_importance_weight DESC NULLS LAST,
                     p.occasion_key
        ) AS occasion_rank
    FROM profile p
),

seasonal_lists AS (
    SELECT
        tpnb,
        COUNT(*) AS seasonal_occasion_count,
        COUNT(DISTINCT occasion_family) AS seasonal_family_count,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(CASE WHEN production_occasion_verdict = 'Specifically seasonal' THEN occasion_name END)))
            AS specifically_seasonal_occasions,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(CASE WHEN production_occasion_verdict = 'Uplift seasonal' THEN occasion_name END)))
            AS uplift_seasonal_occasions,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(CASE WHEN timing_type IN ('Build-up and event', 'Build-up only') THEN occasion_name END)))
            AS buildup_occasions,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(CASE WHEN timing_type = 'Event only' THEN occasion_name END)))
            AS event_only_occasions,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(occasion_family))) AS seasonal_family_list,
        SUM(GREATEST(COALESCE(annual_incremental_sales, CAST(0 AS DOUBLE)), CAST(0 AS DOUBLE)))
            AS occasion_incremental_sales_per_year
    FROM profile
    GROUP BY tpnb
),

primary_occasion AS (
    SELECT * FROM ranked WHERE occasion_rank = 1
),

secondary_occasion AS (
    SELECT
        tpnb,
        occasion_name AS secondary_occasion,
        product_occasion_weight AS secondary_occasion_weight
    FROM ranked
    WHERE occasion_rank = 2
),

price_drop_totals AS (
    SELECT tpnb, SUM(price_drop_sales) AS observed_price_drop_period_sales
    FROM lab_customer_value_analytics.seasonality_v2_6_price_drop_periods
    GROUP BY tpnb
),

base AS (
    SELECT
        s.*,
        (s.sales_2023 + s.sales_2024 + s.sales_2025)
            / NULLIF(CASE WHEN s.sales_2023 > 0 THEN 1 ELSE 0 END
                   + CASE WHEN s.sales_2024 > 0 THEN 1 ELSE 0 END
                   + CASE WHEN s.sales_2025 > 0 THEN 1 ELSE 0 END, 0) AS avg_annual_sales
    FROM lab_customer_value_analytics.seasonality_v2_6_tpnb_summary s
)

SELECT
    b.tpnb,
    b.product_description,
    b.hierarchy_department_name,
    b.hierarchy_section_name,
    b.hierarchy_class_name,
    b.hierarchy_subclass_code AS subcategory_code,
    b.hierarchy_subclass_name AS subcategory_name,

    CASE
        WHEN b.resolved_product_seasonality_v2_6 IN ('Specifically seasonal', 'Uplift seasonal')
            THEN CONCAT(COALESCE(po.occasion_name, 'Seasonal'),
                        CASE WHEN po.timing_type IS NOT NULL THEN CONCAT(' (', LOWER(po.timing_type), ')') ELSE '' END)
        WHEN b.resolved_product_seasonality_v2_6 = 'Occasion-suppressed'
            THEN CONCAT('Everyday, dips at ', COALESCE(NULLIF(b.suppressed_occasion_list, ''), 'an occasion'))
        WHEN b.resolved_product_seasonality_v2_6 = 'Regular' THEN 'Everyday'
        WHEN b.resolved_product_seasonality_v2_6 = 'Review required' THEN 'Review required'
        ELSE 'Not enough evidence'
    END AS theme_label,
    b.resolved_product_seasonality_v2_6 AS product_seasonality,
    CASE
        WHEN COALESCE(b.accepted_annual_occasion_count_v2_6, 0) > 0 THEN 'V2_6_FINAL_ANNUAL_POLICY'
        WHEN COALESCE(b.held_annual_occasion_count_v2_6, 0) > 0 THEN 'V2_6_REVIEW_QUEUE'
        ELSE COALESCE(b.seasonality_evidence_source, 'NONE')
    END AS seasonality_evidence_source,

    CASE WHEN COALESCE(sl.specifically_seasonal_occasions, '') <> '' THEN 'Y' ELSE 'N' END AS specifically_seasonal_flag,
    CASE WHEN COALESCE(sl.uplift_seasonal_occasions, '') <> '' THEN 'Y' ELSE 'N' END AS uplift_seasonal_flag,
    CASE WHEN b.resolved_product_seasonality_v2_6 = 'Regular' THEN 'Y' ELSE 'N' END AS regular_flag,
    b.occasion_suppressed_flag,
    b.post_event_uplift_flag,
    b.limited_edition_flag,
    CASE WHEN COALESCE(sl.buildup_occasions, '') <> '' THEN 'Y' ELSE 'N' END AS buildup_flag,
    CASE WHEN po.shopping_start_days_before IS NOT NULL
          AND po.shopping_start_days_before <= po.event_window_days_before THEN 'Y' ELSE 'N' END AS last_minute_flag,
    CASE WHEN COALESCE(sl.seasonal_family_count, 0) >= 2 THEN 'Y' ELSE 'N' END AS multi_occasion_flag,
    CASE WHEN po.price_drop_alignment = 'Customers start first' THEN 'Y' ELSE 'N' END AS customers_ahead_of_price_drop_flag,
    COALESCE(po.observed_range_starts_after_customer_flag, 'N') AS observed_range_starts_after_customer_flag,

    COALESCE(sl.specifically_seasonal_occasions, '') AS specifically_seasonal_occasions,
    COALESCE(sl.uplift_seasonal_occasions, '') AS uplift_seasonal_occasions,
    COALESCE(sl.buildup_occasions, '') AS buildup_occasions,
    COALESCE(sl.event_only_occasions, '') AS event_only_occasions,
    COALESCE(b.suppressed_occasion_list, '') AS suppressed_occasions,
    COALESCE(b.post_event_uplift_occasion_list, '') AS post_event_uplift_occasions,
    COALESCE(sl.seasonal_family_list, '') AS seasonal_family_list,
    COALESCE(sl.seasonal_occasion_count, 0) AS seasonal_occasion_count,
    b.held_annual_occasion_count_v2_6,
    b.held_annual_occasion_list_v2_6,

    po.occasion_key AS primary_occasion_key,
    po.occasion_name AS primary_occasion,
    po.occasion_family AS primary_occasion_family,
    po.production_occasion_verdict AS primary_occasion_verdict,
    po.evidence_occasion_verdict AS primary_occasion_evidence_verdict,
    po.timing_type AS primary_occasion_timing_type,
    po.evidence_source AS primary_occasion_evidence_source,
    po.final_annual_policy_reason AS primary_occasion_policy_reason,
    po.matched_affinity_patterns AS primary_occasion_affinity_patterns,
    100.0 * po.product_occasion_weight AS primary_occasion_weight_pct,
    so.secondary_occasion,
    100.0 * so.secondary_occasion_weight AS secondary_occasion_weight_pct,
    100.0 * po.occasion_importance_weight AS primary_occasion_importance_pct,
    po.occasion_importance_tier AS primary_occasion_tier,
    sl.occasion_incremental_sales_per_year,
    CASE
        WHEN COALESCE(sl.seasonal_occasion_count, 0) > 0 AND b.avg_annual_sales > 0
            THEN LEAST(CAST(100 AS DOUBLE), 100.0 * (sl.occasion_incremental_sales_per_year / b.avg_annual_sales))
        WHEN b.resolved_product_seasonality_v2_6 IN ('Regular', 'Occasion-suppressed') THEN CAST(0 AS DOUBLE)
    END AS occasion_dependency_pct,

    po.shopping_start_days_before,
    po.peak_days_before AS shopping_peak_days_before,
    po.shopping_end_days_before,
    po.shopping_window_days,
    po.timing_vs_occasion,
    po.occasion_shopping_start_days_before,
    po.observed_range_start_days_before,
    po.range_timing_source,

    po.price_drop_start_days_before,
    po.customers_ahead_of_price_drop_days,
    po.price_drop_alignment,
    po.window_price_drop_share_pct AS occasion_sales_during_observed_price_drop_pct,
    100.0 * (pd.observed_price_drop_period_sales / NULLIF(b.total_sales, 0)) AS annual_sales_during_observed_price_drop_pct,
    po.price_drop_timing_source,

    b.time_of_year_pattern,
    b.peak_season,
    b.peak_weeks,
    b.best_8_week_share_pct,
    b.day_of_week_pattern,
    b.availability_pattern,

    b.avg_annual_sales,
    b.total_sales,
    b.sales_2023,
    b.sales_2024,
    b.sales_2025,
    b.sales_2026_ytd,
    100.0 * (b.sales_2025 / NULLIF(b.sales_2024, 0) - 1.0) AS sales_change_2025_vs_2024_pct,
    CASE
        WHEN b.sales_2024 > 0 AND b.sales_2025 > 0 AND b.sales_2025 >= 1.15 * b.sales_2024 THEN 'Growing'
        WHEN b.sales_2024 > 0 AND b.sales_2025 > 0 AND b.sales_2025 <= 0.85 * b.sales_2024 THEN 'Declining'
        WHEN b.sales_2024 > 0 AND b.sales_2025 > 0 THEN 'Stable'
        ELSE 'Not enough history'
    END AS sales_trend,
    b.total_baskets,
    b.first_sale_date,
    b.last_sale_date
FROM base b
LEFT JOIN seasonal_lists sl
  ON b.tpnb = sl.tpnb
LEFT JOIN primary_occasion po
  ON b.tpnb = po.tpnb
LEFT JOIN secondary_occasion so
  ON b.tpnb = so.tpnb
LEFT JOIN price_drop_totals pd
  ON b.tpnb = pd.tpnb;

-- =====================================================================================
-- PART H - PERIODIC EVENTS: MATCHED-CALENDAR CONTROL (v2.4)
-- =====================================================================================
-- =====================================================================================
-- SEASONALITY THEMES v2.4 PERIODIC-EVENT MATCHED-CALENDAR CONTROL
-- Spark SQL / Databricks / Kyuubi compatible. No QUALIFY.
--
-- Purpose
-- -------
-- Re-estimate one-off and periodic events (FIFA, Olympics, the Ashes, etc.) against the
-- same calendar period in non-event years. This prevents Christmas, summer, range launches
-- and other recurring seasonal patterns from being credited to a tournament merely because
-- the dates overlap.
--
-- This output is occurrence-specific evidence only. It must not become recurring product
-- seasonality.
-- =====================================================================================


DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_4_periodic_params;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_4_periodic_params
USING PARQUET AS
SELECT
    CAST(2 AS INT)       AS min_control_years,
    CAST(0.50 AS DOUBLE) AS min_on_sale_day_coverage,
    CAST(0.80 AS DOUBLE) AS min_matched_event_day_coverage,
    CAST(100.0 AS DOUBLE) AS min_expected_baskets,
    CAST(1.50 AS DOUBLE) AS min_response_index,
    CAST(5.0 AS DOUBLE)  AS min_response_z,
    CAST(100.0 AS DOUBLE) AS min_incremental_baskets,
    CAST(1000.0 AS DOUBLE) AS min_incremental_sales;


DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_4_periodic_matched_response;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_4_periodic_matched_response
USING PARQUET AS
WITH complete_years AS (
    SELECT cal_year
    FROM lab_customer_value_analytics.seasonality_v2_calendar_days
    GROUP BY cal_year
    HAVING MIN(cal_date) = MAKE_DATE(cal_year, 1, 1)
       AND MAX(cal_date) = MAKE_DATE(cal_year, 12, 31)
),

periodic_occurrences AS (
    SELECT
        o.occurrence_id,
        o.parent_occasion_key,
        o.occasion_name AS parent_occasion_name,
        o.event_start_date,
        o.event_end_date,
        DATEDIFF(o.event_end_date, o.event_start_date) + 1 AS event_days
    FROM lab_customer_value_analytics.seasonality_v2_occurrences o
    JOIN lab_customer_value_analytics.seasonality_v2_occasion_config g
      ON o.parent_occasion_key = g.occasion_key
    WHERE o.window_type = 'EVENT'
      AND o.is_completed = 'Y'
      AND g.recurrence_per_year < 1.0
),

event_dates AS (
    SELECT
        o.*,
        x.event_date,
        DAYOFWEEK(x.event_date) AS event_weekday
    FROM periodic_occurrences o
    LATERAL VIEW EXPLODE(SEQUENCE(o.event_start_date, o.event_end_date)) x AS event_date
),

control_date_candidates AS (
    SELECT
        e.occurrence_id,
        e.parent_occasion_key,
        e.parent_occasion_name,
        e.event_start_date,
        e.event_end_date,
        e.event_days,
        e.event_date,
        e.event_weekday,
        y.cal_year AS control_year,
        DATE_ADD(MAKE_DATE(y.cal_year, MONTH(e.event_date), DAY(e.event_date)), off.day_offset) AS control_date,
        ABS(off.day_offset) AS abs_offset
    FROM event_dates e
    CROSS JOIN complete_years y
    LATERAL VIEW EXPLODE(SEQUENCE(-3, 3)) off AS day_offset
    WHERE y.cal_year <> YEAR(e.event_date)
),

control_date_ranked AS (
    SELECT
        c.*,
        ROW_NUMBER() OVER (
            PARTITION BY c.occurrence_id, c.event_date, c.control_year
            ORDER BY c.abs_offset, c.control_date
        ) AS date_rn
    FROM control_date_candidates c
    WHERE YEAR(c.control_date) = c.control_year
      AND DAYOFWEEK(c.control_date) = c.event_weekday
),

control_date_map AS (
    SELECT
        occurrence_id,
        parent_occasion_key,
        parent_occasion_name,
        event_start_date,
        event_end_date,
        event_days,
        event_date,
        control_year,
        control_date
    FROM control_date_ranked
    WHERE date_rn = 1
),

event_products AS (
    SELECT
        e.occurrence_id,
        MAX(e.parent_occasion_key) AS parent_occasion_key,
        MAX(e.parent_occasion_name) AS parent_occasion_name,
        MAX(e.event_start_date) AS event_start_date,
        MAX(e.event_end_date) AS event_end_date,
        MAX(e.event_days) AS event_days,
        d.entity_id AS tpnb
    FROM event_dates e
    JOIN lab_customer_value_analytics.seasonality_v2_entity_daily d
      ON e.event_date = d.sale_date
     AND d.entity_level = 'TPNB'
    GROUP BY e.occurrence_id, d.entity_id
),

candidate_tpnbs AS (
    SELECT DISTINCT tpnb FROM event_products
),

spells AS (
    SELECT
        s.entity_id AS tpnb,
        COLLECT_LIST(NAMED_STRUCT('s', s.spell_start, 'e', s.spell_end)) AS spells
    FROM lab_customer_value_analytics.seasonality_v2_availability_spells s
    JOIN candidate_tpnbs t ON s.entity_id = t.tpnb
    WHERE s.entity_level = 'TPNB'
    GROUP BY s.entity_id
),

event_grid AS (
    SELECT
        p.occurrence_id,
        p.parent_occasion_key,
        p.parent_occasion_name,
        p.event_start_date,
        p.event_end_date,
        p.event_days,
        p.tpnb,
        e.event_date,
        c.all_items AS event_all_items,
        COALESCE(d.baskets, 0) AS event_baskets,
        COALESCE(d.sales, CAST(0 AS DOUBLE)) AS event_sales,
        COALESCE(EXISTS(s.spells, sp -> e.event_date BETWEEN sp.s AND sp.e), FALSE) AS on_sale_event
    FROM event_products p
    JOIN event_dates e ON p.occurrence_id = e.occurrence_id
    JOIN lab_customer_value_analytics.seasonality_v2_calendar_days c ON e.event_date = c.cal_date
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_entity_daily d
      ON p.tpnb = d.entity_id
     AND d.entity_level = 'TPNB'
     AND e.event_date = d.sale_date
    LEFT JOIN spells s ON p.tpnb = s.tpnb
),

control_grid AS (
    SELECT
        p.occurrence_id,
        p.parent_occasion_key,
        p.parent_occasion_name,
        p.event_days,
        p.tpnb,
        m.event_date,
        m.control_year,
        m.control_date,
        c.all_items AS control_all_items,
        COALESCE(d.baskets, 0) AS control_baskets,
        COALESCE(d.sales, CAST(0 AS DOUBLE)) AS control_sales,
        COALESCE(EXISTS(s.spells, sp -> m.control_date BETWEEN sp.s AND sp.e), FALSE) AS on_sale_control
    FROM event_products p
    JOIN control_date_map m ON p.occurrence_id = m.occurrence_id
    JOIN lab_customer_value_analytics.seasonality_v2_calendar_days c ON m.control_date = c.cal_date
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_entity_daily d
      ON p.tpnb = d.entity_id
     AND d.entity_level = 'TPNB'
     AND m.control_date = d.sale_date
    LEFT JOIN spells s ON p.tpnb = s.tpnb
),

control_year_coverage AS (
    SELECT
        occurrence_id,
        tpnb,
        control_year,
        MAX(event_days) AS event_days,
        SUM(CASE WHEN on_sale_control THEN 1 ELSE 0 END) AS on_sale_control_days
    FROM control_grid
    GROUP BY occurrence_id, tpnb, control_year
),

eligible_control_years AS (
    SELECT c.occurrence_id, c.tpnb, c.control_year
    FROM control_year_coverage c
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_4_periodic_params p
    WHERE c.on_sale_control_days >= p.min_on_sale_day_coverage * c.event_days
),

control_daily AS (
    SELECT
        g.occurrence_id,
        g.tpnb,
        g.event_date,
        COUNT(DISTINCT g.control_year) AS control_years_for_date,
        AVG(CASE WHEN g.on_sale_control
                 THEN COALESCE(g.control_baskets, 0) / NULLIF(g.control_all_items, 0) END)
            AS mean_control_item_share,
        SUM(CASE WHEN g.on_sale_control THEN g.control_sales ELSE CAST(0 AS DOUBLE) END)
            / NULLIF(SUM(CASE WHEN g.on_sale_control THEN g.control_baskets ELSE 0 END), 0)
            AS control_sales_per_basket
    FROM control_grid g
    JOIN eligible_control_years y
      ON g.occurrence_id = y.occurrence_id
     AND g.tpnb = y.tpnb
     AND g.control_year = y.control_year
    GROUP BY g.occurrence_id, g.tpnb, g.event_date
),

control_year_count AS (
    SELECT occurrence_id, tpnb, COUNT(DISTINCT control_year) AS eligible_control_years
    FROM eligible_control_years
    GROUP BY occurrence_id, tpnb
),

event_daily_expected AS (
    SELECT
        e.*,
        c.control_years_for_date,
        c.mean_control_item_share,
        c.control_sales_per_basket,
        e.event_all_items * c.mean_control_item_share AS expected_baskets,
        e.event_all_items * c.mean_control_item_share * c.control_sales_per_basket AS expected_sales
    FROM event_grid e
    LEFT JOIN control_daily c
      ON e.occurrence_id = c.occurrence_id
     AND e.tpnb = c.tpnb
     AND e.event_date = c.event_date
),

rolled AS (
    SELECT
        e.occurrence_id,
        MAX(e.parent_occasion_key) AS parent_occasion_key,
        MAX(e.parent_occasion_name) AS parent_occasion_name,
        MAX(e.event_start_date) AS event_start_date,
        MAX(e.event_end_date) AS event_end_date,
        MAX(e.event_days) AS event_days,
        e.tpnb,
        SUM(CASE WHEN e.on_sale_event THEN 1 ELSE 0 END) AS event_on_sale_days,
        SUM(CASE WHEN e.mean_control_item_share IS NOT NULL THEN 1 ELSE 0 END) AS matched_event_days,
        SUM(e.event_baskets) AS event_baskets,
        SUM(e.expected_baskets) AS expected_baskets,
        SUM(e.event_sales) AS event_sales,
        SUM(e.expected_sales) AS expected_sales
    FROM event_daily_expected e
    GROUP BY e.occurrence_id, e.tpnb
),

metrics AS (
    SELECT
        r.*,
        COALESCE(c.eligible_control_years, 0) AS eligible_control_years,
        r.event_baskets / NULLIF(r.expected_baskets, 0) AS matched_calendar_index,
        (r.event_baskets - r.expected_baskets) / NULLIF(SQRT(r.expected_baskets), 0) AS matched_calendar_z,
        r.event_baskets - r.expected_baskets AS incremental_baskets,
        r.event_sales - r.expected_sales AS incremental_sales,
        r.event_on_sale_days / NULLIF(CAST(r.event_days AS DOUBLE), 0) AS event_on_sale_day_coverage,
        r.matched_event_days / NULLIF(CAST(r.event_days AS DOUBLE), 0) AS matched_event_day_coverage
    FROM rolled r
    LEFT JOIN control_year_count c
      ON r.occurrence_id = c.occurrence_id
     AND r.tpnb = c.tpnb
)

SELECT
    m.*,
    CASE
        WHEN m.eligible_control_years < p.min_control_years THEN 'INSUFFICIENT_CONTROL_YEARS'
        WHEN m.event_on_sale_day_coverage < p.min_on_sale_day_coverage THEN 'INSUFFICIENT_EVENT_AVAILABILITY'
        WHEN m.matched_event_day_coverage < p.min_matched_event_day_coverage THEN 'INSUFFICIENT_MATCHED_DAYS'
        WHEN m.expected_baskets < p.min_expected_baskets THEN 'LOW_EXPECTED_VOLUME'
        WHEN m.matched_calendar_index >= p.min_response_index
         AND m.matched_calendar_z >= p.min_response_z
         AND m.incremental_baskets >= p.min_incremental_baskets
         AND m.incremental_sales >= p.min_incremental_sales
            THEN 'MATERIAL_MATCHED_CALENDAR_RESPONSE'
        ELSE 'NO_MATERIAL_MATCHED_CALENDAR_RESPONSE'
    END AS matched_periodic_response_status
FROM metrics m
CROSS JOIN lab_customer_value_analytics.seasonality_v2_4_periodic_params p;

-- =====================================================================================
-- PART I - PERIODIC EVENTS: PRE-EVENT TREND ADJUSTMENT (v2.5)
-- =====================================================================================
-- =====================================================================================
-- SEASONALITY THEMES v2.5 PERIODIC-EVENT TREND-ADJUSTED MATCHED CONTROL
-- Spark SQL / Databricks / Kyuubi compatible. No QUALIFY.
--
-- Purpose
-- -------
-- Add a pre-event difference-in-differences style calibration to the v2.4 matched-calendar
-- periodic response. The v2.4 model removes recurring calendar seasonality; this overlay also
-- removes product-level year-on-year growth, decline and range expansion visible before the
-- event. Output remains occurrence-specific and must not become recurring seasonality.
--
-- Prerequisite
--   seasonality_v2_4_periodic_matched_control.sql
-- =====================================================================================


DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_5_periodic_params;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_5_periodic_params
USING PARQUET AS
SELECT
    CAST(28 AS INT)      AS pre_calibration_days,
    CAST(7 AS INT)       AS pre_gap_days,
    CAST(2 AS INT)       AS min_pre_control_years,
    CAST(0.50 AS DOUBLE) AS min_pre_on_sale_day_coverage,
    CAST(0.80 AS DOUBLE) AS min_pre_matched_day_coverage,
    CAST(100.0 AS DOUBLE) AS min_pre_expected_baskets,
    CAST(0.50 AS DOUBLE) AS min_usable_trend_factor,
    CAST(2.00 AS DOUBLE) AS max_usable_trend_factor,
    CAST(1.50 AS DOUBLE) AS min_response_index,
    CAST(5.0 AS DOUBLE)  AS min_response_z,
    CAST(100.0 AS DOUBLE) AS min_incremental_baskets,
    CAST(1000.0 AS DOUBLE) AS min_incremental_sales;


DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_5_periodic_trend_response;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_5_periodic_trend_response
USING PARQUET AS
WITH candidates AS (
    SELECT
        r.*,
        DATE_SUB(r.event_start_date, p.pre_gap_days + p.pre_calibration_days) AS pre_start_date,
        DATE_SUB(r.event_start_date, p.pre_gap_days + 1) AS pre_end_date
    FROM lab_customer_value_analytics.seasonality_v2_4_periodic_matched_response r
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_5_periodic_params p
    WHERE r.matched_periodic_response_status = 'MATERIAL_MATCHED_CALENDAR_RESPONSE'
),

complete_years AS (
    SELECT cal_year
    FROM lab_customer_value_analytics.seasonality_v2_calendar_days
    GROUP BY cal_year
    HAVING MIN(cal_date) = MAKE_DATE(cal_year, 1, 1)
       AND MAX(cal_date) = MAKE_DATE(cal_year, 12, 31)
),

candidate_tpnbs AS (
    SELECT DISTINCT tpnb FROM candidates
),

spells AS (
    SELECT
        s.entity_id AS tpnb,
        COLLECT_LIST(NAMED_STRUCT('s', s.spell_start, 'e', s.spell_end)) AS spells
    FROM lab_customer_value_analytics.seasonality_v2_availability_spells s
    JOIN candidate_tpnbs t
      ON s.entity_id = t.tpnb
    WHERE s.entity_level = 'TPNB'
    GROUP BY s.entity_id
),

pre_dates AS (
    SELECT
        c.occurrence_id,
        c.tpnb,
        c.parent_occasion_key,
        c.parent_occasion_name,
        c.event_start_date,
        c.event_end_date,
        c.pre_start_date,
        c.pre_end_date,
        x.pre_date,
        DAYOFWEEK(x.pre_date) AS pre_weekday
    FROM candidates c
    LATERAL VIEW EXPLODE(SEQUENCE(c.pre_start_date, c.pre_end_date)) x AS pre_date
),

control_candidates AS (
    SELECT
        d.*,
        y.cal_year AS control_year,
        DATE_ADD(MAKE_DATE(y.cal_year, MONTH(d.pre_date), DAY(d.pre_date)), off.day_offset) AS control_date,
        ABS(off.day_offset) AS abs_offset
    FROM pre_dates d
    CROSS JOIN complete_years y
    LATERAL VIEW EXPLODE(SEQUENCE(-3, 3)) off AS day_offset
    WHERE y.cal_year <> YEAR(d.pre_date)
),

control_ranked AS (
    SELECT
        c.*,
        ROW_NUMBER() OVER (
            PARTITION BY c.occurrence_id, c.tpnb, c.pre_date, c.control_year
            ORDER BY c.abs_offset, c.control_date
        ) AS date_rn
    FROM control_candidates c
    WHERE YEAR(c.control_date) = c.control_year
      AND DAYOFWEEK(c.control_date) = c.pre_weekday
),

control_map AS (
    SELECT
        occurrence_id,
        tpnb,
        pre_date,
        control_year,
        control_date
    FROM control_ranked
    WHERE date_rn = 1
),

current_pre AS (
    SELECT
        d.occurrence_id,
        d.tpnb,
        d.pre_date,
        c.all_items,
        COALESCE(e.baskets, 0) AS baskets,
        COALESCE(e.sales, CAST(0 AS DOUBLE)) AS sales,
        COALESCE(EXISTS(s.spells, sp -> d.pre_date BETWEEN sp.s AND sp.e), FALSE) AS on_sale
    FROM pre_dates d
    JOIN lab_customer_value_analytics.seasonality_v2_calendar_days c
      ON d.pre_date = c.cal_date
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_entity_daily e
      ON d.tpnb = e.entity_id
     AND e.entity_level = 'TPNB'
     AND d.pre_date = e.sale_date
    LEFT JOIN spells s
      ON d.tpnb = s.tpnb
),

control_pre AS (
    SELECT
        m.occurrence_id,
        m.tpnb,
        m.pre_date,
        m.control_year,
        m.control_date,
        c.all_items,
        COALESCE(e.baskets, 0) AS baskets,
        COALESCE(e.sales, CAST(0 AS DOUBLE)) AS sales,
        COALESCE(EXISTS(s.spells, sp -> m.control_date BETWEEN sp.s AND sp.e), FALSE) AS on_sale
    FROM control_map m
    JOIN lab_customer_value_analytics.seasonality_v2_calendar_days c
      ON m.control_date = c.cal_date
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_entity_daily e
      ON m.tpnb = e.entity_id
     AND e.entity_level = 'TPNB'
     AND m.control_date = e.sale_date
    LEFT JOIN spells s
      ON m.tpnb = s.tpnb
),

control_year_coverage AS (
    SELECT
        occurrence_id,
        tpnb,
        control_year,
        COUNT(*) AS mapped_days,
        SUM(CASE WHEN on_sale THEN 1 ELSE 0 END) AS on_sale_days
    FROM control_pre
    GROUP BY occurrence_id, tpnb, control_year
),

eligible_control_years AS (
    SELECT y.occurrence_id, y.tpnb, y.control_year
    FROM control_year_coverage y
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_5_periodic_params p
    WHERE y.on_sale_days >= p.min_pre_on_sale_day_coverage * p.pre_calibration_days
),

control_daily AS (
    SELECT
        g.occurrence_id,
        g.tpnb,
        g.pre_date,
        COUNT(DISTINCT g.control_year) AS control_years_for_date,
        AVG(CASE WHEN g.on_sale THEN g.baskets / NULLIF(g.all_items, 0) END) AS mean_control_item_share,
        SUM(CASE WHEN g.on_sale THEN g.sales ELSE CAST(0 AS DOUBLE) END)
            / NULLIF(SUM(CASE WHEN g.on_sale THEN g.baskets ELSE 0 END), 0) AS control_sales_per_basket
    FROM control_pre g
    JOIN eligible_control_years y
      ON g.occurrence_id = y.occurrence_id
     AND g.tpnb = y.tpnb
     AND g.control_year = y.control_year
    GROUP BY g.occurrence_id, g.tpnb, g.pre_date
),

pre_daily_expected AS (
    SELECT
        p.occurrence_id,
        p.tpnb,
        p.pre_date,
        p.on_sale,
        p.baskets,
        p.sales,
        c.control_years_for_date,
        p.all_items * c.mean_control_item_share AS expected_baskets,
        p.all_items * c.mean_control_item_share * c.control_sales_per_basket AS expected_sales
    FROM current_pre p
    LEFT JOIN control_daily c
      ON p.occurrence_id = c.occurrence_id
     AND p.tpnb = c.tpnb
     AND p.pre_date = c.pre_date
),

pre_rolled AS (
    SELECT
        occurrence_id,
        tpnb,
        COUNT(*) AS pre_days,
        SUM(CASE WHEN on_sale THEN 1 ELSE 0 END) AS pre_on_sale_days,
        SUM(CASE WHEN expected_baskets IS NOT NULL THEN 1 ELSE 0 END) AS pre_matched_days,
        SUM(baskets) AS pre_baskets,
        SUM(expected_baskets) AS pre_expected_baskets,
        SUM(sales) AS pre_sales,
        SUM(expected_sales) AS pre_expected_sales,
        MAX(control_years_for_date) AS max_pre_control_years,
        MIN(control_years_for_date) AS min_pre_control_years_observed
    FROM pre_daily_expected
    GROUP BY occurrence_id, tpnb
),

features AS (
    SELECT
        r.*,
        p.pre_days,
        p.pre_on_sale_days,
        p.pre_matched_days,
        p.pre_baskets,
        p.pre_expected_baskets,
        p.pre_sales,
        p.pre_expected_sales,
        p.max_pre_control_years,
        p.min_pre_control_years_observed,
        p.pre_baskets / NULLIF(p.pre_expected_baskets, 0) AS pre_basket_trend_factor,
        p.pre_sales / NULLIF(p.pre_expected_sales, 0) AS pre_sales_trend_factor,
        p.pre_on_sale_days / NULLIF(CAST(p.pre_days AS DOUBLE), 0) AS pre_on_sale_day_coverage,
        p.pre_matched_days / NULLIF(CAST(p.pre_days AS DOUBLE), 0) AS pre_matched_day_coverage,
        r.expected_baskets * (p.pre_baskets / NULLIF(p.pre_expected_baskets, 0)) AS trend_adjusted_expected_baskets,
        r.expected_sales * (p.pre_sales / NULLIF(p.pre_expected_sales, 0)) AS trend_adjusted_expected_sales
    FROM candidates r
    LEFT JOIN pre_rolled p
      ON r.occurrence_id = p.occurrence_id
     AND r.tpnb = p.tpnb
),

metrics AS (
    SELECT
        f.*,
        f.event_baskets / NULLIF(f.trend_adjusted_expected_baskets, 0) AS trend_adjusted_index,
        (f.event_baskets - f.trend_adjusted_expected_baskets)
            / NULLIF(SQRT(f.trend_adjusted_expected_baskets), 0) AS trend_adjusted_z,
        f.event_baskets - f.trend_adjusted_expected_baskets AS trend_adjusted_incremental_baskets,
        f.event_sales - f.trend_adjusted_expected_sales AS trend_adjusted_incremental_sales
    FROM features f
)

SELECT
    m.*,
    CASE
        WHEN UPPER(COALESCE(d.hierarchy_division_name, '')) = 'MISCELLANEOUS ITEMS'
          OR UPPER(COALESCE(d.hierarchy_subclass_name, '')) LIKE '%DELIVERY%'
          OR UPPER(COALESCE(d.hierarchy_subclass_name, '')) LIKE '%SURCHARGE%'
          OR UPPER(COALESCE(d.hierarchy_subclass_name, '')) LIKE '%GARAGE%'
          OR UPPER(COALESCE(d.hierarchy_subclass_name, '')) LIKE '%FUEL%'
          OR UPPER(COALESCE(d.hierarchy_subclass_name, '')) LIKE '%LOTTERY%'
            THEN 'NON_MERCHANDISE_OR_SERVICE'
        ELSE 'MERCHANDISE'
    END AS response_scope,
    CASE
        WHEN COALESCE(m.min_pre_control_years_observed, 0) < p.min_pre_control_years
            THEN 'INSUFFICIENT_PRE_CONTROL_YEARS'
        WHEN COALESCE(m.pre_on_sale_day_coverage, 0) < p.min_pre_on_sale_day_coverage
            THEN 'INSUFFICIENT_PRE_EVENT_AVAILABILITY'
        WHEN COALESCE(m.pre_matched_day_coverage, 0) < p.min_pre_matched_day_coverage
            THEN 'INSUFFICIENT_PRE_MATCHED_DAYS'
        WHEN COALESCE(m.pre_expected_baskets, 0) < p.min_pre_expected_baskets
            THEN 'LOW_PRE_EXPECTED_VOLUME'
        WHEN m.pre_basket_trend_factor < p.min_usable_trend_factor
          OR m.pre_basket_trend_factor > p.max_usable_trend_factor
          OR m.pre_sales_trend_factor < p.min_usable_trend_factor
          OR m.pre_sales_trend_factor > p.max_usable_trend_factor
            THEN 'UNSTABLE_PRE_EVENT_TREND'
        WHEN m.trend_adjusted_index >= p.min_response_index
         AND m.trend_adjusted_z >= p.min_response_z
         AND m.trend_adjusted_incremental_baskets >= p.min_incremental_baskets
         AND m.trend_adjusted_incremental_sales >= p.min_incremental_sales
            THEN 'MATERIAL_TREND_ADJUSTED_RESPONSE'
        ELSE 'NO_MATERIAL_TREND_ADJUSTED_RESPONSE'
    END AS trend_adjusted_response_status,
    d.product_description,
    d.hierarchy_division_name,
    d.hierarchy_subclass_name
FROM metrics m
CROSS JOIN lab_customer_value_analytics.seasonality_v2_5_periodic_params p
LEFT JOIN lab_customer_value_analytics.seasonality_v2_product_dim d
  ON TRY_CAST(m.tpnb AS BIGINT) = d.tpnb;

-- =====================================================================================
-- PART J - PERIODIC CONFIDENCE, INITIAL PRODUCT/SUBCLASS DOMINANCE (v2.6 Kyuubi-fixed)
-- =====================================================================================
-- =====================================================================================
-- SEASONALITY THEMES v2.6 PERIODIC-EVENT CONFIDENCE LAYER
-- Spark SQL / Databricks / Kyuubi compatible. No QUALIFY.
--
-- Purpose
-- -------
-- Convert the v2.5 trend-adjusted periodic-event results into a conservative reporting
-- layer. Periodic events remain occurrence-specific and are never used as recurring
-- seasonality evidence. The layer rewards:
--   * a material trend-adjusted response;
--   * concentration on one or two periodic events for the product;
--   * corroboration from the product's subclass;
--   * a plausible viewing / celebration mission.
--
-- It does not claim causality. HIGH rows are product-level candidates; MEDIUM rows are
-- suitable for category-level reporting; REVIEW rows remain diagnostic only.
-- =====================================================================================

DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_6_periodic_params;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_6_periodic_params
USING PARQUET AS
SELECT
    CAST(0.40 AS DOUBLE) AS high_product_event_share_min,
    CAST(0.25 AS DOUBLE) AS high_subclass_event_share_min,
    CAST(2 AS INT)       AS high_rank_max,
    CAST(2 AS INT)       AS high_positive_event_count_max,
    CAST(0.25 AS DOUBLE) AS medium_product_event_share_min,
    CAST(0.15 AS DOUBLE) AS medium_subclass_event_share_min,
    CAST(3 AS INT)       AS medium_rank_max,
    CAST(3 AS INT)       AS medium_positive_event_count_max;


DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_6_periodic_confidence;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_6_periodic_confidence
USING PARQUET AS
WITH material AS (
    SELECT
        r.*,
        -- The v2.5 trend-response table keeps the occurrence dates but not an event_year
        -- column. Use the start year as the occurrence year; this also keeps a cross-year
        -- event such as The Ashes 2025-26 labelled as the 2025 occurrence.
        YEAR(r.event_start_date) AS event_year,
        d.hierarchy_subclass_code,
        UPPER(CONCAT_WS(
            ' ',
            COALESCE(r.product_description, ''),
            COALESCE(r.hierarchy_division_name, ''),
            COALESCE(r.hierarchy_subclass_name, '')
        )) AS affinity_text
    FROM lab_customer_value_analytics.seasonality_v2_5_periodic_trend_response r
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_product_dim d
      ON TRY_CAST(r.tpnb AS BIGINT) = d.tpnb
    WHERE r.trend_adjusted_response_status = 'MATERIAL_TREND_ADJUSTED_RESPONSE'
      AND r.response_scope = 'MERCHANDISE'
),

product_roll AS (
    SELECT
        tpnb,
        SUM(GREATEST(COALESCE(trend_adjusted_incremental_sales, CAST(0 AS DOUBLE)), CAST(0 AS DOUBLE))) AS product_total_positive_periodic_sales,
        COUNT(*) AS product_positive_periodic_events
    FROM material
    GROUP BY tpnb
),

product_ranked AS (
    SELECT
        m.*,
        p.product_total_positive_periodic_sales,
        p.product_positive_periodic_events,
        GREATEST(COALESCE(m.trend_adjusted_incremental_sales, CAST(0 AS DOUBLE)), CAST(0 AS DOUBLE))
            / NULLIF(p.product_total_positive_periodic_sales, 0) AS product_event_share,
        ROW_NUMBER() OVER (
            PARTITION BY m.tpnb
            ORDER BY GREATEST(COALESCE(m.trend_adjusted_incremental_sales, CAST(0 AS DOUBLE)), CAST(0 AS DOUBLE)) DESC,
                     m.parent_occasion_key,
                     m.event_year
        ) AS product_event_rank
    FROM material m
    JOIN product_roll p ON m.tpnb = p.tpnb
),

subclass_event AS (
    SELECT
        hierarchy_subclass_code,
        parent_occasion_key,
        parent_occasion_name,
        event_year,
        SUM(GREATEST(COALESCE(trend_adjusted_incremental_sales, CAST(0 AS DOUBLE)), CAST(0 AS DOUBLE))) AS subclass_event_positive_sales,
        SUM(GREATEST(COALESCE(trend_adjusted_incremental_baskets, CAST(0 AS DOUBLE)), CAST(0 AS DOUBLE))) AS subclass_event_positive_baskets,
        COUNT(*) AS subclass_material_products
    FROM material
    WHERE hierarchy_subclass_code IS NOT NULL
    GROUP BY hierarchy_subclass_code, parent_occasion_key, parent_occasion_name, event_year
),

subclass_roll AS (
    SELECT
        hierarchy_subclass_code,
        SUM(subclass_event_positive_sales) AS subclass_total_positive_periodic_sales,
        COUNT(*) AS subclass_positive_periodic_events
    FROM subclass_event
    GROUP BY hierarchy_subclass_code
),

subclass_ranked AS (
    SELECT
        s.*,
        r.subclass_total_positive_periodic_sales,
        r.subclass_positive_periodic_events,
        s.subclass_event_positive_sales / NULLIF(r.subclass_total_positive_periodic_sales, 0) AS subclass_event_share,
        ROW_NUMBER() OVER (
            PARTITION BY s.hierarchy_subclass_code
            ORDER BY s.subclass_event_positive_sales DESC, s.parent_occasion_key, s.event_year
        ) AS subclass_event_rank
    FROM subclass_event s
    JOIN subclass_roll r
      ON s.hierarchy_subclass_code = r.hierarchy_subclass_code
),

features AS (
    SELECT
        p.*,
        s.subclass_event_positive_sales,
        s.subclass_event_positive_baskets,
        s.subclass_material_products,
        s.subclass_total_positive_periodic_sales,
        s.subclass_positive_periodic_events,
        s.subclass_event_share,
        s.subclass_event_rank,
        CASE
            WHEN p.affinity_text RLIKE 'BEER|LAGER|ALE|CIDER|WINE|PROSECCO|CHAMPAGNE|WHISK|GIN|VODKA|RUM|SPIRIT'
              OR p.affinity_text RLIKE 'SOFT DRINK|COLA|ENERGY DRINK|SPORTS DRINK|MIXER|PIMM'
              OR p.affinity_text RLIKE 'CRISP|SNACK|POPCORN|NUTS|TORTILLA|PIZZA|PARTY FOOD|DIP|HOUMOUS|BARBECUE|BBQ'
              OR p.affinity_text RLIKE 'FOOTBALL|RUGBY|CRICKET|OLYMPIC|FIFA|EURO'
                THEN 'Y'
            ELSE 'N'
        END AS viewing_mission_affinity_flag,
        CASE
            WHEN p.affinity_text LIKE CONCAT('%', UPPER(p.parent_occasion_name), '%') THEN 'Y'
            ELSE 'N'
        END AS explicit_event_identity_flag
    FROM product_ranked p
    LEFT JOIN subclass_ranked s
      ON p.hierarchy_subclass_code = s.hierarchy_subclass_code
     AND p.parent_occasion_key = s.parent_occasion_key
     AND p.event_year = s.event_year
),

scored AS (
    SELECT
        f.*,
        CASE
            WHEN f.explicit_event_identity_flag = 'Y' THEN 'HIGH'
            WHEN f.viewing_mission_affinity_flag = 'Y'
             AND COALESCE(f.product_event_share, 0) >= p.high_product_event_share_min
             AND COALESCE(f.subclass_event_share, 0) >= p.high_subclass_event_share_min
             AND COALESCE(f.product_event_rank, 999) <= p.high_rank_max
             AND COALESCE(f.subclass_event_rank, 999) <= p.high_rank_max
             AND COALESCE(f.product_positive_periodic_events, 999) <= p.high_positive_event_count_max
                THEN 'HIGH'
            WHEN f.viewing_mission_affinity_flag = 'Y'
             AND COALESCE(f.product_event_share, 0) >= p.medium_product_event_share_min
             AND COALESCE(f.subclass_event_share, 0) >= p.medium_subclass_event_share_min
             AND COALESCE(f.product_event_rank, 999) <= p.medium_rank_max
             AND COALESCE(f.subclass_event_rank, 999) <= p.medium_rank_max
             AND COALESCE(f.product_positive_periodic_events, 999) <= p.medium_positive_event_count_max
                THEN 'MEDIUM'
            ELSE 'REVIEW'
        END AS periodic_response_confidence,
        CASE
            WHEN f.explicit_event_identity_flag = 'Y' THEN 'EXPLICIT_EVENT_IDENTITY'
            WHEN f.viewing_mission_affinity_flag = 'Y'
             AND COALESCE(f.product_event_share, 0) >= p.high_product_event_share_min
             AND COALESCE(f.subclass_event_share, 0) >= p.high_subclass_event_share_min
             AND COALESCE(f.product_event_rank, 999) <= p.high_rank_max
             AND COALESCE(f.subclass_event_rank, 999) <= p.high_rank_max
             AND COALESCE(f.product_positive_periodic_events, 999) <= p.high_positive_event_count_max
                THEN 'PRODUCT_AND_SUBCLASS_DOMINANT_VIEWING_MISSION'
            WHEN f.viewing_mission_affinity_flag = 'Y'
             AND COALESCE(f.product_event_share, 0) >= p.medium_product_event_share_min
             AND COALESCE(f.subclass_event_share, 0) >= p.medium_subclass_event_share_min
             AND COALESCE(f.product_event_rank, 999) <= p.medium_rank_max
             AND COALESCE(f.subclass_event_rank, 999) <= p.medium_rank_max
             AND COALESCE(f.product_positive_periodic_events, 999) <= p.medium_positive_event_count_max
                THEN 'CORROBORATED_VIEWING_MISSION'
            WHEN f.viewing_mission_affinity_flag = 'N' THEN 'NO_APPROVED_VIEWING_MISSION_AFFINITY'
            WHEN COALESCE(f.product_positive_periodic_events, 0) > p.medium_positive_event_count_max THEN 'RESPONDS_TO_TOO_MANY_PERIODIC_EVENTS'
            WHEN COALESCE(f.product_event_share, 0) < p.medium_product_event_share_min THEN 'WEAK_PRODUCT_EVENT_DOMINANCE'
            WHEN COALESCE(f.subclass_event_share, 0) < p.medium_subclass_event_share_min THEN 'WEAK_SUBCLASS_EVENT_DOMINANCE'
            ELSE 'OTHER_REVIEW'
        END AS periodic_confidence_reason
    FROM features f
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_6_periodic_params p
)

SELECT
    s.*,
    CASE
        WHEN s.periodic_response_confidence = 'HIGH' THEN 'PRODUCT_LEVEL_CANDIDATE'
        WHEN s.periodic_response_confidence = 'MEDIUM' THEN 'CATEGORY_LEVEL_ONLY'
        ELSE 'REVIEW_ONLY'
    END AS recommended_reporting_use
FROM scored s;


DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_6_periodic_event_summary;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_6_periodic_event_summary
USING PARQUET AS
SELECT
    parent_occasion_key,
    parent_occasion_name,
    event_year,
    periodic_response_confidence,
    recommended_reporting_use,
    COUNT(*) AS products,
    SUM(trend_adjusted_incremental_baskets) AS incremental_baskets,
    SUM(trend_adjusted_incremental_sales) AS incremental_sales
FROM lab_customer_value_analytics.seasonality_v2_6_periodic_confidence
GROUP BY parent_occasion_key, parent_occasion_name, event_year,
         periodic_response_confidence, recommended_reporting_use;

-- =====================================================================================
-- PART K - PERIODIC TOKEN-SAFE AFFINITY AND ANNUAL-WINDOW GUARD (v2.7)
-- =====================================================================================
-- =====================================================================================
-- SEASONALITY THEMES v2.7 - PERIODIC EVENT CONFIDENCE, TOKEN-SAFE + ANNUAL-OVERLAP GUARD
-- Spark SQL / Databricks / Kyuubi compatible. No QUALIFY.
--
-- Why this patch exists
-- ---------------------
-- v2.6 used unbounded substring regular expressions for viewing-mission affinity.
-- That made, for example:
--   * REGINA kitchen towel match GIN;
--   * EXTRA VIRGIN olive oil match GIN;
--   * ORIGINAL toothpaste / coffee match GIN;
--   * CHOCOLATE match COLA.
-- v2.7 tokenises the product text before matching, and it explicitly includes genuine
-- chocolate/biscuit/confectionery viewing missions rather than relying on accidental
-- substring matches.
--
-- v2.7 also holds a periodic response for review when the same product has an approved
-- annual-seasonal occasion whose EVENT or BUILDUP window overlaps the periodic event.
-- This prevents Christmas ranges inside The Ashes, for example, from being presented as
-- event-specific product responses. An explicit event identity may override the hold.
--
-- Periodic results remain occurrence-specific. They are never recurring seasonality and
-- they do not claim causality.
-- =====================================================================================

DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_7_periodic_params;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_7_periodic_params
USING PARQUET AS
SELECT
    CAST(0.40 AS DOUBLE) AS high_product_event_share_min,
    CAST(0.25 AS DOUBLE) AS high_subclass_event_share_min,
    CAST(2 AS INT)       AS high_rank_max,
    CAST(2 AS INT)       AS high_positive_event_count_max,
    CAST(0.25 AS DOUBLE) AS medium_product_event_share_min,
    CAST(0.15 AS DOUBLE) AS medium_subclass_event_share_min,
    CAST(3 AS INT)       AS medium_rank_max,
    CAST(3 AS INT)       AS medium_positive_event_count_max;


DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_7_periodic_confidence;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_7_periodic_confidence
USING PARQUET AS
WITH material AS (
    SELECT
        r.*,
        YEAR(r.event_start_date) AS event_year,
        d.hierarchy_subclass_code,
        CONCAT(
            ' ',
            TRIM(
                REGEXP_REPLACE(
                    UPPER(
                        CONCAT_WS(
                            ' ',
                            COALESCE(r.product_description, ''),
                            COALESCE(r.hierarchy_division_name, ''),
                            COALESCE(r.hierarchy_subclass_name, '')
                        )
                    ),
                    '[^A-Z0-9]+',
                    ' '
                )
            ),
            ' '
        ) AS affinity_token_text,
        CONCAT(
            ' ',
            TRIM(
                REGEXP_REPLACE(
                    UPPER(COALESCE(r.parent_occasion_name, '')),
                    '[^A-Z0-9]+',
                    ' '
                )
            ),
            ' '
        ) AS event_name_token_text
    FROM lab_customer_value_analytics.seasonality_v2_5_periodic_trend_response r
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_product_dim d
      ON TRY_CAST(r.tpnb AS BIGINT) = d.tpnb
    WHERE r.trend_adjusted_response_status = 'MATERIAL_TREND_ADJUSTED_RESPONSE'
      AND r.response_scope = 'MERCHANDISE'
),

product_roll AS (
    SELECT
        tpnb,
        SUM(
            GREATEST(
                COALESCE(trend_adjusted_incremental_sales, CAST(0 AS DOUBLE)),
                CAST(0 AS DOUBLE)
            )
        ) AS product_total_positive_periodic_sales,
        COUNT(*) AS product_positive_periodic_events
    FROM material
    GROUP BY tpnb
),

product_ranked AS (
    SELECT
        m.*,
        p.product_total_positive_periodic_sales,
        p.product_positive_periodic_events,
        GREATEST(
            COALESCE(m.trend_adjusted_incremental_sales, CAST(0 AS DOUBLE)),
            CAST(0 AS DOUBLE)
        ) / NULLIF(p.product_total_positive_periodic_sales, 0) AS product_event_share,
        ROW_NUMBER() OVER (
            PARTITION BY m.tpnb
            ORDER BY
                GREATEST(
                    COALESCE(m.trend_adjusted_incremental_sales, CAST(0 AS DOUBLE)),
                    CAST(0 AS DOUBLE)
                ) DESC,
                m.parent_occasion_key,
                m.event_year
        ) AS product_event_rank
    FROM material m
    JOIN product_roll p
      ON m.tpnb = p.tpnb
),

subclass_event AS (
    SELECT
        hierarchy_subclass_code,
        parent_occasion_key,
        parent_occasion_name,
        event_year,
        SUM(
            GREATEST(
                COALESCE(trend_adjusted_incremental_sales, CAST(0 AS DOUBLE)),
                CAST(0 AS DOUBLE)
            )
        ) AS subclass_event_positive_sales,
        SUM(
            GREATEST(
                COALESCE(trend_adjusted_incremental_baskets, CAST(0 AS DOUBLE)),
                CAST(0 AS DOUBLE)
            )
        ) AS subclass_event_positive_baskets,
        COUNT(*) AS subclass_material_products
    FROM material
    WHERE hierarchy_subclass_code IS NOT NULL
    GROUP BY
        hierarchy_subclass_code,
        parent_occasion_key,
        parent_occasion_name,
        event_year
),

subclass_roll AS (
    SELECT
        hierarchy_subclass_code,
        SUM(subclass_event_positive_sales) AS subclass_total_positive_periodic_sales,
        COUNT(*) AS subclass_positive_periodic_events
    FROM subclass_event
    GROUP BY hierarchy_subclass_code
),

subclass_ranked AS (
    SELECT
        s.*,
        r.subclass_total_positive_periodic_sales,
        r.subclass_positive_periodic_events,
        s.subclass_event_positive_sales
            / NULLIF(r.subclass_total_positive_periodic_sales, 0) AS subclass_event_share,
        ROW_NUMBER() OVER (
            PARTITION BY s.hierarchy_subclass_code
            ORDER BY
                s.subclass_event_positive_sales DESC,
                s.parent_occasion_key,
                s.event_year
        ) AS subclass_event_rank
    FROM subclass_event s
    JOIN subclass_roll r
      ON s.hierarchy_subclass_code = r.hierarchy_subclass_code
),

-- Approved annual product occasions whose event/build-up dates overlap a periodic event.
-- These are a confounding risk and are held for review unless the product explicitly names
-- the periodic event itself.
annual_overlap_detail AS (
    SELECT DISTINCT
        m.tpnb,
        m.parent_occasion_key AS periodic_parent_occasion_key,
        m.event_year          AS periodic_event_year,
        a.parent_occasion_key AS annual_parent_occasion_key,
        a.parent_occasion_name AS annual_parent_occasion_name,
        a.occasion_verdict     AS annual_occasion_verdict,
        o.window_type          AS annual_window_type
    FROM material m
    JOIN lab_customer_value_analytics.seasonality_v2_6_responder_products_annual a
      ON m.tpnb = a.tpnb
    JOIN lab_customer_value_analytics.seasonality_v2_occurrences o
      ON o.parent_occasion_key = a.parent_occasion_key
     AND o.is_completed = 'Y'
     AND o.window_type IN ('EVENT', 'BUILDUP')
     AND o.event_start_date <= m.event_end_date
     AND o.event_end_date   >= m.event_start_date
),

annual_overlap_roll AS (
    SELECT
        tpnb,
        periodic_parent_occasion_key,
        periodic_event_year,
        COUNT(DISTINCT annual_parent_occasion_key) AS overlapping_annual_occasion_count,
        CONCAT_WS(
            ', ',
            SORT_ARRAY(COLLECT_SET(annual_parent_occasion_name))
        ) AS overlapping_annual_occasion_list,
        MAX(
            CASE
                WHEN annual_occasion_verdict IN (
                    'Specifically seasonal',
                    'Supported specifically seasonal'
                ) THEN 1
                ELSE 0
            END
        ) AS has_specific_annual_overlap
    FROM annual_overlap_detail
    GROUP BY
        tpnb,
        periodic_parent_occasion_key,
        periodic_event_year
),

features_raw AS (
    SELECT
        p.*,
        s.subclass_event_positive_sales,
        s.subclass_event_positive_baskets,
        s.subclass_material_products,
        s.subclass_total_positive_periodic_sales,
        s.subclass_positive_periodic_events,
        s.subclass_event_share,
        s.subclass_event_rank,
        COALESCE(a.overlapping_annual_occasion_count, 0)
            AS overlapping_annual_occasion_count,
        COALESCE(a.overlapping_annual_occasion_list, '')
            AS overlapping_annual_occasion_list,
        COALESCE(a.has_specific_annual_overlap, 0)
            AS has_specific_annual_overlap,
        CASE
            WHEN INSTR(p.affinity_token_text, p.event_name_token_text) > 0
             AND LENGTH(TRIM(p.event_name_token_text)) > 0
                THEN 'Y'
            ELSE 'N'
        END AS explicit_event_identity_flag,
        CASE
            WHEN p.affinity_token_text RLIKE ' (BEER|LAGER|ALE|CIDER|WINE|WINES|PROSECCO|CHAMPAGNE|WHISKY|WHISKEY|GIN|VODKA|RUM|SPIRIT|SPIRITS) '
                THEN 'ALCOHOL'
            WHEN p.affinity_token_text RLIKE ' (SOFT DRINK|SOFT DRINKS|COLA|ENERGY DRINK|ENERGY DRINKS|SPORTS DRINK|SPORTS DRINKS|MIXER|MIXERS|PIMM|PIMMS) '
                THEN 'SOFT_DRINKS_AND_MIXERS'
            WHEN p.affinity_token_text RLIKE ' (CRISP|CRISPS|SNACK|SNACKS|POPCORN|NUT|NUTS|TORTILLA|TORTILLAS|CHOCOLATE|CHOCOLATES|CONFECTIONERY|BISCUIT|BISCUITS|COOKIE|COOKIES|SWEET|SWEETS|ICE CREAM) '
                THEN 'SNACKS_AND_CONFECTIONERY'
            WHEN p.affinity_token_text RLIKE ' (PIZZA|PIZZAS|PARTY FOOD|DIP|DIPS|HOUMOUS|HUMMUS|BARBECUE|BBQ|CHICKEN WINGS|DRUMSTICK|DRUMSTICKS|SAUSAGE ROLL|SAUSAGE ROLLS|PORK PIE|PORK PIES) '
                THEN 'PARTY_AND_VIEWING_FOOD'
            WHEN p.affinity_token_text RLIKE ' (FOOTBALL|RUGBY|CRICKET|OLYMPIC|OLYMPICS|FIFA|UEFA|EURO) '
                THEN 'EXPLICIT_SPORT_MISSION'
            ELSE ''
        END AS viewing_mission_affinity_group
    FROM product_ranked p
    LEFT JOIN subclass_ranked s
      ON p.hierarchy_subclass_code = s.hierarchy_subclass_code
     AND p.parent_occasion_key = s.parent_occasion_key
     AND p.event_year = s.event_year
    LEFT JOIN annual_overlap_roll a
      ON p.tpnb = a.tpnb
     AND p.parent_occasion_key = a.periodic_parent_occasion_key
     AND p.event_year = a.periodic_event_year
),

features AS (
    SELECT
        f.*,
        CASE
            WHEN f.viewing_mission_affinity_group <> '' THEN 'Y'
            ELSE 'N'
        END AS viewing_mission_affinity_flag
    FROM features_raw f
),

scored AS (
    SELECT
        f.*,
        CASE
            WHEN f.explicit_event_identity_flag = 'Y'
                THEN 'HIGH'
            WHEN f.overlapping_annual_occasion_count > 0
                THEN 'REVIEW'
            WHEN f.viewing_mission_affinity_flag = 'Y'
             AND COALESCE(f.product_event_share, 0) >= p.high_product_event_share_min
             AND COALESCE(f.subclass_event_share, 0) >= p.high_subclass_event_share_min
             AND COALESCE(f.product_event_rank, 999) <= p.high_rank_max
             AND COALESCE(f.subclass_event_rank, 999) <= p.high_rank_max
             AND COALESCE(f.product_positive_periodic_events, 999)
                    <= p.high_positive_event_count_max
                THEN 'HIGH'
            WHEN f.viewing_mission_affinity_flag = 'Y'
             AND COALESCE(f.product_event_share, 0) >= p.medium_product_event_share_min
             AND COALESCE(f.subclass_event_share, 0) >= p.medium_subclass_event_share_min
             AND COALESCE(f.product_event_rank, 999) <= p.medium_rank_max
             AND COALESCE(f.subclass_event_rank, 999) <= p.medium_rank_max
             AND COALESCE(f.product_positive_periodic_events, 999)
                    <= p.medium_positive_event_count_max
                THEN 'MEDIUM'
            ELSE 'REVIEW'
        END AS periodic_response_confidence,
        CASE
            WHEN f.explicit_event_identity_flag = 'Y'
                THEN 'EXPLICIT_EVENT_IDENTITY'
            WHEN f.overlapping_annual_occasion_count > 0
                THEN 'ANNUAL_SEASONAL_WINDOW_OVERLAP'
            WHEN f.viewing_mission_affinity_flag = 'Y'
             AND COALESCE(f.product_event_share, 0) >= p.high_product_event_share_min
             AND COALESCE(f.subclass_event_share, 0) >= p.high_subclass_event_share_min
             AND COALESCE(f.product_event_rank, 999) <= p.high_rank_max
             AND COALESCE(f.subclass_event_rank, 999) <= p.high_rank_max
             AND COALESCE(f.product_positive_periodic_events, 999)
                    <= p.high_positive_event_count_max
                THEN 'PRODUCT_AND_SUBCLASS_DOMINANT_TOKEN_SAFE_VIEWING_MISSION'
            WHEN f.viewing_mission_affinity_flag = 'Y'
             AND COALESCE(f.product_event_share, 0) >= p.medium_product_event_share_min
             AND COALESCE(f.subclass_event_share, 0) >= p.medium_subclass_event_share_min
             AND COALESCE(f.product_event_rank, 999) <= p.medium_rank_max
             AND COALESCE(f.subclass_event_rank, 999) <= p.medium_rank_max
             AND COALESCE(f.product_positive_periodic_events, 999)
                    <= p.medium_positive_event_count_max
                THEN 'CORROBORATED_TOKEN_SAFE_VIEWING_MISSION'
            WHEN f.viewing_mission_affinity_flag = 'N'
                THEN 'NO_APPROVED_TOKEN_SAFE_VIEWING_MISSION_AFFINITY'
            WHEN COALESCE(f.product_positive_periodic_events, 0)
                    > p.medium_positive_event_count_max
                THEN 'RESPONDS_TO_TOO_MANY_PERIODIC_EVENTS'
            WHEN COALESCE(f.product_event_share, 0)
                    < p.medium_product_event_share_min
                THEN 'WEAK_PRODUCT_EVENT_DOMINANCE'
            WHEN COALESCE(f.subclass_event_share, 0)
                    < p.medium_subclass_event_share_min
                THEN 'WEAK_SUBCLASS_EVENT_DOMINANCE'
            ELSE 'OTHER_REVIEW'
        END AS periodic_confidence_reason
    FROM features f
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_7_periodic_params p
)

SELECT
    s.*,
    CASE
        WHEN s.periodic_response_confidence = 'HIGH'
            THEN 'PRODUCT_LEVEL_CANDIDATE'
        WHEN s.periodic_response_confidence = 'MEDIUM'
            THEN 'CATEGORY_LEVEL_ONLY'
        ELSE 'REVIEW_ONLY'
    END AS recommended_reporting_use
FROM scored s;


DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_7_periodic_event_summary;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_7_periodic_event_summary
USING PARQUET AS
SELECT
    parent_occasion_key,
    parent_occasion_name,
    event_year,
    periodic_response_confidence,
    recommended_reporting_use,
    COUNT(*) AS products,
    SUM(trend_adjusted_incremental_baskets) AS incremental_baskets,
    SUM(trend_adjusted_incremental_sales) AS incremental_sales
FROM lab_customer_value_analytics.seasonality_v2_7_periodic_confidence
GROUP BY
    parent_occasion_key,
    parent_occasion_name,
    event_year,
    periodic_response_confidence,
    recommended_reporting_use;

-- =====================================================================================
-- PART L - PERIODIC SEMANTIC-SAFE AFFINITY (v2.8)
-- =====================================================================================
-- =====================================================================================
-- SEASONALITY THEMES v2.8 - PERIODIC EVENT CONFIDENCE, SEMANTIC-SAFE AFFINITY
-- Spark SQL / Databricks / Kyuubi compatible. No QUALIFY and no SELECT * EXCEPT.
--
-- Purpose
-- -------
-- v2.7 fixed unsafe substring matching and annual-season overlap. Its diagnostics still
-- showed a smaller semantic issue: the complete token SWEET can describe confectionery,
-- but it can also occur in unrelated products such as Sweet Orange body wash or Sweet
-- Vine tomatoes. v2.8 removes that broad token, restricts viewing-mission affinity to
-- food/beverage divisions (unless the product explicitly names the sporting event), and
-- adds a small set of high-confidence missing snack/party-food tokens such as PEANUTS,
-- CRACKERS and DIPPERS.
--
-- This is a reporting-confidence layer only. It does not alter matched-calendar or trend-
-- adjusted response calculations, does not claim causality, and never enters recurring
-- annual seasonality.
-- =====================================================================================

DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_8_periodic_params;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_8_periodic_params
USING PARQUET AS
SELECT
    CAST(0.40 AS DOUBLE) AS high_product_event_share_min,
    CAST(0.25 AS DOUBLE) AS high_subclass_event_share_min,
    CAST(2 AS INT)       AS high_rank_max,
    CAST(2 AS INT)       AS high_positive_event_count_max,
    CAST(0.25 AS DOUBLE) AS medium_product_event_share_min,
    CAST(0.15 AS DOUBLE) AS medium_subclass_event_share_min,
    CAST(3 AS INT)       AS medium_rank_max,
    CAST(3 AS INT)       AS medium_positive_event_count_max;


DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_8_periodic_confidence;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_8_periodic_confidence
USING PARQUET AS
WITH scope_flags AS (
    SELECT
        v.*,
        CASE
            WHEN UPPER(TRIM(COALESCE(v.hierarchy_division_name, ''))) IN (
                'WINES & SPIRITS',
                'AMBIENT DRY GROCERY',
                'FRESH FOODS',
                'BREAD/BAKERY & REST.'
            ) THEN 'Y'
            ELSE 'N'
        END AS food_beverage_scope_flag_v2_8
    FROM lab_customer_value_analytics.seasonality_v2_7_periodic_confidence v
),

affinity AS (
    SELECT
        s.*,
        CASE
            -- An explicit event identity is allowed in any division.
            WHEN s.explicit_event_identity_flag = 'Y'
                THEN 'EXPLICIT_SPORT_MISSION'

            -- All generic viewing missions must be merchandise in a food/beverage division.
            WHEN s.food_beverage_scope_flag_v2_8 = 'N'
                THEN ''

            -- Alcohol and no/low-alcohol viewing drinks. Avoid food/homeware uses of WINE.
            WHEN (
                    UPPER(TRIM(COALESCE(s.hierarchy_division_name, ''))) = 'WINES & SPIRITS'
                    OR s.affinity_token_text RLIKE
                       ' (BEER|LAGER|ALE|CIDER|WINE|WINES|PROSECCO|CHAMPAGNE|WHISKY|WHISKEY|GIN|VODKA|RUM|SPIRIT|SPIRITS) '
                 )
             AND NOT s.affinity_token_text RLIKE
                 ' (VINEGAR|GLASS|GLASSES|OPENER|CORKSCREW|DECANTER|GUM|GUMS|SAUCE|SAUCES) '
                THEN 'ALCOHOL'

            -- Soft drinks and mixers. Terms are intentionally narrow and beverage-specific.
            WHEN s.affinity_token_text RLIKE
                 ' (SOFT DRINK|SOFT DRINKS|COLA|ENERGY DRINK|ENERGY DRINKS|SPORTS DRINK|SPORTS DRINKS|MIXER|MIXERS|TONIC WATER|LEMONADE|SODA WATER|PIMM|PIMMS|CORDIAL) '
                THEN 'SOFT_DRINKS_AND_MIXERS'

            -- Ready-to-eat snacks/confectionery. SWEET singular is deliberately excluded.
            WHEN s.affinity_token_text RLIKE
                 ' (CRISP|CRISPS|SNACK|SNACKS|POPCORN|NUT|NUTS|PEANUT|PEANUTS|ALMOND|ALMONDS|CASHEW|CASHEWS|TORTILLA|TORTILLAS|NACHO|NACHOS|PRETZEL|PRETZELS|CRACKER|CRACKERS|CHOCOLATE|CHOCOLATES|CONFECTIONERY|BISCUIT|BISCUITS|COOKIE|COOKIES|SWEETS|ICE CREAM|LOLLY|LOLLIES) '
                THEN 'SNACKS_AND_CONFECTIONERY'

            -- Ready-to-eat party/viewing food. Pizza ingredients are not treated as pizza.
            WHEN (
                    s.affinity_token_text RLIKE
                    ' (PARTY FOOD|DIP|DIPS|HOUMOUS|HUMMUS|BARBECUE|BBQ|CHICKEN WINGS|DRUMSTICK|DRUMSTICKS|SAUSAGE ROLL|SAUSAGE ROLLS|PORK PIE|PORK PIES|NUGGET|NUGGETS|DIPPER|DIPPERS) '
                    OR (
                        s.affinity_token_text RLIKE ' (PIZZA|PIZZAS) '
                        AND NOT s.affinity_token_text RLIKE
                            ' (SAUCE|SAUCES|BASE|BASES|FLOUR|SEASONING|SEASONINGS) '
                    )
                 )
                THEN 'PARTY_AND_VIEWING_FOOD'

            ELSE ''
        END AS viewing_mission_affinity_group_v2_8
    FROM scope_flags s
),

features AS (
    SELECT
        a.*,
        CASE
            WHEN a.viewing_mission_affinity_group_v2_8 <> '' THEN 'Y'
            ELSE 'N'
        END AS viewing_mission_affinity_flag_v2_8
    FROM affinity a
),

scored AS (
    SELECT
        f.*,
        CASE
            WHEN f.explicit_event_identity_flag = 'Y'
                THEN 'HIGH'
            WHEN COALESCE(f.overlapping_annual_occasion_count, 0) > 0
                THEN 'REVIEW'
            WHEN f.viewing_mission_affinity_flag_v2_8 = 'Y'
             AND COALESCE(f.product_event_share, 0) >= p.high_product_event_share_min
             AND COALESCE(f.subclass_event_share, 0) >= p.high_subclass_event_share_min
             AND COALESCE(f.product_event_rank, 999) <= p.high_rank_max
             AND COALESCE(f.subclass_event_rank, 999) <= p.high_rank_max
             AND COALESCE(f.product_positive_periodic_events, 999)
                    <= p.high_positive_event_count_max
                THEN 'HIGH'
            WHEN f.viewing_mission_affinity_flag_v2_8 = 'Y'
             AND COALESCE(f.product_event_share, 0) >= p.medium_product_event_share_min
             AND COALESCE(f.subclass_event_share, 0) >= p.medium_subclass_event_share_min
             AND COALESCE(f.product_event_rank, 999) <= p.medium_rank_max
             AND COALESCE(f.subclass_event_rank, 999) <= p.medium_rank_max
             AND COALESCE(f.product_positive_periodic_events, 999)
                    <= p.medium_positive_event_count_max
                THEN 'MEDIUM'
            ELSE 'REVIEW'
        END AS periodic_response_confidence_v2_8,
        CASE
            WHEN f.explicit_event_identity_flag = 'Y'
                THEN 'EXPLICIT_EVENT_IDENTITY'
            WHEN COALESCE(f.overlapping_annual_occasion_count, 0) > 0
                THEN 'ANNUAL_SEASONAL_WINDOW_OVERLAP'
            WHEN f.viewing_mission_affinity_flag_v2_8 = 'Y'
             AND COALESCE(f.product_event_share, 0) >= p.high_product_event_share_min
             AND COALESCE(f.subclass_event_share, 0) >= p.high_subclass_event_share_min
             AND COALESCE(f.product_event_rank, 999) <= p.high_rank_max
             AND COALESCE(f.subclass_event_rank, 999) <= p.high_rank_max
             AND COALESCE(f.product_positive_periodic_events, 999)
                    <= p.high_positive_event_count_max
                THEN 'PRODUCT_AND_SUBCLASS_DOMINANT_SEMANTIC_SAFE_VIEWING_MISSION'
            WHEN f.viewing_mission_affinity_flag_v2_8 = 'Y'
             AND COALESCE(f.product_event_share, 0) >= p.medium_product_event_share_min
             AND COALESCE(f.subclass_event_share, 0) >= p.medium_subclass_event_share_min
             AND COALESCE(f.product_event_rank, 999) <= p.medium_rank_max
             AND COALESCE(f.subclass_event_rank, 999) <= p.medium_rank_max
             AND COALESCE(f.product_positive_periodic_events, 999)
                    <= p.medium_positive_event_count_max
                THEN 'CORROBORATED_SEMANTIC_SAFE_VIEWING_MISSION'
            WHEN f.viewing_mission_affinity_flag_v2_8 = 'N'
                THEN 'NO_APPROVED_SEMANTIC_SAFE_VIEWING_MISSION_AFFINITY'
            WHEN COALESCE(f.product_positive_periodic_events, 0)
                    > p.medium_positive_event_count_max
                THEN 'RESPONDS_TO_TOO_MANY_PERIODIC_EVENTS'
            WHEN COALESCE(f.product_event_share, 0)
                    < p.medium_product_event_share_min
                THEN 'WEAK_PRODUCT_EVENT_DOMINANCE'
            WHEN COALESCE(f.subclass_event_share, 0)
                    < p.medium_subclass_event_share_min
                THEN 'WEAK_SUBCLASS_EVENT_DOMINANCE'
            ELSE 'OTHER_REVIEW'
        END AS periodic_confidence_reason_v2_8
    FROM features f
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_8_periodic_params p
)

SELECT
    s.*,
    CASE
        WHEN s.periodic_response_confidence_v2_8 = 'HIGH'
            THEN 'PRODUCT_LEVEL_CANDIDATE'
        WHEN s.periodic_response_confidence_v2_8 = 'MEDIUM'
            THEN 'CATEGORY_LEVEL_ONLY'
        ELSE 'REVIEW_ONLY'
    END AS recommended_reporting_use_v2_8
FROM scored s;


DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_8_periodic_event_summary;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_8_periodic_event_summary
USING PARQUET AS
SELECT
    parent_occasion_key,
    parent_occasion_name,
    event_year,
    periodic_response_confidence_v2_8 AS periodic_response_confidence,
    recommended_reporting_use_v2_8   AS recommended_reporting_use,
    COUNT(*) AS products,
    SUM(trend_adjusted_incremental_baskets) AS incremental_baskets,
    SUM(trend_adjusted_incremental_sales) AS incremental_sales
FROM lab_customer_value_analytics.seasonality_v2_8_periodic_confidence
GROUP BY
    parent_occasion_key,
    parent_occasion_name,
    event_year,
    periodic_response_confidence_v2_8,
    recommended_reporting_use_v2_8;

-- =====================================================================================
-- PART M - PERIODIC CATEGORY-SAFE AFFINITY (v2.9)
-- =====================================================================================
-- =====================================================================================
-- SEASONALITY THEMES v2.9 - PERIODIC EVENT CONFIDENCE, CATEGORY-SAFE AFFINITY
-- Spark SQL / Databricks / Kyuubi compatible. No QUALIFY and no SELECT * EXCEPT.
--
-- Purpose
-- -------
-- v2.8 removed broad SWEET/non-food matches and restored several missing viewing-food
-- terms. Its review found a final context problem: individual words can still describe a
-- different product mission. Examples include CRISP in cooking oil, PEANUT in peanut oil,
-- NUGGETS in dog treats, ALMOND in dairy alternatives, BEER in ginger beer and ALE in a
-- pie. v2.9 makes the mission category-aware:
--   * hierarchy/subclass meaning takes precedence over product-name words;
--   * pet food, cooking oils, spreads and breakfast cereals are excluded from generic
--     viewing-snack affinity;
--   * alcohol is inferred from the Wines & Spirits division or an alcoholic-beverage
--     subclass, not merely from BEER/ALE/WINE appearing anywhere in the description;
--   * lollies and snack subclasses take precedence over COLA or other flavour words;
--   * genuine gummy/chewy confectionery is restored through its subclass meaning.
--
-- This remains an observed occurrence-response reporting layer. It does not claim
-- causality and never enters recurring annual seasonality.
-- =====================================================================================

DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_9_periodic_params;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_9_periodic_params
USING PARQUET AS
SELECT
    CAST(0.40 AS DOUBLE) AS high_product_event_share_min,
    CAST(0.25 AS DOUBLE) AS high_subclass_event_share_min,
    CAST(2 AS INT)       AS high_rank_max,
    CAST(2 AS INT)       AS high_positive_event_count_max,
    CAST(0.25 AS DOUBLE) AS medium_product_event_share_min,
    CAST(0.15 AS DOUBLE) AS medium_subclass_event_share_min,
    CAST(3 AS INT)       AS medium_rank_max,
    CAST(3 AS INT)       AS medium_positive_event_count_max;


DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_9_periodic_confidence;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_9_periodic_confidence
USING PARQUET AS
WITH text_features AS (
    SELECT
        v.*,
        UPPER(TRIM(COALESCE(v.hierarchy_division_name, ''))) AS division_upper_v2_9,
        CONCAT(
            ' ',
            TRIM(
                REGEXP_REPLACE(
                    UPPER(COALESCE(v.product_description, '')),
                    '[^A-Z0-9]+',
                    ' '
                )
            ),
            ' '
        ) AS product_token_text_v2_9,
        CONCAT(
            ' ',
            TRIM(
                REGEXP_REPLACE(
                    UPPER(COALESCE(v.hierarchy_subclass_name, '')),
                    '[^A-Z0-9]+',
                    ' '
                )
            ),
            ' '
        ) AS subclass_token_text_v2_9,
        CASE
            WHEN UPPER(TRIM(COALESCE(v.hierarchy_division_name, ''))) IN (
                'WINES & SPIRITS',
                'AMBIENT DRY GROCERY',
                'FRESH FOODS',
                'BREAD/BAKERY & REST.'
            ) THEN 'Y'
            ELSE 'N'
        END AS food_beverage_scope_flag_v2_9
    FROM lab_customer_value_analytics.seasonality_v2_8_periodic_confidence v
),

context_flags AS (
    SELECT
        t.*,
        CASE
            WHEN t.subclass_token_text_v2_9 RLIKE
                 ' (DOG|DOGS|CAT|CATS|PET|PETS|ANIMAL|ANIMALS) '
              OR t.product_token_text_v2_9 RLIKE
                 ' (DOG TREAT|DOG TREATS|CAT TREAT|CAT TREATS|PET TREAT|PET TREATS) '
                THEN 'Y'
            ELSE 'N'
        END AS pet_product_flag_v2_9,
        CASE
            WHEN t.subclass_token_text_v2_9 RLIKE
                 ' (OIL|OILS|VINEGAR|VINEGARS|PEANUT BUTTER|NUT BUTTER|SWEET SPREAD|SWEET SPREADS|SPREADABLES|COOKING SAUCE|COOKING SAUCES|POUR OVER SAUCES|PASTA SAUCE|PASTA SAUCES) '
                THEN 'Y'
            ELSE 'N'
        END AS cooking_ingredient_or_spread_flag_v2_9,
        CASE
            WHEN t.subclass_token_text_v2_9 RLIKE
                 ' (CEREAL|CEREALS|MUESLI|GRANOLA|BREAKFAST CEREALS|EVERYDAY CEREALS|KIDS CEREALS) '
                THEN 'Y'
            ELSE 'N'
        END AS breakfast_cereal_flag_v2_9,
        CASE
            WHEN t.subclass_token_text_v2_9 RLIKE
                 ' (DAIRY ALT|DAIRY ALTS|MILK ALTERNATIVE|MILK ALTERNATIVES|LONGLIFE DAIRY ALTS|CHILLED DAIRY ALTS) '
                THEN 'Y'
            ELSE 'N'
        END AS dairy_alternative_flag_v2_9,
        CASE
            WHEN t.subclass_token_text_v2_9 RLIKE
                 ' (LOLLY|LOLLIES|ICE LOLLIES|CONES|ICE CREAM|CHOCOLATE|CONFECTIONERY|BISCUIT|BISCUITS|COOKIE|COOKIES|CRISP|CRISPS|SNACK|SNACKS|POPCORN|PEANUTS|NUTS|ALMONDS|CASHEWS|TORTILLA CHIPS|NACHOS|PRETZELS|CRACKER|CRACKERS|GUMMY|CHEWY|SWEET BAGS|CAKE MINI ROLLS|CAKE SLICES|EVERYDAY TREATS|TWISTWRAP) '
                THEN 'Y'
            ELSE 'N'
        END AS snack_subclass_flag_v2_9,
        CASE
            WHEN t.subclass_token_text_v2_9 RLIKE
                 ' (PIZZA|PIZZAS|PARTY FOOD|PARTYFOOD|DIP|DIPS|HOUMOUS|HUMMUS|BARBECUE|BBQ|CHICKEN PORTIONS|COATED POULTRY|BITE SIZE|SAUSAGE ROLL|SAUSAGE ROLLS|PORK PIE|PORK PIES|HOT PIES|SLOW COOKED BBQ|FROZEN FISH BARBECUE|SNACK MEALS|COOKED MEAT SNACKS|MICRO SNACKING|FOS SNACKING) '
                THEN 'Y'
            ELSE 'N'
        END AS party_food_subclass_flag_v2_9,
        CASE
            WHEN t.division_upper_v2_9 = 'WINES & SPIRITS'
              OR t.subclass_token_text_v2_9 RLIKE
                 ' (BEER|BEERS|LAGER|LAGERS|STOUT|STOUTS|CIDER|CIDERS|WINE|WINES|PROSECCO|CHAMPAGNE|WHISKY|WHISKEY|GIN|VODKA|RUM|SPIRIT|SPIRITS|LIQUEUR|LIQUEURS|COGNAC|TEQUILA|ALCOPOPS|NO ALCOHOL BEER|NO ALCOHOL CIDER) '
                THEN 'Y'
            ELSE 'N'
        END AS alcohol_beverage_context_flag_v2_9,
        CASE
            WHEN t.subclass_token_text_v2_9 RLIKE
                 ' (COLA|SOFT DRINK|SOFT DRINKS|ENERGY|SPORTS AND ENERGY|TONIC WATER|LEMONADE|SODA WATER|MIXER|MIXERS|HIGH JUICE|CORDIAL|CORDIALS|ADULT HERB FRUIT DRINKS|FRUIT FLAV M PACK CANS) '
                THEN 'Y'
            ELSE 'N'
        END AS soft_drink_subclass_flag_v2_9
    FROM text_features t
),

affinity AS (
    SELECT
        c.*,
        CASE
            -- Explicit event-branded merchandise is allowed in any division.
            WHEN c.explicit_event_identity_flag = 'Y'
                THEN 'EXPLICIT_SPORT_MISSION'

            -- Generic missions must be merchandise in an approved food/beverage division.
            WHEN c.food_beverage_scope_flag_v2_9 = 'N'
                THEN ''

            -- Pet/animal lines never qualify from human-food words such as NUGGETS.
            WHEN c.pet_product_flag_v2_9 = 'Y'
                THEN ''

            -- Subclass meaning takes precedence over flavour/product-name words.
            WHEN c.party_food_subclass_flag_v2_9 = 'Y'
                THEN 'PARTY_AND_VIEWING_FOOD'

            WHEN c.snack_subclass_flag_v2_9 = 'Y'
             AND c.cooking_ingredient_or_spread_flag_v2_9 = 'N'
             AND c.breakfast_cereal_flag_v2_9 = 'N'
             AND c.dairy_alternative_flag_v2_9 = 'N'
                THEN 'SNACKS_AND_CONFECTIONERY'

            -- Alcohol requires a beverage hierarchy context; ALE in a pie and BEER in
            -- ginger beer no longer qualify merely from the product name.
            WHEN c.alcohol_beverage_context_flag_v2_9 = 'Y'
                THEN 'ALCOHOL'

            WHEN c.soft_drink_subclass_flag_v2_9 = 'Y'
                THEN 'SOFT_DRINKS_AND_MIXERS'

            -- Safe product-name fallbacks for generic/poorly named subclasses.
            WHEN c.cooking_ingredient_or_spread_flag_v2_9 = 'N'
             AND c.breakfast_cereal_flag_v2_9 = 'N'
             AND c.dairy_alternative_flag_v2_9 = 'N'
             AND c.product_token_text_v2_9 RLIKE
                 ' (CRISPS|POPCORN|PRETZEL|PRETZELS|CRACKER|CRACKERS|CHOCOLATE|CHOCOLATES|CONFECTIONERY|BISCUIT|BISCUITS|COOKIE|COOKIES|SWEETS|GUMMY|GUMMIES|CHEWY|ICE CREAM|LOLLY|LOLLIES) '
                THEN 'SNACKS_AND_CONFECTIONERY'

            WHEN c.product_token_text_v2_9 RLIKE
                 ' (PARTY FOOD|CHICKEN WINGS|DRUMSTICK|DRUMSTICKS|SAUSAGE ROLL|SAUSAGE ROLLS|PORK PIE|PORK PIES|CHICKEN DIPPER|CHICKEN DIPPERS|CHICKEN NUGGET|CHICKEN NUGGETS) '
                THEN 'PARTY_AND_VIEWING_FOOD'

            WHEN c.product_token_text_v2_9 RLIKE
                 ' (GINGER BEER|ROOT BEER|SOFT DRINK|SOFT DRINKS|COLA|ENERGY DRINK|ENERGY DRINKS|SPORTS DRINK|SPORTS DRINKS|TONIC WATER|LEMONADE|SODA WATER|CORDIAL) '
                THEN 'SOFT_DRINKS_AND_MIXERS'

            ELSE ''
        END AS viewing_mission_affinity_group_v2_9,
        CASE
            WHEN c.pet_product_flag_v2_9 = 'Y'
                THEN 'PET_OR_ANIMAL_PRODUCT'
            WHEN c.cooking_ingredient_or_spread_flag_v2_9 = 'Y'
                THEN 'COOKING_INGREDIENT_OR_SPREAD'
            WHEN c.breakfast_cereal_flag_v2_9 = 'Y'
                THEN 'BREAKFAST_CEREAL_OR_GRANOLA'
            WHEN c.dairy_alternative_flag_v2_9 = 'Y'
                THEN 'DAIRY_ALTERNATIVE'
            WHEN c.food_beverage_scope_flag_v2_9 = 'N'
             AND c.explicit_event_identity_flag <> 'Y'
                THEN 'NON_FOOD_OR_BEVERAGE_SCOPE'
            ELSE ''
        END AS affinity_exclusion_reason_v2_9
    FROM context_flags c
),

features AS (
    SELECT
        a.*,
        CASE
            WHEN a.viewing_mission_affinity_group_v2_9 <> '' THEN 'Y'
            ELSE 'N'
        END AS viewing_mission_affinity_flag_v2_9
    FROM affinity a
),

scored AS (
    SELECT
        f.*,
        CASE
            WHEN f.explicit_event_identity_flag = 'Y'
                THEN 'HIGH'
            WHEN COALESCE(f.overlapping_annual_occasion_count, 0) > 0
                THEN 'REVIEW'
            WHEN f.viewing_mission_affinity_flag_v2_9 = 'Y'
             AND COALESCE(f.product_event_share, 0) >= p.high_product_event_share_min
             AND COALESCE(f.subclass_event_share, 0) >= p.high_subclass_event_share_min
             AND COALESCE(f.product_event_rank, 999) <= p.high_rank_max
             AND COALESCE(f.subclass_event_rank, 999) <= p.high_rank_max
             AND COALESCE(f.product_positive_periodic_events, 999)
                    <= p.high_positive_event_count_max
                THEN 'HIGH'
            WHEN f.viewing_mission_affinity_flag_v2_9 = 'Y'
             AND COALESCE(f.product_event_share, 0) >= p.medium_product_event_share_min
             AND COALESCE(f.subclass_event_share, 0) >= p.medium_subclass_event_share_min
             AND COALESCE(f.product_event_rank, 999) <= p.medium_rank_max
             AND COALESCE(f.subclass_event_rank, 999) <= p.medium_rank_max
             AND COALESCE(f.product_positive_periodic_events, 999)
                    <= p.medium_positive_event_count_max
                THEN 'MEDIUM'
            ELSE 'REVIEW'
        END AS periodic_response_confidence_v2_9,
        CASE
            WHEN f.explicit_event_identity_flag = 'Y'
                THEN 'EXPLICIT_EVENT_IDENTITY'
            WHEN COALESCE(f.overlapping_annual_occasion_count, 0) > 0
                THEN 'ANNUAL_SEASONAL_WINDOW_OVERLAP'
            WHEN f.viewing_mission_affinity_flag_v2_9 = 'Y'
             AND COALESCE(f.product_event_share, 0) >= p.high_product_event_share_min
             AND COALESCE(f.subclass_event_share, 0) >= p.high_subclass_event_share_min
             AND COALESCE(f.product_event_rank, 999) <= p.high_rank_max
             AND COALESCE(f.subclass_event_rank, 999) <= p.high_rank_max
             AND COALESCE(f.product_positive_periodic_events, 999)
                    <= p.high_positive_event_count_max
                THEN 'PRODUCT_AND_SUBCLASS_DOMINANT_CATEGORY_SAFE_VIEWING_MISSION'
            WHEN f.viewing_mission_affinity_flag_v2_9 = 'Y'
             AND COALESCE(f.product_event_share, 0) >= p.medium_product_event_share_min
             AND COALESCE(f.subclass_event_share, 0) >= p.medium_subclass_event_share_min
             AND COALESCE(f.product_event_rank, 999) <= p.medium_rank_max
             AND COALESCE(f.subclass_event_rank, 999) <= p.medium_rank_max
             AND COALESCE(f.product_positive_periodic_events, 999)
                    <= p.medium_positive_event_count_max
                THEN 'CORROBORATED_CATEGORY_SAFE_VIEWING_MISSION'
            WHEN f.viewing_mission_affinity_flag_v2_9 = 'N'
                THEN CASE
                    WHEN f.affinity_exclusion_reason_v2_9 <> ''
                        THEN CONCAT('EXCLUDED_', f.affinity_exclusion_reason_v2_9)
                    ELSE 'NO_APPROVED_CATEGORY_SAFE_VIEWING_MISSION_AFFINITY'
                END
            WHEN COALESCE(f.product_positive_periodic_events, 0)
                    > p.medium_positive_event_count_max
                THEN 'RESPONDS_TO_TOO_MANY_PERIODIC_EVENTS'
            WHEN COALESCE(f.product_event_share, 0)
                    < p.medium_product_event_share_min
                THEN 'WEAK_PRODUCT_EVENT_DOMINANCE'
            WHEN COALESCE(f.subclass_event_share, 0)
                    < p.medium_subclass_event_share_min
                THEN 'WEAK_SUBCLASS_EVENT_DOMINANCE'
            ELSE 'OTHER_REVIEW'
        END AS periodic_confidence_reason_v2_9
    FROM features f
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_9_periodic_params p
)

SELECT
    s.*,
    CASE
        WHEN s.periodic_response_confidence_v2_9 = 'HIGH'
            THEN 'PRODUCT_LEVEL_CANDIDATE'
        WHEN s.periodic_response_confidence_v2_9 = 'MEDIUM'
            THEN 'CATEGORY_LEVEL_ONLY'
        ELSE 'REVIEW_ONLY'
    END AS recommended_reporting_use_v2_9
FROM scored s;


DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_9_periodic_event_summary;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_9_periodic_event_summary
USING PARQUET AS
SELECT
    parent_occasion_key,
    parent_occasion_name,
    event_year,
    periodic_response_confidence_v2_9 AS periodic_response_confidence,
    recommended_reporting_use_v2_9   AS recommended_reporting_use,
    COUNT(*) AS products,
    SUM(trend_adjusted_incremental_baskets) AS incremental_baskets,
    SUM(trend_adjusted_incremental_sales) AS incremental_sales
FROM lab_customer_value_analytics.seasonality_v2_9_periodic_confidence
GROUP BY
    parent_occasion_key,
    parent_occasion_name,
    event_year,
    periodic_response_confidence_v2_9,
    recommended_reporting_use_v2_9;

-- =====================================================================================
-- PART N - FINAL PERIODIC ANNUAL-IDENTITY GUARD AND REPORTING POLICY (v2.10)
-- =====================================================================================
-- =====================================================================================
-- SEASONALITY THEMES v2.10 - FINAL PERIODIC EVENT REPORTING POLICY
-- Spark SQL / Databricks / Kyuubi compatible. No QUALIFY and no SELECT * EXCEPT.
--
-- Purpose
-- -------
-- v2.9 made viewing-mission affinity category-safe. Its review found a final leakage path:
-- products with an explicit annual-season identity in their product/hierarchy text can
-- still appear as HIGH or MEDIUM for a periodic event when they were not present in the
-- approved annual responder table. Examples include XMAS biscuits or HALLOWEEN
-- confectionery during The Ashes. This lightweight overlay holds those rows for review.
--
-- It does not change matched-calendar controls, trend adjustment, dominance thresholds,
-- or the annual seasonality pipeline. It remains an observed occurrence-response layer,
-- not a causal claim and not recurring seasonality.
-- =====================================================================================

DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_10_periodic_confidence;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_10_periodic_confidence
USING PARQUET AS
WITH identity_features AS (
    SELECT
        v.*,
        CASE
            WHEN v.subclass_token_text_v2_9 RLIKE ' (CHRISTMAS|XMAS|ADVENT) '
              OR v.product_token_text_v2_9 RLIKE ' (CHRISTMAS|XMAS|ADVENT CALENDAR) '
                THEN 'CHRISTMAS_NEW_YEAR'

            WHEN v.subclass_token_text_v2_9 RLIKE ' (HALLOWEEN|BONFIRE NIGHT|GUY FAWKES) '
              OR v.product_token_text_v2_9 RLIKE ' (HALLOWEEN|BONFIRE NIGHT|GUY FAWKES) '
                THEN 'HALLOWEEN_BONFIRE'

            WHEN v.subclass_token_text_v2_9 RLIKE ' (EASTER|HOT CROSS BUN|CREME EGG|MINI EASTER EGGS) '
              OR v.product_token_text_v2_9 RLIKE ' (EASTER|HOT CROSS BUN|CREME EGG) '
                THEN 'EASTER'

            WHEN v.subclass_token_text_v2_9 RLIKE ' (VALENTINE|VALENTINES|MOTHERS DAY|MOTHER S DAY|FATHERS DAY|FATHER S DAY) '
              OR v.product_token_text_v2_9 RLIKE ' (VALENTINE|VALENTINES|MOTHERS DAY|MOTHER S DAY|FATHERS DAY|FATHER S DAY) '
                THEN 'GIFTING_CELEBRATIONS'

            WHEN v.subclass_token_text_v2_9 RLIKE ' (BURNS NIGHT|HAGGIS) '
              OR v.product_token_text_v2_9 RLIKE ' (BURNS NIGHT|HAGGIS) '
                THEN 'BURNS_NIGHT'

            WHEN v.subclass_token_text_v2_9 RLIKE ' (DIWALI|PASSOVER|HANUKKAH|LUNAR NEW YEAR|CHINESE NEW YEAR) '
              OR v.product_token_text_v2_9 RLIKE ' (DIWALI|PASSOVER|HANUKKAH|LUNAR NEW YEAR|CHINESE NEW YEAR) '
                THEN 'WORLD_RELIGIOUS_CULTURAL'

            WHEN v.subclass_token_text_v2_9 RLIKE ' (PANCAKE DAY) '
              OR v.product_token_text_v2_9 RLIKE ' (PANCAKE DAY) '
                THEN 'PANCAKE_DAY'

            WHEN v.subclass_token_text_v2_9 RLIKE ' (BACK TO SCHOOL) '
              OR v.product_token_text_v2_9 RLIKE ' (BACK TO SCHOOL) '
                THEN 'BACK_TO_SCHOOL'

            WHEN v.subclass_token_text_v2_9 RLIKE ' (WORLD BOOK DAY) '
              OR v.product_token_text_v2_9 RLIKE ' (WORLD BOOK DAY) '
                THEN 'WORLD_BOOK_DAY'

            ELSE ''
        END AS explicit_annual_identity_group_v2_10
    FROM lab_customer_value_analytics.seasonality_v2_9_periodic_confidence v
),

final_policy AS (
    SELECT
        i.*,
        CASE
            WHEN i.explicit_annual_identity_group_v2_10 <> '' THEN 'Y'
            ELSE 'N'
        END AS explicit_annual_identity_flag_v2_10,

        CASE
            WHEN i.explicit_event_identity_flag = 'Y'
                THEN i.periodic_response_confidence_v2_9
            WHEN i.explicit_annual_identity_group_v2_10 <> ''
             AND i.periodic_response_confidence_v2_9 IN ('HIGH', 'MEDIUM')
                THEN 'REVIEW'
            ELSE i.periodic_response_confidence_v2_9
        END AS periodic_response_confidence_v2_10,

        CASE
            WHEN i.explicit_event_identity_flag = 'Y'
                THEN i.periodic_confidence_reason_v2_9
            WHEN i.explicit_annual_identity_group_v2_10 <> ''
             AND i.periodic_response_confidence_v2_9 IN ('HIGH', 'MEDIUM')
                THEN 'EXPLICIT_ANNUAL_SEASONAL_PRODUCT_IDENTITY'
            ELSE i.periodic_confidence_reason_v2_9
        END AS periodic_confidence_reason_v2_10
    FROM identity_features i
)

SELECT
    f.*,
    CASE
        WHEN f.periodic_response_confidence_v2_10 = 'HIGH'
            THEN 'PRODUCT_LEVEL_CANDIDATE'
        WHEN f.periodic_response_confidence_v2_10 = 'MEDIUM'
            THEN 'CATEGORY_LEVEL_ONLY'
        ELSE 'REVIEW_ONLY'
    END AS recommended_reporting_use_v2_10
FROM final_policy f;


DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_v2_10_periodic_event_summary;

CREATE TABLE lab_customer_value_analytics.seasonality_v2_10_periodic_event_summary
USING PARQUET AS
SELECT
    parent_occasion_key,
    parent_occasion_name,
    event_year,
    periodic_response_confidence_v2_10 AS periodic_response_confidence,
    recommended_reporting_use_v2_10   AS recommended_reporting_use,
    COUNT(*) AS products,
    SUM(trend_adjusted_incremental_baskets) AS incremental_baskets,
    SUM(trend_adjusted_incremental_sales) AS incremental_sales
FROM lab_customer_value_analytics.seasonality_v2_10_periodic_confidence
GROUP BY
    parent_occasion_key,
    parent_occasion_name,
    event_year,
    periodic_response_confidence_v2_10,
    recommended_reporting_use_v2_10;

-- =====================================================================================
-- END OF FINAL PRODUCTION BUILD
-- Run seasonality_themes_final_post_run_validation.sql after completion.
-- =====================================================================================
