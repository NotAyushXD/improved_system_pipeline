-- =====================================================================================
-- SEASONALITY GRAPH FEATURES v1.1 - KYUUBI FIXED
-- Spark SQL / Databricks / Kyuubi compatible (Spark 3.2+)
--
-- Prerequisite:
--   Run seasonality_themes_final_production_v2_10.sql first.
--
-- Correction in v1.1:
--   The v2.6 theme source already exposes buildup_flag. The graph phase summary also
--   created buildup_flag, and t.* carried both columns into the finalised CTE. Spark
--   therefore reported an ambiguous reference. The graph-derived column is now named
--   graph_buildup_flag internally and is exposed once as buildup_flag in the final table.
--   No thresholds, flag definitions, weights, or source methodologies have changed.
--
-- Purpose:
--   Create reusable graph/node features without replacing the signed-off annual v2.6
--   or periodic v2.10 methodologies.
--
-- Outputs:
--   1) seasonality_graph_params
--      Central, editable thresholds used only by this graph-feature layer.
--   2) seasonality_graph_identity_rules
--      Auditable seasonal-identity dictionary.
--   3) seasonality_graph_tpnb_identity
--      One row per TPNB with explicit seasonal identity evidence.
--   4) seasonality_graph_annual_relationships
--      Accepted annual relationships plus held/review relationships whose observed
--      seasonal signal must be preserved separately from occasion attribution.
--   5) seasonality_graph_tpnb_occasion_phase_features
--      One row per TPNB x annual occasion x occurrence year x phase.
--   6) seasonality_graph_tpnb_features
--      FINAL graph-node feature table: one row per TPNB.
--
-- Important:
--   * The source annual verdict is retained unchanged.
--   * Periodic/one-off events remain separate and do not overwrite annual flags.
--   * General-sales presence and seasonal behaviour are non-exclusive features.
--   * Held relationships may support a seasonal-pattern flag, but they do not receive
--     a production occasion weight.
--   * The phase table is long-form. It can later be mapped to Tesco retail periods or
--     pivoted to fixed model periods after that period calendar is agreed.
-- =====================================================================================


-- =====================================================================================
-- 0. GRAPH-FEATURE PARAMETERS
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_params;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_params
USING PARQUET AS
SELECT
    -- General-sales presence. These values are deliberately centralised for sensitivity tests.
    CAST(0.20 AS DOUBLE) AS general_basket_share_min,
    CAST(8 AS INT)       AS general_active_week_positions_min,
    CAST(2 AS INT)       AS general_active_quarters_min,
    CAST(104 AS BIGINT)  AS general_baskets_min,

    -- Seasonal-led means a material share of the product's own baskets sits in seasonal periods.
    CAST(0.50 AS DOUBLE) AS seasonal_led_basket_share_min,

    -- A phase can carry seasonal demand even when a baseline uplift cannot be measured.
    CAST(0.10 AS DOUBLE) AS phase_episode_basket_share_min,
    CAST(20 AS BIGINT)   AS phase_baskets_min,

    -- Visible post-event markdown support. This is not a complete promotion calendar.
    CAST(0.10 AS DOUBLE) AS post_event_price_drop_sales_share_min,

    -- Minimum product evidence for the broader time-of-year pattern route.
    CAST(104 AS BIGINT)  AS min_profile_baskets,

    CURRENT_DATE()       AS feature_as_of_date;


-- =====================================================================================
-- 1. EDITABLE SEASONAL-IDENTITY RULES
--    Identity is evidence about what a product is. It does not, by itself, create a
--    seasonal-pattern flag; observed timing or an accepted annual relationship is also needed.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_identity_rules;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_identity_rules
USING PARQUET AS
SELECT *
FROM VALUES
    ('CHRISTMAS_DAY', 'CHRISTMAS_NEW_YEAR', 'WINTER', 'ANY_TEXT', 'CHRISTMAS', 'HARD'),
    ('CHRISTMAS_DAY', 'CHRISTMAS_NEW_YEAR', 'WINTER', 'ANY_TEXT', 'XMAS', 'HARD'),
    ('CHRISTMAS_DAY', 'CHRISTMAS_NEW_YEAR', 'WINTER', 'ANY_TEXT', 'ADVENT', 'HARD'),
    ('CHRISTMAS_DAY', 'CHRISTMAS_NEW_YEAR', 'WINTER', 'ANY_TEXT', 'MINCE PIE', 'HARD'),
    ('CHRISTMAS_DAY', 'CHRISTMAS_NEW_YEAR', 'WINTER', 'ANY_TEXT', 'MINCE PIES', 'HARD'),
    ('CHRISTMAS_DAY', 'CHRISTMAS_NEW_YEAR', 'WINTER', 'ANY_TEXT', 'PANETTONE', 'HARD'),
    ('CHRISTMAS_DAY', 'CHRISTMAS_NEW_YEAR', 'WINTER', 'ANY_TEXT', 'STOLLEN', 'HARD'),
    ('CHRISTMAS_DAY', 'CHRISTMAS_NEW_YEAR', 'WINTER', 'ANY_TEXT', 'PIGS IN BLANKETS', 'HARD'),

    ('HALLOWEEN', 'HALLOWEEN_BONFIRE', 'AUTUMN', 'ANY_TEXT', 'HALLOWEEN', 'HARD'),
    ('HALLOWEEN', 'HALLOWEEN_BONFIRE', 'AUTUMN', 'ANY_TEXT', 'JAFFAWEEN', 'HARD'),
    ('HALLOWEEN', 'HALLOWEEN_BONFIRE', 'AUTUMN', 'ANY_TEXT', 'TRICK OR TREAT', 'HARD'),
    ('HALLOWEEN', 'HALLOWEEN_BONFIRE', 'AUTUMN', 'HIERARCHY_TEXT', 'HALLOWEEN', 'HARD'),
    ('BONFIRE_NIGHT', 'HALLOWEEN_BONFIRE', 'AUTUMN', 'ANY_TEXT', 'BONFIRE', 'HARD'),
    ('BONFIRE_NIGHT', 'HALLOWEEN_BONFIRE', 'AUTUMN', 'HIERARCHY_TEXT', 'FIREWORK', 'SOFT'),
    ('BONFIRE_NIGHT', 'HALLOWEEN_BONFIRE', 'AUTUMN', 'HIERARCHY_TEXT', 'FIREWORKS', 'SOFT'),
    ('BONFIRE_NIGHT', 'HALLOWEEN_BONFIRE', 'AUTUMN', 'HIERARCHY_TEXT', 'SPARKLER', 'SOFT'),
    ('BONFIRE_NIGHT', 'HALLOWEEN_BONFIRE', 'AUTUMN', 'HIERARCHY_TEXT', 'SPARKLERS', 'SOFT'),

    ('EASTER_SUNDAY', 'EASTER', 'SPRING', 'ANY_TEXT', 'EASTER', 'HARD'),
    ('EASTER_SUNDAY', 'EASTER', 'SPRING', 'ANY_TEXT', 'HOT CROSS', 'HARD'),
    ('EASTER_SUNDAY', 'EASTER', 'SPRING', 'ANY_TEXT', 'CREME EGG', 'HARD'),
    ('EASTER_SUNDAY', 'EASTER', 'SPRING', 'ANY_TEXT', 'CREME EGGS', 'HARD'),
    ('EASTER_SUNDAY', 'EASTER', 'SPRING', 'ANY_TEXT', 'MINI EGG', 'HARD'),
    ('EASTER_SUNDAY', 'EASTER', 'SPRING', 'ANY_TEXT', 'MINI EGGS', 'HARD'),
    ('EASTER_SUNDAY', 'EASTER', 'SPRING', 'ANY_TEXT', 'SIMNEL', 'HARD'),

    ('VALENTINES_GALENTINES', 'GIFTING_CELEBRATIONS', 'WINTER', 'ANY_TEXT', 'VALENTINE', 'HARD'),
    ('VALENTINES_GALENTINES', 'GIFTING_CELEBRATIONS', 'WINTER', 'ANY_TEXT', 'VALENTINES', 'HARD'),
    ('MOTHERS_DAY', 'GIFTING_CELEBRATIONS', 'SPRING', 'ANY_TEXT', 'MOTHERS DAY', 'HARD'),
    ('MOTHERS_DAY', 'GIFTING_CELEBRATIONS', 'SPRING', 'ANY_TEXT', 'MOTHER S DAY', 'HARD'),
    ('FATHERS_DAY', 'GIFTING_CELEBRATIONS', 'SUMMER', 'ANY_TEXT', 'FATHERS DAY', 'HARD'),
    ('FATHERS_DAY', 'GIFTING_CELEBRATIONS', 'SUMMER', 'ANY_TEXT', 'FATHER S DAY', 'HARD'),

    ('BURNS_NIGHT', 'NATIONAL_DAYS', 'WINTER', 'ANY_TEXT', 'HAGGIS', 'HARD'),
    ('DIWALI', 'WORLD_RELIGIOUS_CULTURAL', 'AUTUMN', 'ANY_TEXT', 'DIWALI', 'HARD'),
    ('PASSOVER', 'WORLD_RELIGIOUS_CULTURAL', 'SPRING', 'ANY_TEXT', 'PASSOVER', 'HARD'),
    ('PASSOVER', 'WORLD_RELIGIOUS_CULTURAL', 'SPRING', 'ANY_TEXT', 'MATZO', 'HARD'),
    ('LUNAR_NEW_YEAR', 'WORLD_RELIGIOUS_CULTURAL', 'WINTER', 'ANY_TEXT', 'LUNAR NEW YEAR', 'HARD'),
    ('LUNAR_NEW_YEAR', 'WORLD_RELIGIOUS_CULTURAL', 'WINTER', 'ANY_TEXT', 'CHINESE NEW YEAR', 'HARD'),
    ('HANUKKAH', 'WORLD_RELIGIOUS_CULTURAL', 'WINTER', 'ANY_TEXT', 'HANUKKAH', 'HARD'),

    ('PANCAKE_DAY', 'FOOD_MOMENTS', 'WINTER', 'ANY_TEXT', 'PANCAKE DAY', 'HARD'),
    ('BACK_TO_SCHOOL_UNIVERSITY', 'RETAIL_EVENTS', 'AUTUMN', 'ANY_TEXT', 'BACK TO SCHOOL', 'HARD'),
    ('WORLD_BOOK_DAY_UK_IRELAND', 'AWARENESS_DAYS', 'SPRING', 'ANY_TEXT', 'WORLD BOOK DAY', 'HARD')
AS r(identity_occasion_key, identity_family, identity_season, match_scope, match_pattern, identity_strength);


-- =====================================================================================
-- 2. TPNB IDENTITY FEATURES (one row per TPNB)
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_tpnb_identity;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_tpnb_identity
USING PARQUET AS
WITH base AS (
    SELECT
        t.tpnb,
        t.product_description,
        t.subcategory_code,
        t.subcategory_name,
        CONCAT(' ', TRIM(REGEXP_REPLACE(UPPER(COALESCE(t.product_description, '')), '[^A-Z0-9]+', ' ')), ' ') AS product_text,
        CONCAT(' ', TRIM(REGEXP_REPLACE(UPPER(CONCAT_WS(
            ' ',
            COALESCE(t.hierarchy_department_name, ''),
            COALESCE(t.hierarchy_section_name, ''),
            COALESCE(t.hierarchy_class_name, ''),
            COALESCE(t.subcategory_name, '')
        )), '[^A-Z0-9]+', ' ')), ' ') AS hierarchy_text,
        CONCAT(' ', TRIM(REGEXP_REPLACE(UPPER(CONCAT_WS(
            ' ',
            COALESCE(t.product_description, ''),
            COALESCE(t.hierarchy_department_name, ''),
            COALESCE(t.hierarchy_section_name, ''),
            COALESCE(t.hierarchy_class_name, ''),
            COALESCE(t.subcategory_name, '')
        )), '[^A-Z0-9]+', ' ')), ' ') AS any_text
    FROM lab_customer_value_analytics.seasonality_v2_6_theme_output t
),
matched AS (
    SELECT /*+ BROADCAST(r) */
        b.tpnb,
        r.identity_occasion_key,
        r.identity_family,
        r.identity_season,
        r.identity_strength,
        r.match_pattern
    FROM base b
    JOIN lab_customer_value_analytics.seasonality_graph_identity_rules r
      ON (r.match_scope = 'ANY_TEXT' AND b.any_text LIKE CONCAT('% ', r.match_pattern, ' %'))
      OR (r.match_scope = 'PRODUCT_TEXT' AND b.product_text LIKE CONCAT('% ', r.match_pattern, ' %'))
      OR (r.match_scope = 'HIERARCHY_TEXT' AND b.hierarchy_text LIKE CONCAT('% ', r.match_pattern, ' %'))
),
agg AS (
    SELECT
        tpnb,
        COUNT(DISTINCT identity_occasion_key) AS explicit_identity_count,
        MAX(CASE WHEN identity_strength = 'HARD' THEN 1 ELSE 0 END) AS hard_identity_flag,
        SORT_ARRAY(COLLECT_SET(identity_occasion_key)) AS identity_occasion_array,
        SORT_ARRAY(COLLECT_SET(identity_family)) AS identity_family_array,
        SORT_ARRAY(COLLECT_SET(identity_season)) AS identity_season_array,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(identity_occasion_key))) AS explicit_identity_occasion_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(identity_family))) AS explicit_identity_family_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(identity_season))) AS explicit_identity_season_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(match_pattern))) AS matched_identity_patterns
    FROM matched
    GROUP BY tpnb
)
SELECT
    b.tpnb,
    CASE WHEN a.tpnb IS NOT NULL THEN 1 ELSE 0 END AS explicit_seasonal_identity_flag,
    COALESCE(a.hard_identity_flag, 0) AS hard_seasonal_identity_flag,
    COALESCE(a.explicit_identity_count, 0) AS explicit_identity_count,
    a.identity_occasion_array,
    a.identity_family_array,
    a.identity_season_array,
    COALESCE(a.explicit_identity_occasion_list, '') AS explicit_identity_occasion_list,
    COALESCE(a.explicit_identity_family_list, '') AS explicit_identity_family_list,
    COALESCE(a.explicit_identity_season_list, '') AS explicit_identity_season_list,
    COALESCE(a.matched_identity_patterns, '') AS matched_identity_patterns
FROM base b
LEFT JOIN agg a
  ON b.tpnb = a.tpnb;


-- =====================================================================================
-- 3. ANNUAL RELATIONSHIPS USED BY THE GRAPH LAYER
--    ACCEPTED: production occasion relationship from v2.6.
--    HELD_REVIEW: observed seasonal relationship retained for timing/pattern evidence,
--                 but not used as an accepted named-occasion weight.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_annual_relationships;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_annual_relationships
USING PARQUET AS
SELECT
    p.tpnb,
    p.parent_occasion_key,
    p.parent_occasion_name,
    p.occasion_family,
    p.resolved_occasion_verdict_v2_4 AS evidence_occasion_verdict,
    CASE
        WHEN p.resolved_occasion_verdict_v2_4 IN ('Specifically seasonal', 'Supported specifically seasonal')
            THEN 'Specifically seasonal'
        WHEN p.resolved_occasion_verdict_v2_4 IN ('Uplift seasonal', 'Supported uplift seasonal')
            THEN 'Uplift seasonal'
    END AS graph_occasion_verdict,
    CASE WHEN p.accepted_final_annual THEN 'ACCEPTED' ELSE 'HELD_REVIEW' END AS relationship_status,
    CASE
        WHEN p.accepted_final_annual THEN 'CONFIDENT'
        WHEN p.final_annual_policy_reason = 'HOLD_CONFLICTING_PRODUCT_IDENTITY'
            THEN 'CONFLICTING_PRODUCT_IDENTITY'
        WHEN p.final_annual_policy_reason = 'HOLD_MINOR_NO_APPROVED_AFFINITY'
            THEN 'SEASONAL_PERIOD_WITHOUT_APPROVED_NAMED_OCCASION'
        ELSE 'REVIEW_REQUIRED'
    END AS occasion_attribution_status,
    p.final_annual_policy_reason,
    p.identity_anchor_parent_v2_6,
    p.identity_anchor_family_v2_6,
    p.identity_conflict_flag_v2_6,
    p.matched_affinity_patterns,
    p.product_parent_signal_share,
    p.subclass_parent_signal_share,
    p.product_parent_signal_rank,
    p.subclass_parent_signal_rank,
    p.evidence_source_v2_4
FROM lab_customer_value_analytics.seasonality_v2_6_product_occasion_policy p
WHERE p.accepted_final_annual
   OR (
        p.final_annual_policy_reason LIKE 'HOLD_%'
        AND p.resolved_occasion_verdict_v2_4 IN (
            'Specifically seasonal', 'Supported specifically seasonal',
            'Uplift seasonal', 'Supported uplift seasonal'
        )
   );


-- =====================================================================================
-- 4. ANNUAL OCCASION-PHASE FEATURES
--    Grain: TPNB x parent occasion x event year x phase.
--    Phases: BUILDUP, PRE_EVENT, EVENT, POST_EVENT.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_tpnb_occasion_phase_features;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_tpnb_occasion_phase_features
USING PARQUET AS
WITH prm AS (
    SELECT
        g.*,
        p.min_baseline_days,
        p.min_expected_baskets_window,
        p.uplift_index_min,
        p.z_min
    FROM lab_customer_value_analytics.seasonality_graph_params g
    CROSS JOIN lab_customer_value_analytics.seasonality_v2_params p
),
relations AS (
    SELECT *
    FROM lab_customer_value_analytics.seasonality_graph_annual_relationships
),
phase_metric_base AS (
    -- Data-driven build-up occurrence. Its event window is the build-up itself.
    SELECT
        r.tpnb,
        r.parent_occasion_key,
        r.parent_occasion_name,
        r.occasion_family,
        r.graph_occasion_verdict,
        r.evidence_occasion_verdict,
        r.relationship_status,
        r.occasion_attribution_status,
        r.final_annual_policy_reason,
        r.evidence_source_v2_4,
        m.occasion_key,
        m.occurrence_id,
        m.event_year,
        'BUILDUP' AS season_phase,
        m.event_start_date AS phase_start_date,
        m.event_end_date AS phase_end_date,
        m.eligibility_status,
        m.baseline_days_available,
        m.event_baskets AS metric_actual_baskets,
        m.event_expected_baskets AS expected_baskets,
        m.event_share_index AS basket_share_index,
        m.event_z AS z_score,
        m.event_sales AS metric_actual_sales,
        m.event_uplift AS source_phase_uplift_flag,
        m.is_specific_year AS source_specific_year_flag,
        m.annual_baskets,
        m.annual_sales,
        m.core_expected_sales / NULLIF(m.core_expected_baskets, 0) AS baseline_sales_per_expected_basket
    FROM relations r
    JOIN lab_customer_value_analytics.seasonality_v2_occasion_metrics m
      ON m.entity_level = 'TPNB'
     AND m.entity_id = r.tpnb
     AND m.parent_occasion_key = r.parent_occasion_key
     AND m.window_type = 'BUILDUP'
    WHERE m.event_end_date <= CURRENT_DATE()

    UNION ALL

    -- Fixed pre-event period.
    SELECT
        r.tpnb,
        r.parent_occasion_key,
        r.parent_occasion_name,
        r.occasion_family,
        r.graph_occasion_verdict,
        r.evidence_occasion_verdict,
        r.relationship_status,
        r.occasion_attribution_status,
        r.final_annual_policy_reason,
        r.evidence_source_v2_4,
        m.occasion_key,
        m.occurrence_id,
        m.event_year,
        'PRE_EVENT' AS season_phase,
        m.pre_start_date AS phase_start_date,
        DATE_SUB(m.event_start_date, 1) AS phase_end_date,
        m.eligibility_status,
        m.baseline_days_available,
        m.pre_baskets AS metric_actual_baskets,
        m.pre_expected_baskets AS expected_baskets,
        m.pre_share_index AS basket_share_index,
        m.pre_z AS z_score,
        m.pre_sales AS metric_actual_sales,
        m.pre_uplift AS source_phase_uplift_flag,
        m.is_specific_year AS source_specific_year_flag,
        m.annual_baskets,
        m.annual_sales,
        m.core_expected_sales / NULLIF(m.core_expected_baskets, 0) AS baseline_sales_per_expected_basket
    FROM relations r
    JOIN lab_customer_value_analytics.seasonality_v2_occasion_metrics m
      ON m.entity_level = 'TPNB'
     AND m.entity_id = r.tpnb
     AND m.parent_occasion_key = r.parent_occasion_key
     AND m.window_type = 'EVENT'
    WHERE m.event_end_date <= CURRENT_DATE()
      AND m.pre_start_date < m.event_start_date

    UNION ALL

    -- Occasion/event period.
    SELECT
        r.tpnb,
        r.parent_occasion_key,
        r.parent_occasion_name,
        r.occasion_family,
        r.graph_occasion_verdict,
        r.evidence_occasion_verdict,
        r.relationship_status,
        r.occasion_attribution_status,
        r.final_annual_policy_reason,
        r.evidence_source_v2_4,
        m.occasion_key,
        m.occurrence_id,
        m.event_year,
        'EVENT' AS season_phase,
        m.event_start_date AS phase_start_date,
        m.event_end_date AS phase_end_date,
        m.eligibility_status,
        m.baseline_days_available,
        m.event_baskets AS metric_actual_baskets,
        m.event_expected_baskets AS expected_baskets,
        m.event_share_index AS basket_share_index,
        m.event_z AS z_score,
        m.event_sales AS metric_actual_sales,
        m.event_uplift AS source_phase_uplift_flag,
        m.is_specific_year AS source_specific_year_flag,
        m.annual_baskets,
        m.annual_sales,
        m.core_expected_sales / NULLIF(m.core_expected_baskets, 0) AS baseline_sales_per_expected_basket
    FROM relations r
    JOIN lab_customer_value_analytics.seasonality_v2_occasion_metrics m
      ON m.entity_level = 'TPNB'
     AND m.entity_id = r.tpnb
     AND m.parent_occasion_key = r.parent_occasion_key
     AND m.window_type = 'EVENT'
    WHERE m.event_end_date <= CURRENT_DATE()

    UNION ALL

    -- Configured post-event period.
    SELECT
        r.tpnb,
        r.parent_occasion_key,
        r.parent_occasion_name,
        r.occasion_family,
        r.graph_occasion_verdict,
        r.evidence_occasion_verdict,
        r.relationship_status,
        r.occasion_attribution_status,
        r.final_annual_policy_reason,
        r.evidence_source_v2_4,
        m.occasion_key,
        m.occurrence_id,
        m.event_year,
        'POST_EVENT' AS season_phase,
        DATE_ADD(m.event_end_date, 1) AS phase_start_date,
        m.post_end_date AS phase_end_date,
        m.eligibility_status,
        m.baseline_days_available,
        m.post_baskets AS metric_actual_baskets,
        m.post_expected_baskets AS expected_baskets,
        m.post_share_index AS basket_share_index,
        m.post_z AS z_score,
        m.post_sales AS metric_actual_sales,
        m.post_uplift AS source_phase_uplift_flag,
        CAST(FALSE AS BOOLEAN) AS source_specific_year_flag,
        m.annual_baskets,
        m.annual_sales,
        m.core_expected_sales / NULLIF(m.core_expected_baskets, 0) AS baseline_sales_per_expected_basket
    FROM relations r
    JOIN lab_customer_value_analytics.seasonality_v2_occasion_metrics m
      ON m.entity_level = 'TPNB'
     AND m.entity_id = r.tpnb
     AND m.parent_occasion_key = r.parent_occasion_key
     AND m.window_type = 'EVENT'
    WHERE m.event_end_date <= CURRENT_DATE()
      AND m.post_end_date > m.event_end_date
),
phase_dates AS (
    SELECT
        p.*,
        x.phase_date
    FROM phase_metric_base p
    LATERAL VIEW EXPLODE(SEQUENCE(p.phase_start_date, p.phase_end_date)) x AS phase_date
),
spells AS (
    SELECT
        entity_id AS tpnb,
        COLLECT_LIST(NAMED_STRUCT('s', spell_start, 'e', spell_end)) AS spells
    FROM lab_customer_value_analytics.seasonality_v2_availability_spells
    WHERE entity_level = 'TPNB'
    GROUP BY entity_id
),
price_drops AS (
    SELECT
        tpnb,
        COLLECT_LIST(NAMED_STRUCT('s', price_drop_start, 'e', price_drop_end)) AS price_drop_periods
    FROM lab_customer_value_analytics.seasonality_v2_6_price_drop_periods
    GROUP BY tpnb
),
day_enriched AS (
    SELECT /*+ BROADCAST(s, pr) */
        d.*,
        COALESCE(x.baskets, 0) AS daily_baskets,
        COALESCE(x.sales, CAST(0 AS DOUBLE)) AS daily_sales,
        COALESCE(EXISTS(s.spells, q -> d.phase_date BETWEEN q.s AND q.e), FALSE) AS on_sale,
        COALESCE(EXISTS(pr.price_drop_periods, q -> d.phase_date BETWEEN q.s AND q.e), FALSE) AS on_price_drop
    FROM phase_dates d
    LEFT JOIN lab_customer_value_analytics.seasonality_v2_entity_daily x
      ON x.entity_level = 'TPNB'
     AND x.entity_id = d.tpnb
     AND x.sale_date = d.phase_date
    LEFT JOIN spells s
      ON s.tpnb = d.tpnb
    LEFT JOIN price_drops pr
      ON pr.tpnb = d.tpnb
),
day_stats AS (
    SELECT
        tpnb,
        parent_occasion_key,
        parent_occasion_name,
        occasion_family,
        graph_occasion_verdict,
        evidence_occasion_verdict,
        relationship_status,
        occasion_attribution_status,
        final_annual_policy_reason,
        evidence_source_v2_4,
        occasion_key,
        occurrence_id,
        event_year,
        season_phase,
        phase_start_date,
        phase_end_date,
        eligibility_status,
        baseline_days_available,
        MAX(expected_baskets) AS expected_baskets,
        MAX(basket_share_index) AS basket_share_index,
        MAX(z_score) AS z_score,
        MAX(CASE WHEN source_phase_uplift_flag THEN 1 ELSE 0 END) AS source_phase_uplift_flag,
        MAX(CASE WHEN source_specific_year_flag THEN 1 ELSE 0 END) AS source_specific_year_flag,
        MAX(annual_baskets) AS annual_baskets,
        MAX(annual_sales) AS annual_sales,
        MAX(baseline_sales_per_expected_basket) AS baseline_sales_per_expected_basket,
        COUNT(*) AS phase_calendar_days,
        SUM(CASE WHEN on_sale THEN 1 ELSE 0 END) AS on_sale_days,
        SUM(daily_baskets) AS actual_baskets,
        SUM(daily_sales) AS actual_sales,
        SUM(CASE WHEN on_price_drop THEN 1 ELSE 0 END) AS price_drop_days,
        SUM(CASE WHEN on_price_drop THEN daily_sales ELSE CAST(0 AS DOUBLE) END) AS price_drop_sales
    FROM day_enriched
    GROUP BY
        tpnb,
        parent_occasion_key,
        parent_occasion_name,
        occasion_family,
        graph_occasion_verdict,
        evidence_occasion_verdict,
        relationship_status,
        occasion_attribution_status,
        final_annual_policy_reason,
        evidence_source_v2_4,
        occasion_key,
        occurrence_id,
        event_year,
        season_phase,
        phase_start_date,
        phase_end_date,
        eligibility_status,
        baseline_days_available
),
calculated AS (
    SELECT
        d.*,
        d.expected_baskets * d.baseline_sales_per_expected_basket AS expected_sales,
        d.actual_baskets - d.expected_baskets AS incremental_baskets,
        d.actual_sales - d.expected_baskets * d.baseline_sales_per_expected_basket AS incremental_sales,
        d.actual_sales / NULLIF(d.on_sale_days, 0) AS sales_per_on_sale_day,
        d.actual_baskets / NULLIF(d.on_sale_days, 0) AS baskets_per_on_sale_day,
        100.0 * d.on_sale_days / NULLIF(d.phase_calendar_days, 0) AS on_sale_day_coverage_pct,
        100.0 * d.actual_sales / NULLIF(d.annual_sales, 0) AS phase_sales_share_of_year_pct,
        CASE
            WHEN d.eligibility_status = 'ELIGIBLE'
             AND d.baseline_days_available >= p.min_baseline_days
             AND d.expected_baskets >= p.min_expected_baskets_window
             AND d.basket_share_index >= p.uplift_index_min
             AND d.z_score >= p.z_min
                THEN 1 ELSE 0
        END AS measured_uplift_flag,
        CASE
            WHEN d.expected_baskets < p.min_expected_baskets_window THEN 'LOW_EXPECTED_VOLUME'
            WHEN d.baseline_days_available < p.min_baseline_days THEN 'INSUFFICIENT_BASELINE'
            WHEN d.basket_share_index >= p.uplift_index_min AND d.z_score >= p.z_min THEN 'SIGNIFICANT_UPLIFT'
            WHEN d.basket_share_index <= 1.0 / p.uplift_index_min AND d.z_score <= -p.z_min THEN 'SIGNIFICANT_DECLINE'
            ELSE 'NOT_SIGNIFICANT'
        END AS phase_evidence_status
    FROM day_stats d
    CROSS JOIN prm p
),
episode_totals AS (
    SELECT
        tpnb,
        parent_occasion_key,
        event_year,
        SUM(actual_baskets) AS episode_baskets,
        SUM(actual_sales) AS episode_sales
    FROM calculated
    GROUP BY tpnb, parent_occasion_key, event_year
),
with_materiality AS (
    SELECT
        c.*,
        e.episode_baskets,
        e.episode_sales,
        100.0 * c.actual_baskets / NULLIF(e.episode_baskets, 0) AS phase_basket_share_of_episode_pct,
        100.0 * c.actual_sales / NULLIF(e.episode_sales, 0) AS phase_sales_share_of_episode_pct,
        CASE
            WHEN c.actual_baskets >= p.phase_baskets_min
             AND c.actual_baskets / NULLIF(e.episode_baskets, 0) >= p.phase_episode_basket_share_min
                THEN 1 ELSE 0
        END AS material_phase_demand_flag,
        CASE
            WHEN c.measured_uplift_flag = 1
              OR (
                    c.actual_baskets >= p.phase_baskets_min
                    AND c.actual_baskets / NULLIF(e.episode_baskets, 0) >= p.phase_episode_basket_share_min
                 )
                THEN 1 ELSE 0
        END AS period_seasonal_flag,
        CASE
            WHEN c.season_phase = 'POST_EVENT'
             AND c.price_drop_sales / NULLIF(c.actual_sales, 0) >= p.post_event_price_drop_sales_share_min
                THEN 1 ELSE 0
        END AS post_event_clearance_flag
    FROM calculated c
    JOIN episode_totals e
      ON c.tpnb = e.tpnb
     AND c.parent_occasion_key = e.parent_occasion_key
     AND c.event_year = e.event_year
    CROSS JOIN prm p
),
weight_totals AS (
    SELECT
        tpnb,
        SUM(CASE WHEN relationship_status = 'ACCEPTED' THEN GREATEST(incremental_baskets, CAST(0 AS DOUBLE)) ELSE 0 END)
            AS accepted_positive_incremental_baskets,
        SUM(CASE WHEN relationship_status = 'ACCEPTED' THEN GREATEST(incremental_sales, CAST(0 AS DOUBLE)) ELSE 0 END)
            AS accepted_positive_incremental_sales,
        SUM(GREATEST(incremental_baskets, CAST(0 AS DOUBLE))) AS candidate_positive_incremental_baskets,
        SUM(GREATEST(incremental_sales, CAST(0 AS DOUBLE))) AS candidate_positive_incremental_sales
    FROM with_materiality
    GROUP BY tpnb
)
SELECT
    m.tpnb,
    m.parent_occasion_key,
    m.parent_occasion_name,
    m.occasion_family,
    m.graph_occasion_verdict,
    m.evidence_occasion_verdict,
    m.relationship_status,
    m.occasion_attribution_status,
    m.final_annual_policy_reason,
    m.evidence_source_v2_4,
    m.occasion_key,
    m.occurrence_id,
    m.event_year,
    m.season_phase,
    CONCAT(m.parent_occasion_key, '__', CAST(m.event_year AS STRING), '__', m.season_phase) AS period_id,
    m.phase_start_date,
    m.phase_end_date,
    m.phase_calendar_days,
    m.on_sale_days,
    m.on_sale_day_coverage_pct,
    CASE WHEN m.actual_baskets > 0 THEN 1 ELSE 0 END AS sold_flag,
    m.actual_baskets,
    m.expected_baskets,
    m.basket_share_index,
    m.z_score,
    m.phase_evidence_status,
    m.measured_uplift_flag,
    m.material_phase_demand_flag,
    m.period_seasonal_flag,
    m.source_specific_year_flag,
    m.actual_sales,
    m.expected_sales,
    m.incremental_baskets,
    m.incremental_sales,
    m.sales_per_on_sale_day,
    m.baskets_per_on_sale_day,
    m.phase_sales_share_of_year_pct,
    m.phase_basket_share_of_episode_pct,
    m.phase_sales_share_of_episode_pct,
    CASE WHEN m.relationship_status = 'ACCEPTED' THEN 1 ELSE 0 END AS accepted_occasion_weight_eligible_flag,
    CASE
        WHEN m.relationship_status = 'ACCEPTED'
            THEN GREATEST(m.incremental_baskets, CAST(0 AS DOUBLE))
                 / NULLIF(w.accepted_positive_incremental_baskets, 0)
    END AS period_demand_weight,
    CASE
        WHEN m.relationship_status = 'ACCEPTED'
            THEN GREATEST(m.incremental_sales, CAST(0 AS DOUBLE))
                 / NULLIF(w.accepted_positive_incremental_sales, 0)
    END AS period_value_weight,
    GREATEST(m.incremental_baskets, CAST(0 AS DOUBLE))
        / NULLIF(w.candidate_positive_incremental_baskets, 0) AS candidate_period_demand_weight,
    GREATEST(m.incremental_sales, CAST(0 AS DOUBLE))
        / NULLIF(w.candidate_positive_incremental_sales, 0) AS candidate_period_value_weight,
    m.price_drop_days,
    m.price_drop_sales,
    100.0 * m.price_drop_sales / NULLIF(m.actual_sales, 0) AS price_drop_sales_share_pct,
    m.post_event_clearance_flag,
    m.annual_baskets,
    m.annual_sales
FROM with_materiality m
JOIN weight_totals w
  ON m.tpnb = w.tpnb;


-- =====================================================================================
-- 5. FINAL TPNB GRAPH-NODE FEATURES (one row per TPNB)
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_tpnb_features;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_tpnb_features
USING PARQUET AS
WITH prm AS (
    SELECT * FROM lab_customer_value_analytics.seasonality_graph_params
),
phase_summary AS (
    SELECT
        tpnb,
        MAX(CASE WHEN period_seasonal_flag = 1 AND season_phase IN ('BUILDUP', 'PRE_EVENT') THEN 1 ELSE 0 END)
            AS season_before_flag,
        MAX(CASE WHEN period_seasonal_flag = 1 AND season_phase = 'BUILDUP' THEN 1 ELSE 0 END)
            AS buildup_flag,
        MAX(CASE WHEN period_seasonal_flag = 1 AND season_phase = 'PRE_EVENT' THEN 1 ELSE 0 END)
            AS pre_event_flag,
        MAX(CASE WHEN period_seasonal_flag = 1 AND season_phase = 'EVENT' THEN 1 ELSE 0 END)
            AS season_event_flag,
        MAX(CASE WHEN period_seasonal_flag = 1 AND season_phase = 'POST_EVENT' THEN 1 ELSE 0 END)
            AS season_after_flag,
        MAX(post_event_clearance_flag) AS post_event_clearance_flag,
        MAX(measured_uplift_flag) AS phase_measured_uplift_flag,
        COUNT(DISTINCT CASE WHEN period_seasonal_flag = 1 THEN parent_occasion_key END) AS named_seasonal_peak_count,
        COUNT(DISTINCT CASE WHEN relationship_status = 'ACCEPTED' THEN parent_occasion_key END) AS accepted_occasion_count,
        COUNT(DISTINCT CASE WHEN relationship_status = 'HELD_REVIEW' THEN parent_occasion_key END) AS held_occasion_count,
        COUNT(DISTINCT CASE
            WHEN relationship_status = 'ACCEPTED'
             AND graph_occasion_verdict = 'Specifically seasonal' THEN parent_occasion_key END) AS accepted_specific_occasion_count,
        COUNT(DISTINCT CASE
            WHEN relationship_status = 'HELD_REVIEW'
             AND graph_occasion_verdict = 'Specifically seasonal' THEN parent_occasion_key END) AS held_specific_occasion_count,
        MAX(CASE WHEN relationship_status = 'HELD_REVIEW'
                  AND occasion_attribution_status = 'CONFLICTING_PRODUCT_IDENTITY' THEN 1 ELSE 0 END)
            AS occasion_identity_conflict_flag,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(CASE WHEN relationship_status = 'ACCEPTED' THEN parent_occasion_name END)))
            AS accepted_occasion_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(CASE WHEN relationship_status = 'HELD_REVIEW' THEN parent_occasion_name END)))
            AS held_candidate_occasion_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(CASE WHEN period_seasonal_flag = 1 THEN season_phase END)))
            AS seasonal_phase_list,
        SUM(CASE WHEN period_seasonal_flag = 1 THEN actual_baskets ELSE 0 END) AS named_seasonal_baskets,
        SUM(CASE WHEN period_seasonal_flag = 1 THEN actual_sales ELSE CAST(0 AS DOUBLE) END) AS named_seasonal_sales,
        SUM(CASE WHEN period_seasonal_flag = 1 AND relationship_status = 'ACCEPTED'
                 THEN GREATEST(incremental_baskets, CAST(0 AS DOUBLE)) ELSE 0 END)
            AS accepted_positive_seasonal_incremental_baskets,
        SUM(CASE WHEN period_seasonal_flag = 1 AND relationship_status = 'ACCEPTED'
                 THEN GREATEST(incremental_sales, CAST(0 AS DOUBLE)) ELSE 0 END)
            AS accepted_positive_seasonal_incremental_sales,
        SUM(CASE WHEN period_seasonal_flag = 1 AND relationship_status = 'HELD_REVIEW'
                 THEN GREATEST(incremental_baskets, CAST(0 AS DOUBLE)) ELSE 0 END)
            AS held_positive_seasonal_incremental_baskets,
        SUM(CASE WHEN period_seasonal_flag = 1 AND relationship_status = 'HELD_REVIEW'
                 THEN GREATEST(incremental_sales, CAST(0 AS DOUBLE)) ELSE 0 END)
            AS held_positive_seasonal_incremental_sales,
        MAX(CASE WHEN period_seasonal_flag = 1 THEN basket_share_index END) AS max_seasonal_basket_share_index,
        MAX(CASE WHEN period_seasonal_flag = 1 THEN z_score END) AS max_seasonal_z_score
    FROM lab_customer_value_analytics.seasonality_graph_tpnb_occasion_phase_features
    GROUP BY tpnb
),
seasonal_dates AS (
    SELECT DISTINCT
        f.tpnb,
        x.seasonal_date
    FROM lab_customer_value_analytics.seasonality_graph_tpnb_occasion_phase_features f
    LATERAL VIEW EXPLODE(SEQUENCE(f.phase_start_date, f.phase_end_date)) x AS seasonal_date
    WHERE f.period_seasonal_flag = 1
),
daily_summary AS (
    SELECT
        d.entity_id AS tpnb,
        SUM(d.baskets) AS daily_total_baskets,
        SUM(d.sales) AS daily_total_sales,
        COUNT(DISTINCT LEAST(52, CAST(CEIL(DAYOFYEAR(d.sale_date) / 7.0) AS INT))) AS active_week_positions,
        COUNT(DISTINCT QUARTER(d.sale_date)) AS active_quarters,
        SUM(CASE WHEN s.seasonal_date IS NULL THEN d.baskets ELSE 0 END) AS named_general_baskets,
        SUM(CASE WHEN s.seasonal_date IS NULL THEN d.sales ELSE CAST(0 AS DOUBLE) END) AS named_general_sales,
        COUNT(DISTINCT CASE WHEN s.seasonal_date IS NULL
            THEN LEAST(52, CAST(CEIL(DAYOFYEAR(d.sale_date) / 7.0) AS INT)) END) AS general_active_week_positions,
        COUNT(DISTINCT CASE WHEN s.seasonal_date IS NULL THEN QUARTER(d.sale_date) END) AS general_active_quarters,
        COUNT(DISTINCT CASE WHEN s.seasonal_date IS NULL THEN d.sale_date END) AS general_active_days
    FROM lab_customer_value_analytics.seasonality_v2_entity_daily d
    LEFT JOIN seasonal_dates s
      ON d.entity_id = s.tpnb
     AND d.sale_date = s.seasonal_date
    WHERE d.entity_level = 'TPNB'
    GROUP BY d.entity_id
),
periodic AS (
    SELECT
        CAST(tpnb AS STRING) AS tpnb,
        MAX(CASE WHEN periodic_response_confidence_v2_10 = 'HIGH' THEN 1 ELSE 0 END) AS periodic_high_response_flag,
        MAX(CASE WHEN periodic_response_confidence_v2_10 = 'MEDIUM' THEN 1 ELSE 0 END) AS periodic_medium_evidence_flag,
        MAX(CASE WHEN periodic_response_confidence_v2_10 = 'REVIEW' THEN 1 ELSE 0 END) AS periodic_review_evidence_flag,
        COUNT(DISTINCT CASE WHEN periodic_response_confidence_v2_10 = 'HIGH' THEN parent_occasion_key END) AS periodic_high_event_count,
        COUNT(DISTINCT CASE WHEN periodic_response_confidence_v2_10 = 'MEDIUM' THEN parent_occasion_key END) AS periodic_medium_event_count,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(CASE WHEN periodic_response_confidence_v2_10 = 'HIGH' THEN parent_occasion_name END)))
            AS periodic_high_event_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(CASE WHEN periodic_response_confidence_v2_10 = 'MEDIUM' THEN parent_occasion_name END)))
            AS periodic_medium_event_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(CASE WHEN periodic_response_confidence_v2_10 = 'REVIEW' THEN parent_occasion_name END)))
            AS periodic_review_event_list,
        SUM(CASE WHEN periodic_response_confidence_v2_10 = 'HIGH'
                 THEN GREATEST(trend_adjusted_incremental_baskets, CAST(0 AS DOUBLE)) ELSE 0 END)
            AS periodic_high_incremental_baskets,
        SUM(CASE WHEN periodic_response_confidence_v2_10 = 'HIGH'
                 THEN GREATEST(trend_adjusted_incremental_sales, CAST(0 AS DOUBLE)) ELSE 0 END)
            AS periodic_high_incremental_sales
    FROM lab_customer_value_analytics.seasonality_v2_10_periodic_confidence
    GROUP BY CAST(tpnb AS STRING)
),
base AS (
    SELECT
        t.*,
        COALESCE(i.explicit_seasonal_identity_flag, 0) AS explicit_seasonal_identity_flag,
        COALESCE(i.hard_seasonal_identity_flag, 0) AS hard_seasonal_identity_flag,
        COALESCE(i.explicit_identity_count, 0) AS explicit_identity_count,
        i.identity_occasion_array,
        i.identity_family_array,
        i.identity_season_array,
        COALESCE(i.explicit_identity_occasion_list, '') AS explicit_identity_occasion_list,
        COALESCE(i.explicit_identity_family_list, '') AS explicit_identity_family_list,
        COALESCE(i.explicit_identity_season_list, '') AS explicit_identity_season_list,
        COALESCE(i.matched_identity_patterns, '') AS matched_identity_patterns,
        COALESCE(p.season_before_flag, 0) AS season_before_flag,
        -- The source theme table already contains a column named buildup_flag.
        -- Keep the graph-derived phase flag under a unique internal name so Spark does
        -- not create two buildup_flag columns inside the base/finalised CTE lineage.
        COALESCE(p.buildup_flag, 0) AS graph_buildup_flag,
        COALESCE(p.pre_event_flag, 0) AS pre_event_flag,
        COALESCE(p.season_event_flag, 0) AS season_event_flag,
        COALESCE(p.season_after_flag, 0) AS season_after_flag,
        COALESCE(p.post_event_clearance_flag, 0) AS post_event_clearance_flag,
        COALESCE(p.phase_measured_uplift_flag, 0) AS phase_measured_uplift_flag,
        COALESCE(p.named_seasonal_peak_count, 0) AS named_seasonal_peak_count,
        COALESCE(p.accepted_occasion_count, 0) AS accepted_occasion_count,
        COALESCE(p.held_occasion_count, 0) AS graph_held_occasion_count,
        COALESCE(p.accepted_specific_occasion_count, 0) AS accepted_specific_occasion_count,
        COALESCE(p.held_specific_occasion_count, 0) AS held_specific_occasion_count,
        COALESCE(p.occasion_identity_conflict_flag, 0) AS occasion_identity_conflict_flag,
        COALESCE(p.accepted_occasion_list, '') AS graph_accepted_occasion_list,
        COALESCE(p.held_candidate_occasion_list, '') AS graph_held_candidate_occasion_list,
        COALESCE(p.seasonal_phase_list, '') AS seasonal_phase_list,
        COALESCE(p.named_seasonal_baskets, 0) AS named_seasonal_baskets,
        COALESCE(p.named_seasonal_sales, CAST(0 AS DOUBLE)) AS named_seasonal_sales,
        COALESCE(p.accepted_positive_seasonal_incremental_baskets, CAST(0 AS DOUBLE)) AS accepted_positive_seasonal_incremental_baskets,
        COALESCE(p.accepted_positive_seasonal_incremental_sales, CAST(0 AS DOUBLE)) AS accepted_positive_seasonal_incremental_sales,
        COALESCE(p.held_positive_seasonal_incremental_baskets, CAST(0 AS DOUBLE)) AS held_positive_seasonal_incremental_baskets,
        COALESCE(p.held_positive_seasonal_incremental_sales, CAST(0 AS DOUBLE)) AS held_positive_seasonal_incremental_sales,
        p.max_seasonal_basket_share_index,
        p.max_seasonal_z_score,
        COALESCE(d.daily_total_baskets, t.total_baskets, 0) AS feature_total_baskets,
        COALESCE(d.daily_total_sales, t.total_sales, CAST(0 AS DOUBLE)) AS feature_total_sales,
        COALESCE(d.active_week_positions, 0) AS active_week_positions,
        COALESCE(d.active_quarters, 0) AS active_quarters,
        COALESCE(d.named_general_baskets, COALESCE(d.daily_total_baskets, t.total_baskets, 0)) AS named_general_baskets,
        COALESCE(d.named_general_sales, COALESCE(d.daily_total_sales, t.total_sales, CAST(0 AS DOUBLE))) AS named_general_sales,
        COALESCE(d.general_active_week_positions, d.active_week_positions, 0) AS general_active_week_positions,
        COALESCE(d.general_active_quarters, d.active_quarters, 0) AS general_active_quarters,
        COALESCE(d.general_active_days, 0) AS general_active_days,
        COALESCE(q.periodic_high_response_flag, 0) AS periodic_high_response_flag,
        COALESCE(q.periodic_medium_evidence_flag, 0) AS periodic_medium_evidence_flag,
        COALESCE(q.periodic_review_evidence_flag, 0) AS periodic_review_evidence_flag,
        COALESCE(q.periodic_high_event_count, 0) AS periodic_high_event_count,
        COALESCE(q.periodic_medium_event_count, 0) AS periodic_medium_event_count,
        COALESCE(q.periodic_high_event_list, '') AS periodic_high_event_list,
        COALESCE(q.periodic_medium_event_list, '') AS periodic_medium_event_list,
        COALESCE(q.periodic_review_event_list, '') AS periodic_review_event_list,
        COALESCE(q.periodic_high_incremental_baskets, CAST(0 AS DOUBLE)) AS periodic_high_incremental_baskets,
        COALESCE(q.periodic_high_incremental_sales, CAST(0 AS DOUBLE)) AS periodic_high_incremental_sales
    FROM lab_customer_value_analytics.seasonality_v2_6_theme_output t
    LEFT JOIN lab_customer_value_analytics.seasonality_graph_tpnb_identity i
      ON t.tpnb = i.tpnb
    LEFT JOIN phase_summary p
      ON t.tpnb = p.tpnb
    LEFT JOIN daily_summary d
      ON t.tpnb = d.tpnb
    LEFT JOIN periodic q
      ON t.tpnb = q.tpnb
),
shape AS (
    SELECT
        b.*,
        CASE
            WHEN LOWER(COALESCE(b.time_of_year_pattern, '')) LIKE '%peak%'
             AND LOWER(COALESCE(b.time_of_year_pattern, '')) NOT LIKE '%low volume%'
             AND b.feature_total_baskets >= p.min_profile_baskets
                THEN 1 ELSE 0
        END AS recurring_time_of_year_peak_flag,
        CASE
            WHEN b.feature_total_baskets - b.named_general_baskets > 0
                THEN 100.0 * (b.feature_total_baskets - b.named_general_baskets) / NULLIF(b.feature_total_baskets, 0)
            WHEN LOWER(COALESCE(b.time_of_year_pattern, '')) LIKE '%peak%'
             AND LOWER(COALESCE(b.time_of_year_pattern, '')) NOT LIKE '%low volume%'
                THEN COALESCE(b.best_8_week_share_pct, CAST(0 AS DOUBLE))
            ELSE CAST(0 AS DOUBLE)
        END AS effective_seasonal_basket_share_pct,
        CASE
            WHEN b.feature_total_sales - b.named_general_sales > 0
                THEN 100.0 * (b.feature_total_sales - b.named_general_sales) / NULLIF(b.feature_total_sales, 0)
            WHEN LOWER(COALESCE(b.time_of_year_pattern, '')) LIKE '%peak%'
             AND LOWER(COALESCE(b.time_of_year_pattern, '')) NOT LIKE '%low volume%'
                THEN COALESCE(b.best_8_week_share_pct, CAST(0 AS DOUBLE))
            ELSE CAST(0 AS DOUBLE)
        END AS effective_seasonal_sales_share_pct
    FROM base b
    CROSS JOIN prm p
),
flags_1 AS (
    SELECT
        s.*,
        CASE
            WHEN s.accepted_occasion_count > 0
              OR s.graph_held_occasion_count > 0
              OR s.recurring_time_of_year_peak_flag = 1
              OR (
                    s.explicit_seasonal_identity_flag = 1
                    AND s.availability_pattern = 'Recurring seasonal'
                    AND s.feature_total_baskets >= 20
                 )
                THEN 1 ELSE 0
        END AS seasonal_pattern_flag,
        CASE
            WHEN s.phase_measured_uplift_flag = 1
              OR s.product_seasonality = 'Uplift seasonal'
                THEN 1 ELSE 0
        END AS measured_seasonal_uplift_flag,
        CASE
            WHEN (100.0 - s.effective_seasonal_basket_share_pct) / 100.0 >= p.general_basket_share_min
             AND s.general_active_week_positions >= p.general_active_week_positions_min
             AND s.general_active_quarters >= p.general_active_quarters_min
             AND s.feature_total_baskets * (100.0 - s.effective_seasonal_basket_share_pct) / 100.0 >= p.general_baskets_min
                THEN 1 ELSE 0
        END AS general_sales_flag
    FROM shape s
    CROSS JOIN prm p
),
flags_2 AS (
    SELECT
        f.*,
        CASE
            WHEN f.product_seasonality = 'Specifically seasonal' THEN 1
            WHEN f.accepted_specific_occasion_count > 0 OR f.held_specific_occasion_count > 0 THEN 1
            WHEN f.seasonal_pattern_flag = 1
             AND f.effective_seasonal_basket_share_pct / 100.0 >= p.seasonal_led_basket_share_min THEN 1
            WHEN f.explicit_seasonal_identity_flag = 1
             AND f.recurring_time_of_year_peak_flag = 1
             AND f.general_sales_flag = 0 THEN 1
            ELSE 0
        END AS seasonal_led_flag,
        CASE WHEN f.seasonal_pattern_flag = 1 AND f.general_sales_flag = 1 THEN 1 ELSE 0 END
            AS seasonality_plus_general_flag,
        CASE WHEN f.seasonal_pattern_flag = 0 AND f.general_sales_flag = 1 THEN 1 ELSE 0 END
            AS general_only_flag,
        CASE
            WHEN f.seasonal_pattern_flag = 1
             AND f.season_before_flag = 0
             AND f.season_event_flag = 0
             AND f.season_after_flag = 0
                THEN 1 ELSE 0
        END AS seasonal_period_only_flag
    FROM flags_1 f
    CROSS JOIN prm p
),
finalised AS (
    SELECT
        f.*,
        CASE
            WHEN f.occasion_identity_conflict_flag = 1 THEN 'CONFLICTING_NAMED_OCCASION'
            WHEN f.explicit_seasonal_identity_flag = 0 THEN 'NO_EXPLICIT_SEASONAL_IDENTITY'
            WHEN f.recurring_time_of_year_peak_flag = 0
             AND f.accepted_occasion_count = 0
             AND f.graph_held_occasion_count = 0 THEN 'NO_SEASONAL_ALIGNMENT'
            WHEN ARRAY_CONTAINS(f.identity_season_array, UPPER(COALESCE(f.peak_season, '')))
                THEN 'ALIGNED_TO_IDENTITY_SEASON'
            WHEN f.recurring_time_of_year_peak_flag = 1 THEN 'ALIGNED_TO_DIFFERENT_SEASON'
            ELSE 'SEASONAL_RELATIONSHIP_WITHOUT_RELIABLE_PEAK_SEASON'
        END AS identity_timing_alignment,
        CASE
            WHEN f.seasonal_pattern_flag = 1 AND f.accepted_occasion_count > 0 THEN 'CONFIRMED_OR_SUPPORTED_NAMED_SEASONAL'
            WHEN f.seasonal_pattern_flag = 1 AND f.graph_held_occasion_count > 0 THEN 'SEASONAL_SIGNAL_WITH_REVIEWED_ATTRIBUTION'
            WHEN f.seasonal_pattern_flag = 1 AND f.recurring_time_of_year_peak_flag = 1 THEN 'RECURRING_TIME_OF_YEAR_PATTERN'
            WHEN f.seasonal_pattern_flag = 1 AND f.explicit_seasonal_identity_flag = 1 THEN 'IDENTITY_AND_AVAILABILITY_SUPPORTED_PATTERN'
            WHEN f.general_only_flag = 1 AND f.product_seasonality = 'Insufficient evidence'
                THEN 'PROVISIONAL_GENERAL_FROM_DISTRIBUTED_OFF_SEASON_SALES'
            WHEN f.general_only_flag = 1 THEN 'CONFIRMED_GENERAL'
            WHEN f.feature_total_baskets < 104 THEN 'UNRESOLVED_TOO_LITTLE_TOTAL_VOLUME'
            ELSE 'UNRESOLVED_PATTERN'
        END AS graph_feature_assignment_status,
        CASE
            WHEN f.season_before_flag = 1 AND f.season_event_flag = 1 AND f.season_after_flag = 1 THEN 'BEFORE_EVENT_AND_AFTER'
            WHEN f.season_before_flag = 1 AND f.season_event_flag = 1 THEN 'BEFORE_AND_EVENT'
            WHEN f.season_event_flag = 1 AND f.season_after_flag = 1 THEN 'EVENT_AND_AFTER'
            WHEN f.season_before_flag = 1 AND f.season_after_flag = 1 THEN 'BEFORE_AND_AFTER'
            WHEN f.season_before_flag = 1 THEN 'BEFORE_ONLY'
            WHEN f.season_event_flag = 1 THEN 'EVENT_ONLY'
            WHEN f.season_after_flag = 1 THEN 'AFTER_ONLY'
            WHEN f.seasonal_period_only_flag = 1 THEN 'SEASONAL_PERIOD_ONLY'
            ELSE 'NO_SEASONAL_PATTERN'
        END AS seasonal_timing_profile
    FROM flags_2 f
)
SELECT
    -- Product identity and hierarchy
    tpnb,
    product_description,
    hierarchy_department_name,
    hierarchy_section_name,
    hierarchy_class_name,
    subcategory_code,
    subcategory_name,
    p.feature_as_of_date AS as_of_date,

    -- Signed-off source/audit fields retained unchanged
    product_seasonality AS source_product_seasonality,
    seasonality_evidence_source AS source_seasonality_evidence,
    availability_pattern AS source_availability_pattern,
    CASE WHEN product_seasonality = 'Insufficient evidence' THEN 1 ELSE 0 END AS source_insufficient_evidence_flag,
    CASE WHEN product_seasonality = 'Review required' THEN 1 ELSE 0 END AS source_review_required_flag,
    occasion_suppressed_flag,
    held_annual_occasion_count_v2_6 AS source_held_occasion_count,
    held_annual_occasion_list_v2_6 AS source_held_occasion_list,

    -- Explicit identity and attribution interpretation
    explicit_seasonal_identity_flag,
    hard_seasonal_identity_flag,
    explicit_identity_count,
    explicit_identity_occasion_list,
    explicit_identity_family_list,
    explicit_identity_season_list,
    matched_identity_patterns,
    identity_timing_alignment,
    CASE
        WHEN occasion_identity_conflict_flag = 1 THEN 'CONFLICTING_PRODUCT_IDENTITY'
        WHEN graph_held_occasion_count > 0 THEN 'AMBIGUOUS_OR_HELD_NAMED_OCCASION'
        WHEN accepted_occasion_count > 0 THEN 'CONFIDENT_ACCEPTED_OCCASION'
        WHEN seasonal_pattern_flag = 1 THEN 'SEASONAL_PERIOD_ONLY'
        ELSE 'NO_ACCEPTED_OCCASION'
    END AS occasion_attribution_status,

    -- Final non-exclusive graph flags
    seasonal_pattern_flag AS seasonality_flag,
    seasonal_pattern_flag,
    measured_seasonal_uplift_flag,
    seasonal_led_flag,
    general_sales_flag,
    seasonality_plus_general_flag,
    general_only_flag,

    -- Timing flags
    season_before_flag,
    graph_buildup_flag AS buildup_flag,
    pre_event_flag,
    season_event_flag,
    season_after_flag,
    post_event_clearance_flag,
    seasonal_period_only_flag,
    seasonal_timing_profile,

    -- Multiple peaks and occasion lists
    named_seasonal_peak_count AS seasonal_peak_count,
    CASE WHEN named_seasonal_peak_count >= 2 THEN 1 ELSE 0 END AS multi_seasonal_peak_flag,
    accepted_occasion_count,
    graph_accepted_occasion_list AS accepted_occasion_list,
    graph_held_occasion_count AS candidate_held_occasion_count,
    graph_held_candidate_occasion_list AS candidate_held_occasion_list,
    primary_occasion_key,
    primary_occasion,
    secondary_occasion,
    primary_occasion_weight_pct,
    secondary_occasion_weight_pct,

    -- Existing timing and yearly shape
    shopping_start_days_before,
    shopping_peak_days_before,
    shopping_end_days_before,
    shopping_window_days,
    peak_season,
    time_of_year_pattern,
    best_8_week_share_pct,
    recurring_time_of_year_peak_flag,

    -- Normalised product-level volume features
    feature_total_baskets AS total_baskets,
    feature_total_sales AS total_sales,
    avg_annual_sales,
    active_week_positions,
    active_quarters,
    feature_total_baskets - named_general_baskets AS named_seasonal_baskets,
    feature_total_sales - named_general_sales AS named_seasonal_sales,
    named_general_baskets,
    named_general_sales,
    effective_seasonal_basket_share_pct AS seasonal_basket_share_pct,
    100.0 - effective_seasonal_basket_share_pct AS general_basket_share_pct,
    effective_seasonal_sales_share_pct AS seasonal_sales_share_pct,
    100.0 - effective_seasonal_sales_share_pct AS general_sales_share_pct,
    CASE WHEN accepted_occasion_count > 0 THEN accepted_positive_seasonal_incremental_baskets
         ELSE held_positive_seasonal_incremental_baskets END AS seasonal_incremental_baskets,
    CASE WHEN accepted_occasion_count > 0 THEN accepted_positive_seasonal_incremental_sales
         ELSE held_positive_seasonal_incremental_sales END AS seasonal_incremental_sales,
    occasion_dependency_pct AS source_occasion_dependency_pct,
    max_seasonal_basket_share_index,
    max_seasonal_z_score,
    general_active_week_positions,
    general_active_quarters,
    general_active_days,

    -- Periodic event summary. It never changes annual flags.
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

    -- Explainability/status
    graph_feature_assignment_status
FROM finalised
CROSS JOIN prm p;


-- =====================================================================================
-- END OF BUILD
-- Run seasonality_graph_features_v1_validation.sql after this file.
-- =====================================================================================


-- =====================================================================================
-- SEASONALITY GRAPH FEATURES v2 - FINAL OVERLAY
-- Spark SQL / Databricks / Kyuubi compatible (Spark 3.2+)
--
-- Purpose
--   Create the final, reusable TPNB-level feature set for Themes, product graphs,
--   Need-State analysis and recommendation features.
--
-- Prerequisites
--   1) seasonality_themes_final_production_v2_10.sql has completed successfully.
--   2) seasonality_graph_features_v1_1_kyuubi_fixed.sql has completed successfully.
--
-- Why this is an overlay
--   The signed-off annual and periodic evidence is not recalculated here. This script
--   tightens the graph interpretation layer by requiring a time-of-year pattern to repeat
--   in the same broad season across at least two complete years before it becomes a
--   supported seasonal feature.
--
-- Final decisions implemented
--   * Repeated unnamed seasonality: 2 usable complete years, same peak season in 2 years,
--     median peak-season basket share >= 45%.
--   * Unnamed seasonal-led: repeated peak-season share >= 50%.
--   * General materiality: >=20% general basket share, >=8 off-season week positions,
--     >=2 off-season quarters and >=104 general baskets.
--   * Repeated time-of-year evidence is SUPPORTED, never CONFIRMED.
--   * Candidate classes are never used as supervised training labels.
--   * Source v2.6 annual verdicts and v2.10 periodic statuses remain unchanged for audit.
--
-- Outputs
--   seasonality_graph_v2_params
--   seasonality_graph_v2_tpnb_year_peak_profile
--   seasonality_graph_v2_tpnb_repeatability
--   seasonality_graph_v2_tpnb_period_features
--   seasonality_graph_v2_tpnb_data_state
--   seasonality_graph_v2_tpnb_features          FINAL one row per TPNB
--   seasonality_graph_v2_theme_nodes            subclass x graph behaviour
-- =====================================================================================


-- =====================================================================================
-- 0. PARAMETERS
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_v2_params;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_v2_params
USING PARQUET AS
SELECT
    CAST(104 AS BIGINT) AS usable_year_baskets_min,
    CAST(8 AS INT) AS usable_year_active_weeks_min,
    CAST(2 AS INT) AS repeated_pattern_min_usable_years,
    CAST(2 AS INT) AS repeated_pattern_min_same_peak_years,
    CAST(0.45 AS DOUBLE) AS repeated_pattern_peak_share_min,
    CAST(0.50 AS DOUBLE) AS repeated_pattern_seasonal_led_share_min,

    CAST(0.20 AS DOUBLE) AS general_basket_share_min,
    CAST(8 AS INT) AS general_active_week_positions_min,
    CAST(2 AS INT) AS general_active_quarters_min,
    CAST(104 AS BIGINT) AS general_baskets_min,

    CAST(365 AS INT) AS cold_start_history_days,
    CAST(365 AS INT) AS short_sale_span_days,
    CAST(90 AS INT) AS inactive_days_for_short_lived,
    CAST(104 AS BIGINT) AS low_volume_total_baskets_max,

    CAST(0.45 AS DOUBLE) AS identity_peak_share_candidate_min,
    CURRENT_DATE() AS feature_as_of_date;


-- =====================================================================================
-- 1. ONE PEAK-SEASON PROFILE PER TPNB x COMPLETE YEAR
--
-- A year is usable when the TPNB has at least 104 baskets and is active in at least
-- eight week positions. The broad seasons follow the tested decision-query definition:
-- Winter = weeks 49-52 and 1-8; Spring = 9-21; Summer = 22-34; Autumn = 35-48.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_v2_tpnb_year_peak_profile;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_v2_tpnb_year_peak_profile
USING PARQUET AS
WITH prm AS (
    SELECT *
    FROM lab_customer_value_analytics.seasonality_graph_v2_params
),
complete_years AS (
    SELECT cal_year
    FROM lab_customer_value_analytics.seasonality_v2_calendar_days
    GROUP BY cal_year
    HAVING MIN(cal_date) = MAKE_DATE(cal_year, 1, 1)
       AND MAX(cal_date) = MAKE_DATE(cal_year, 12, 31)
),
entity_week AS (
    SELECT
        d.entity_id AS tpnb,
        d.sale_year,
        LEAST(52, CAST(CEIL(DAYOFYEAR(d.sale_date) / 7.0) AS INT)) AS week_of_year,
        SUM(d.baskets) AS baskets,
        SUM(d.sales) AS sales
    FROM lab_customer_value_analytics.seasonality_v2_entity_daily d
    JOIN complete_years y
      ON d.sale_year = y.cal_year
    WHERE d.entity_level = 'TPNB'
    GROUP BY
        d.entity_id,
        d.sale_year,
        LEAST(52, CAST(CEIL(DAYOFYEAR(d.sale_date) / 7.0) AS INT))
),
year_totals AS (
    SELECT
        tpnb,
        sale_year,
        SUM(baskets) AS year_baskets,
        SUM(sales) AS year_sales,
        SUM(CASE WHEN baskets > 0 THEN 1 ELSE 0 END) AS active_weeks
    FROM entity_week
    GROUP BY tpnb, sale_year
),
season_totals AS (
    SELECT
        w.tpnb,
        w.sale_year,
        CASE
            WHEN w.week_of_year <= 8 OR w.week_of_year >= 49 THEN 'WINTER'
            WHEN w.week_of_year <= 21 THEN 'SPRING'
            WHEN w.week_of_year <= 34 THEN 'SUMMER'
            ELSE 'AUTUMN'
        END AS broad_peak_season,
        SUM(w.baskets) AS season_baskets,
        SUM(w.sales) AS season_sales
    FROM entity_week w
    GROUP BY
        w.tpnb,
        w.sale_year,
        CASE
            WHEN w.week_of_year <= 8 OR w.week_of_year >= 49 THEN 'WINTER'
            WHEN w.week_of_year <= 21 THEN 'SPRING'
            WHEN w.week_of_year <= 34 THEN 'SUMMER'
            ELSE 'AUTUMN'
        END
),
ranked AS (
    SELECT
        s.tpnb,
        s.sale_year,
        s.broad_peak_season,
        s.season_baskets,
        s.season_sales,
        y.year_baskets,
        y.year_sales,
        y.active_weeks,
        s.season_baskets / NULLIF(y.year_baskets, 0) AS peak_season_basket_share,
        s.season_sales / NULLIF(y.year_sales, 0) AS peak_season_sales_share,
        ROW_NUMBER() OVER (
            PARTITION BY s.tpnb, s.sale_year
            ORDER BY s.season_baskets DESC, s.broad_peak_season
        ) AS peak_rank
    FROM season_totals s
    JOIN year_totals y
      ON s.tpnb = y.tpnb
     AND s.sale_year = y.sale_year
)
SELECT
    r.tpnb,
    r.sale_year,
    r.broad_peak_season AS peak_season,
    r.year_baskets,
    r.year_sales,
    r.active_weeks,
    r.season_baskets AS peak_season_baskets,
    r.season_sales AS peak_season_sales,
    r.peak_season_basket_share,
    r.peak_season_sales_share,
    CASE
        WHEN r.year_baskets >= p.usable_year_baskets_min
         AND r.active_weeks >= p.usable_year_active_weeks_min
            THEN 1 ELSE 0
    END AS usable_complete_year_flag
FROM ranked r
CROSS JOIN prm p
WHERE r.peak_rank = 1;


-- =====================================================================================
-- 2. REPEATABILITY SUMMARY (one row per TPNB)
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_v2_tpnb_repeatability;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_v2_tpnb_repeatability
USING PARQUET AS
WITH prm AS (
    SELECT *
    FROM lab_customer_value_analytics.seasonality_graph_v2_params
),
complete_years AS (
    SELECT cal_year
    FROM lab_customer_value_analytics.seasonality_v2_calendar_days
    GROUP BY cal_year
    HAVING MIN(cal_date) = MAKE_DATE(cal_year, 1, 1)
       AND MAX(cal_date) = MAKE_DATE(cal_year, 12, 31)
),
usable AS (
    SELECT *
    FROM lab_customer_value_analytics.seasonality_graph_v2_tpnb_year_peak_profile
    WHERE usable_complete_year_flag = 1
),
by_season AS (
    SELECT
        tpnb,
        peak_season,
        COUNT(*) AS years_as_peak,
        PERCENTILE_APPROX(peak_season_basket_share, 0.5) AS median_peak_basket_share,
        PERCENTILE_APPROX(peak_season_sales_share, 0.5) AS median_peak_sales_share,
        SUM(peak_season_baskets) AS selected_season_baskets_in_peak_years,
        SUM(peak_season_sales) AS selected_season_sales_in_peak_years
    FROM usable
    GROUP BY tpnb, peak_season
),
season_choice AS (
    SELECT
        x.*,
        ROW_NUMBER() OVER (
            PARTITION BY x.tpnb
            ORDER BY x.years_as_peak DESC,
                     x.median_peak_basket_share DESC,
                     x.peak_season
        ) AS choice_rank
    FROM by_season x
),
chosen AS (
    SELECT *
    FROM season_choice
    WHERE choice_rank = 1
),
year_summary AS (
    SELECT
        tpnb,
        COUNT(*) AS usable_complete_years,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(peak_season))) AS observed_peak_season_list,
        MAX(peak_season_basket_share) AS maximum_single_year_peak_share
    FROM usable
    GROUP BY tpnb
),
usable_years AS (
    SELECT DISTINCT tpnb, sale_year
    FROM usable
),
usable_daily AS (
    SELECT
        d.entity_id AS tpnb,
        d.sale_year,
        d.sale_date,
        d.baskets,
        d.sales,
        LEAST(52, CAST(CEIL(DAYOFYEAR(d.sale_date) / 7.0) AS INT)) AS week_of_year,
        QUARTER(d.sale_date) AS calendar_quarter
    FROM lab_customer_value_analytics.seasonality_v2_entity_daily d
    JOIN complete_years y
      ON d.sale_year = y.cal_year
    JOIN usable_years u
      ON d.entity_id = u.tpnb
     AND d.sale_year = u.sale_year
    WHERE d.entity_level = 'TPNB'
),
selected_daily AS (
    SELECT
        d.*,
        c.peak_season AS strongest_peak_season,
        CASE
            WHEN d.week_of_year <= 8 OR d.week_of_year >= 49 THEN 'WINTER'
            WHEN d.week_of_year <= 21 THEN 'SPRING'
            WHEN d.week_of_year <= 34 THEN 'SUMMER'
            ELSE 'AUTUMN'
        END AS day_broad_season
    FROM usable_daily d
    JOIN chosen c
      ON d.tpnb = c.tpnb
),
off_peak AS (
    SELECT
        tpnb,
        SUM(baskets) AS usable_year_baskets,
        SUM(sales) AS usable_year_sales,
        SUM(CASE WHEN day_broad_season = strongest_peak_season THEN baskets ELSE 0 END)
            AS strongest_season_baskets,
        SUM(CASE WHEN day_broad_season = strongest_peak_season THEN sales ELSE CAST(0 AS DOUBLE) END)
            AS strongest_season_sales,
        SUM(CASE WHEN day_broad_season <> strongest_peak_season THEN baskets ELSE 0 END)
            AS off_peak_baskets,
        SUM(CASE WHEN day_broad_season <> strongest_peak_season THEN sales ELSE CAST(0 AS DOUBLE) END)
            AS off_peak_sales,
        COUNT(DISTINCT CASE
            WHEN day_broad_season <> strongest_peak_season AND baskets > 0
                THEN week_of_year END) AS off_peak_active_week_positions,
        COUNT(DISTINCT CASE
            WHEN day_broad_season <> strongest_peak_season AND baskets > 0
                THEN calendar_quarter END) AS off_peak_active_quarters,
        COUNT(DISTINCT CASE
            WHEN day_broad_season <> strongest_peak_season AND baskets > 0
                THEN sale_date END) AS off_peak_active_days
    FROM selected_daily
    GROUP BY tpnb
)
SELECT
    t.tpnb,
    COALESCE(y.usable_complete_years, 0) AS usable_complete_years,
    COALESCE(c.years_as_peak, 0) AS max_years_same_peak_season,
    c.peak_season AS strongest_observed_peak_season,
    CASE WHEN COALESCE(c.years_as_peak, 0) >= p.repeated_pattern_min_same_peak_years
        THEN c.peak_season END AS repeated_peak_season,
    c.median_peak_basket_share AS strongest_peak_median_basket_share,
    c.median_peak_sales_share AS strongest_peak_median_sales_share,
    CASE WHEN COALESCE(c.years_as_peak, 0) >= p.repeated_pattern_min_same_peak_years
        THEN c.median_peak_basket_share END AS repeated_peak_median_basket_share,
    CASE WHEN COALESCE(c.years_as_peak, 0) >= p.repeated_pattern_min_same_peak_years
        THEN c.median_peak_sales_share END AS repeated_peak_median_sales_share,
    y.observed_peak_season_list,
    y.maximum_single_year_peak_share,
    COALESCE(o.usable_year_baskets, 0) AS usable_year_baskets,
    COALESCE(o.usable_year_sales, CAST(0 AS DOUBLE)) AS usable_year_sales,
    COALESCE(o.strongest_season_baskets, 0) AS strongest_season_baskets,
    COALESCE(o.strongest_season_sales, CAST(0 AS DOUBLE)) AS strongest_season_sales,
    COALESCE(o.off_peak_baskets, 0) AS off_peak_baskets,
    COALESCE(o.off_peak_sales, CAST(0 AS DOUBLE)) AS off_peak_sales,
    COALESCE(o.off_peak_active_week_positions, 0) AS off_peak_active_week_positions,
    COALESCE(o.off_peak_active_quarters, 0) AS off_peak_active_quarters,
    COALESCE(o.off_peak_active_days, 0) AS off_peak_active_days,
    100.0 * o.strongest_season_baskets / NULLIF(o.usable_year_baskets, 0)
        AS strongest_season_basket_share_complete_years_pct,
    100.0 * o.strongest_season_sales / NULLIF(o.usable_year_sales, 0)
        AS strongest_season_sales_share_complete_years_pct,
    CASE
        WHEN COALESCE(y.usable_complete_years, 0) >= p.repeated_pattern_min_usable_years
         AND COALESCE(c.years_as_peak, 0) >= p.repeated_pattern_min_same_peak_years
         AND COALESCE(c.median_peak_basket_share, 0) >= p.repeated_pattern_peak_share_min
            THEN 1 ELSE 0
    END AS repeated_time_of_year_pattern_flag,
    CASE
        WHEN COALESCE(y.usable_complete_years, 0) >= p.repeated_pattern_min_usable_years
         AND COALESCE(c.years_as_peak, 0) >= p.repeated_pattern_min_same_peak_years
         AND COALESCE(c.median_peak_basket_share, 0) >= p.repeated_pattern_seasonal_led_share_min
            THEN 1 ELSE 0
    END AS repeated_time_of_year_seasonal_led_flag
FROM lab_customer_value_analytics.seasonality_v2_6_theme_output t
LEFT JOIN year_summary y
  ON t.tpnb = y.tpnb
LEFT JOIN chosen c
  ON t.tpnb = c.tpnb
LEFT JOIN off_peak o
  ON t.tpnb = o.tpnb
CROSS JOIN prm p;


-- =====================================================================================
-- 3. FINAL PERIOD / OCCASION / PHASE FEATURES
--
-- This is a renamed and clearer view of the tested v1 phase table. A material seasonal
-- sales flag says a meaningful share of the seasonal episode occurred in the phase. It is
-- deliberately separate from the measured uplift flag.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_v2_tpnb_period_features;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_v2_tpnb_period_features
USING PARQUET AS
SELECT
    tpnb,
    parent_occasion_key,
    parent_occasion_name,
    occasion_family,
    graph_occasion_verdict,
    evidence_occasion_verdict,
    relationship_status,
    occasion_attribution_status,
    final_annual_policy_reason,
    evidence_source_v2_4,
    occasion_key,
    occurrence_id,
    event_year,
    season_phase,
    period_id,
    phase_start_date,
    phase_end_date,
    phase_calendar_days,
    on_sale_days,
    on_sale_day_coverage_pct,
    sold_flag,
    actual_baskets,
    expected_baskets,
    basket_share_index,
    z_score,
    phase_evidence_status,
    measured_uplift_flag AS period_measured_uplift_flag,
    material_phase_demand_flag AS period_material_demand_flag,
    period_seasonal_flag AS period_material_seasonal_sales_flag,
    source_specific_year_flag,
    actual_sales,
    expected_sales,
    incremental_baskets,
    incremental_sales,
    sales_per_on_sale_day,
    baskets_per_on_sale_day,
    phase_sales_share_of_year_pct,
    phase_basket_share_of_episode_pct,
    phase_sales_share_of_episode_pct,
    accepted_occasion_weight_eligible_flag,
    period_demand_weight AS accepted_period_demand_weight,
    period_value_weight AS accepted_period_value_weight,
    candidate_period_demand_weight,
    candidate_period_value_weight,
    CASE
        WHEN relationship_status = 'ACCEPTED' THEN 'PRODUCTION_ACCEPTED_WEIGHT'
        WHEN relationship_status = 'HELD_REVIEW' THEN 'CANDIDATE_REVIEW_WEIGHT_ONLY'
        ELSE 'NO_GRAPH_WEIGHT'
    END AS period_weight_use,
    price_drop_days,
    price_drop_sales,
    price_drop_sales_share_pct,
    post_event_clearance_flag,
    annual_baskets,
    annual_sales
FROM lab_customer_value_analytics.seasonality_graph_tpnb_occasion_phase_features;


-- =====================================================================================
-- 3B. TPNB PERIOD ROLLUP / NORMALISED SEASONAL SALES VECTOR
--
-- One row per TPNB. The detailed period table remains the source of truth, while this
-- rollup adds aligned arrays to the TPNB output so the graph pipeline can consume the
-- product's full seasonal period profile without another aggregation step.
--
-- Normalisation is within the same TPNB:
--   * actual sales/basket weights use accepted material seasonal periods;
--   * incremental weights use only positive accepted incremental demand;
--   * candidate weights remain separate and never become accepted production weights.
--
-- The period vocabulary here is OCCASION x YEAR x PHASE. It is intentionally not named
-- period_01 ... period_12 because an official 12-period commercial calendar has not been
-- supplied. The arrays are keyed by seasonality_period_id_array and can be zero-filled or
-- pivoted to any agreed model-period calendar during model preparation.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_v2_tpnb_period_rollup;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_v2_tpnb_period_rollup
USING PARQUET AS
WITH base AS (
    SELECT
        tpnb,
        period_id,
        parent_occasion_key,
        parent_occasion_name,
        event_year,
        season_phase,
        relationship_status,
        occasion_attribution_status,
        sold_flag,
        on_sale_days,
        on_sale_day_coverage_pct,
        actual_baskets,
        expected_baskets,
        basket_share_index,
        z_score,
        actual_sales,
        expected_sales,
        incremental_baskets,
        incremental_sales,
        baskets_per_on_sale_day,
        sales_per_on_sale_day,
        period_material_seasonal_sales_flag,
        period_measured_uplift_flag,
        accepted_period_demand_weight,
        accepted_period_value_weight,
        candidate_period_demand_weight,
        candidate_period_value_weight,
        CONCAT(
            LPAD(CAST(event_year AS STRING), 4, '0'), '|',
            COALESCE(parent_occasion_key, ''), '|',
            CASE season_phase
                WHEN 'BUILDUP' THEN '1'
                WHEN 'PRE_EVENT' THEN '2'
                WHEN 'EVENT' THEN '3'
                WHEN 'POST_EVENT' THEN '4'
                ELSE '9'
            END, '|',
            COALESCE(period_id, '')
        ) AS period_sort_key
    FROM lab_customer_value_analytics.seasonality_graph_v2_tpnb_period_features
),
totals AS (
    SELECT
        tpnb,
        COUNT(*) AS period_feature_row_count,
        SUM(CASE WHEN relationship_status = 'ACCEPTED' THEN 1 ELSE 0 END)
            AS accepted_period_row_count,
        SUM(CASE WHEN relationship_status = 'ACCEPTED'
                  AND period_material_seasonal_sales_flag = 1 THEN 1 ELSE 0 END)
            AS accepted_material_seasonal_period_count,
        SUM(CASE WHEN relationship_status = 'ACCEPTED'
                  AND period_measured_uplift_flag = 1 THEN 1 ELSE 0 END)
            AS accepted_measured_uplift_period_count,
        SUM(CASE WHEN relationship_status = 'HELD_REVIEW'
                  AND period_material_seasonal_sales_flag = 1 THEN 1 ELSE 0 END)
            AS candidate_material_seasonal_period_count,
        SUM(CASE WHEN relationship_status = 'ACCEPTED'
                  AND period_material_seasonal_sales_flag = 1
                 THEN GREATEST(actual_baskets, CAST(0 AS DOUBLE)) ELSE CAST(0 AS DOUBLE) END)
            AS accepted_material_period_baskets_total,
        SUM(CASE WHEN relationship_status = 'ACCEPTED'
                  AND period_material_seasonal_sales_flag = 1
                 THEN GREATEST(actual_sales, CAST(0 AS DOUBLE)) ELSE CAST(0 AS DOUBLE) END)
            AS accepted_material_period_sales_total,
        SUM(CASE WHEN relationship_status = 'ACCEPTED'
                 THEN GREATEST(incremental_baskets, CAST(0 AS DOUBLE)) ELSE CAST(0 AS DOUBLE) END)
            AS accepted_positive_incremental_baskets_total,
        SUM(CASE WHEN relationship_status = 'ACCEPTED'
                 THEN GREATEST(incremental_sales, CAST(0 AS DOUBLE)) ELSE CAST(0 AS DOUBLE) END)
            AS accepted_positive_incremental_sales_total,
        SUM(CASE WHEN relationship_status = 'HELD_REVIEW'
                  AND period_material_seasonal_sales_flag = 1
                 THEN GREATEST(actual_baskets, CAST(0 AS DOUBLE)) ELSE CAST(0 AS DOUBLE) END)
            AS candidate_material_period_baskets_total,
        SUM(CASE WHEN relationship_status = 'HELD_REVIEW'
                  AND period_material_seasonal_sales_flag = 1
                 THEN GREATEST(actual_sales, CAST(0 AS DOUBLE)) ELSE CAST(0 AS DOUBLE) END)
            AS candidate_material_period_sales_total
    FROM base
    GROUP BY tpnb
),
scored AS (
    SELECT
        b.*,
        t.period_feature_row_count,
        t.accepted_period_row_count,
        t.accepted_material_seasonal_period_count,
        t.accepted_measured_uplift_period_count,
        t.candidate_material_seasonal_period_count,
        t.accepted_material_period_baskets_total,
        t.accepted_material_period_sales_total,
        t.accepted_positive_incremental_baskets_total,
        t.accepted_positive_incremental_sales_total,
        t.candidate_material_period_baskets_total,
        t.candidate_material_period_sales_total,
        CASE
            WHEN b.relationship_status = 'ACCEPTED'
             AND b.period_material_seasonal_sales_flag = 1
             AND t.accepted_material_period_baskets_total > 0
                THEN GREATEST(b.actual_baskets, CAST(0 AS DOUBLE))
                     / t.accepted_material_period_baskets_total
            ELSE CAST(0 AS DOUBLE)
        END AS tpnb_period_actual_basket_weight,
        CASE
            WHEN b.relationship_status = 'ACCEPTED'
             AND b.period_material_seasonal_sales_flag = 1
             AND t.accepted_material_period_sales_total > 0
                THEN GREATEST(b.actual_sales, CAST(0 AS DOUBLE))
                     / t.accepted_material_period_sales_total
            ELSE CAST(0 AS DOUBLE)
        END AS tpnb_period_actual_sales_weight,
        CASE
            WHEN b.relationship_status = 'ACCEPTED'
             AND t.accepted_positive_incremental_baskets_total > 0
                THEN GREATEST(b.incremental_baskets, CAST(0 AS DOUBLE))
                     / t.accepted_positive_incremental_baskets_total
            ELSE CAST(0 AS DOUBLE)
        END AS tpnb_period_incremental_basket_weight,
        CASE
            WHEN b.relationship_status = 'ACCEPTED'
             AND t.accepted_positive_incremental_sales_total > 0
                THEN GREATEST(b.incremental_sales, CAST(0 AS DOUBLE))
                     / t.accepted_positive_incremental_sales_total
            ELSE CAST(0 AS DOUBLE)
        END AS tpnb_period_incremental_sales_weight,
        CASE
            WHEN b.relationship_status = 'HELD_REVIEW'
             AND b.period_material_seasonal_sales_flag = 1
             AND t.candidate_material_period_baskets_total > 0
                THEN GREATEST(b.actual_baskets, CAST(0 AS DOUBLE))
                     / t.candidate_material_period_baskets_total
            ELSE CAST(0 AS DOUBLE)
        END AS tpnb_candidate_period_actual_basket_weight,
        CASE
            WHEN b.relationship_status = 'HELD_REVIEW'
             AND b.period_material_seasonal_sales_flag = 1
             AND t.candidate_material_period_sales_total > 0
                THEN GREATEST(b.actual_sales, CAST(0 AS DOUBLE))
                     / t.candidate_material_period_sales_total
            ELSE CAST(0 AS DOUBLE)
        END AS tpnb_candidate_period_actual_sales_weight
    FROM base b
    JOIN totals t
      ON b.tpnb = t.tpnb
),
aggregated AS (
    SELECT
        tpnb,
        MAX(period_feature_row_count) AS period_feature_row_count,
        MAX(accepted_period_row_count) AS accepted_period_row_count,
        MAX(accepted_material_seasonal_period_count) AS accepted_material_seasonal_period_count,
        MAX(accepted_measured_uplift_period_count) AS accepted_measured_uplift_period_count,
        MAX(candidate_material_seasonal_period_count) AS candidate_material_seasonal_period_count,
        MAX(accepted_material_period_baskets_total) AS accepted_material_period_baskets_total,
        MAX(accepted_material_period_sales_total) AS accepted_material_period_sales_total,
        MAX(accepted_positive_incremental_baskets_total) AS accepted_positive_incremental_baskets_total,
        MAX(accepted_positive_incremental_sales_total) AS accepted_positive_incremental_sales_total,
        MAX(candidate_material_period_baskets_total) AS candidate_material_period_baskets_total,
        MAX(candidate_material_period_sales_total) AS candidate_material_period_sales_total,
        ARRAY_SORT(COLLECT_LIST(NAMED_STRUCT(
            'sort_key', period_sort_key,
            'period_id', period_id,
            'occasion_key', parent_occasion_key,
            'occasion_name', parent_occasion_name,
            'event_year', event_year,
            'season_phase', season_phase,
            'relationship_status', relationship_status,
            'occasion_attribution_status', occasion_attribution_status,
            'sold_flag', sold_flag,
            'on_sale_days', on_sale_days,
            'on_sale_day_coverage_pct', on_sale_day_coverage_pct,
            'actual_baskets', actual_baskets,
            'expected_baskets', expected_baskets,
            'basket_share_index', basket_share_index,
            'z_score', z_score,
            'actual_sales', actual_sales,
            'expected_sales', expected_sales,
            'incremental_baskets', incremental_baskets,
            'incremental_sales', incremental_sales,
            'baskets_per_on_sale_day', baskets_per_on_sale_day,
            'sales_per_on_sale_day', sales_per_on_sale_day,
            'material_seasonal_sales_flag', period_material_seasonal_sales_flag,
            'measured_uplift_flag', period_measured_uplift_flag,
            'actual_basket_weight_within_tpnb', tpnb_period_actual_basket_weight,
            'actual_sales_weight_within_tpnb', tpnb_period_actual_sales_weight,
            'incremental_basket_weight_within_tpnb', tpnb_period_incremental_basket_weight,
            'incremental_sales_weight_within_tpnb', tpnb_period_incremental_sales_weight,
            'candidate_actual_basket_weight_within_tpnb', tpnb_candidate_period_actual_basket_weight,
            'candidate_actual_sales_weight_within_tpnb', tpnb_candidate_period_actual_sales_weight
        ))) AS sorted_period_feature_array
    FROM scored
    GROUP BY tpnb
)
SELECT
    tpnb,
    period_feature_row_count,
    accepted_period_row_count,
    accepted_material_seasonal_period_count,
    accepted_measured_uplift_period_count,
    candidate_material_seasonal_period_count,
    accepted_material_period_baskets_total,
    accepted_material_period_sales_total,
    accepted_positive_incremental_baskets_total,
    accepted_positive_incremental_sales_total,
    candidate_material_period_baskets_total,
    candidate_material_period_sales_total,
    TRANSFORM(sorted_period_feature_array, x -> x.period_id)
        AS seasonality_period_id_array,
    TRANSFORM(sorted_period_feature_array, x -> x.occasion_key)
        AS seasonality_period_occasion_key_array,
    TRANSFORM(sorted_period_feature_array, x -> x.event_year)
        AS seasonality_period_year_array,
    TRANSFORM(sorted_period_feature_array, x -> x.season_phase)
        AS seasonality_period_phase_array,
    TRANSFORM(sorted_period_feature_array, x -> x.sold_flag)
        AS seasonality_period_sold_flag_array,
    TRANSFORM(sorted_period_feature_array, x -> x.actual_baskets)
        AS seasonality_period_actual_baskets_array,
    TRANSFORM(sorted_period_feature_array, x -> x.actual_basket_weight_within_tpnb)
        AS seasonality_period_actual_basket_weight_array,
    TRANSFORM(sorted_period_feature_array, x -> x.incremental_baskets)
        AS seasonality_period_incremental_baskets_array,
    TRANSFORM(sorted_period_feature_array, x -> x.incremental_basket_weight_within_tpnb)
        AS seasonality_period_incremental_basket_weight_array,
    TRANSFORM(sorted_period_feature_array, x -> x.actual_sales)
        AS seasonality_period_actual_sales_array,
    TRANSFORM(sorted_period_feature_array, x -> x.actual_sales_weight_within_tpnb)
        AS seasonality_period_actual_sales_weight_array,
    TRANSFORM(sorted_period_feature_array, x -> x.incremental_sales)
        AS seasonality_period_incremental_sales_array,
    TRANSFORM(sorted_period_feature_array, x -> x.incremental_sales_weight_within_tpnb)
        AS seasonality_period_incremental_sales_weight_array,
    TRANSFORM(sorted_period_feature_array, x -> x.material_seasonal_sales_flag)
        AS seasonality_period_material_sales_flag_array,
    TRANSFORM(sorted_period_feature_array, x -> x.measured_uplift_flag)
        AS seasonality_period_measured_uplift_flag_array,
    TRANSFORM(sorted_period_feature_array, x -> x.basket_share_index)
        AS seasonality_period_basket_share_index_array,
    TRANSFORM(sorted_period_feature_array, x -> x.z_score)
        AS seasonality_period_z_score_array,
    sorted_period_feature_array AS seasonality_period_feature_array,
    'OCCASION_YEAR_PHASE' AS seasonality_period_vector_source,
    'WITHIN_TPNB_ACCEPTED_MATERIAL_SEASONAL_PERIODS' AS seasonality_period_sales_normalisation
FROM aggregated;


-- =====================================================================================
-- 4. HUMAN-READABLE DATA STATE (one row per TPNB)
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_v2_tpnb_data_state;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_v2_tpnb_data_state
USING PARQUET AS
WITH data_end AS (
    SELECT MAX(last_sale_date) AS data_end_date
    FROM lab_customer_value_analytics.seasonality_v2_6_theme_output
),
base AS (
    SELECT
        g.tpnb,
        g.source_product_seasonality,
        g.source_availability_pattern,
        g.source_review_required_flag,
        g.candidate_held_occasion_count,
        g.accepted_occasion_count,
        g.explicit_seasonal_identity_flag,
        g.active_week_positions,
        g.active_quarters,
        g.total_baskets,
        t.first_sale_date,
        t.last_sale_date,
        DATEDIFF(d.data_end_date, t.first_sale_date) AS observable_history_days,
        DATEDIFF(t.last_sale_date, t.first_sale_date) AS observed_sale_span_days,
        DATEDIFF(d.data_end_date, t.last_sale_date) AS days_since_last_sale,
        COALESCE(r.usable_complete_years, 0) AS usable_complete_years,
        COALESCE(r.repeated_time_of_year_pattern_flag, 0) AS repeated_time_of_year_pattern_flag
    FROM lab_customer_value_analytics.seasonality_graph_tpnb_features g
    JOIN lab_customer_value_analytics.seasonality_v2_6_theme_output t
      ON g.tpnb = t.tpnb
    LEFT JOIN lab_customer_value_analytics.seasonality_graph_v2_tpnb_repeatability r
      ON g.tpnb = r.tpnb
    CROSS JOIN data_end d
),
classified AS (
    SELECT
        b.*,
        CASE
            WHEN b.source_review_required_flag = 1
              OR b.candidate_held_occasion_count > 0
                THEN 'ATTRIBUTION_CONFLICT_OR_REVIEW'

            WHEN b.source_availability_pattern = 'One-off limited edition'
                THEN 'ONE_OFF_RANGE_NO_REPEAT_TEST'

            WHEN b.observable_history_days < p.cold_start_history_days
                THEN 'COLD_START_NEW_PRODUCT_UNDER_12_MONTHS'

            WHEN b.observed_sale_span_days < p.short_sale_span_days
             AND b.days_since_last_sale >= p.inactive_days_for_short_lived
                THEN 'SHORT_LIVED_OR_DELISTED_RANGE'

            WHEN b.total_baskets < p.low_volume_total_baskets_max
                THEN 'LOW_VOLUME_LONG_TAIL'

            WHEN b.source_availability_pattern = 'Intermittent'
                THEN 'INTERMITTENT_OR_RANGE_LIMITED'

            WHEN b.accepted_occasion_count > 0
              OR b.repeated_time_of_year_pattern_flag = 1
              OR b.usable_complete_years >= 2
              OR (
                    b.source_product_seasonality IN ('Regular', 'Occasion-suppressed')
                    AND b.observable_history_days >= 365
                 )
                THEN 'MATURE_MULTI_YEAR_HISTORY'

            WHEN b.explicit_seasonal_identity_flag = 1
                THEN 'SEASONAL_IDENTITY_WITHOUT_CONFIRMED_BEHAVIOUR'

            WHEN b.active_week_positions >= p.general_active_week_positions_min
             AND b.active_quarters >= p.general_active_quarters_min
                THEN 'SUFFICIENT_VOLUME_NO_STABLE_ANNUAL_PATTERN'

            ELSE 'OTHER_LIMITED_EVIDENCE'
        END AS graph_data_state
    FROM base b
    CROSS JOIN lab_customer_value_analytics.seasonality_graph_v2_params p
)
SELECT
    tpnb,
    graph_data_state,
    first_sale_date,
    last_sale_date,
    observable_history_days,
    observed_sale_span_days,
    days_since_last_sale
FROM classified;


-- =====================================================================================
-- 5. FINAL TPNB FEATURES (one row per TPNB)
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_v2_tpnb_features;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_v2_tpnb_features
USING PARQUET AS
WITH prm AS (
    SELECT *
    FROM lab_customer_value_analytics.seasonality_graph_v2_params
),
base AS (
    SELECT
        g.*,
        t.first_sale_date,
        t.last_sale_date,
        i.identity_season_array,
        COALESCE(r.usable_complete_years, 0) AS usable_complete_years,
        COALESCE(r.max_years_same_peak_season, 0) AS max_years_same_peak_season,
        r.strongest_observed_peak_season,
        r.repeated_peak_season,
        r.strongest_peak_median_basket_share,
        r.strongest_peak_median_sales_share,
        r.repeated_peak_median_basket_share,
        r.repeated_peak_median_sales_share,
        r.observed_peak_season_list,
        r.maximum_single_year_peak_share,
        COALESCE(r.usable_year_baskets, 0) AS usable_year_baskets,
        COALESCE(r.usable_year_sales, CAST(0 AS DOUBLE)) AS usable_year_sales,
        COALESCE(r.strongest_season_baskets, 0) AS strongest_season_baskets,
        COALESCE(r.strongest_season_sales, CAST(0 AS DOUBLE)) AS strongest_season_sales,
        COALESCE(r.off_peak_baskets, 0) AS repeat_off_peak_baskets,
        COALESCE(r.off_peak_sales, CAST(0 AS DOUBLE)) AS repeat_off_peak_sales,
        COALESCE(r.off_peak_active_week_positions, 0) AS repeat_off_peak_active_week_positions,
        COALESCE(r.off_peak_active_quarters, 0) AS repeat_off_peak_active_quarters,
        COALESCE(r.off_peak_active_days, 0) AS repeat_off_peak_active_days,
        r.strongest_season_basket_share_complete_years_pct,
        r.strongest_season_sales_share_complete_years_pct,
        COALESCE(r.repeated_time_of_year_pattern_flag, 0) AS repeated_time_of_year_pattern_flag,
        COALESCE(r.repeated_time_of_year_seasonal_led_flag, 0) AS repeated_time_of_year_seasonal_led_flag,
        d.graph_data_state,
        d.observable_history_days,
        d.observed_sale_span_days,
        d.days_since_last_sale,
        COALESCE(pr.period_feature_row_count, 0) AS period_feature_row_count,
        COALESCE(pr.accepted_period_row_count, 0) AS accepted_period_row_count,
        COALESCE(pr.accepted_material_seasonal_period_count, 0) AS accepted_material_seasonal_period_count,
        COALESCE(pr.accepted_measured_uplift_period_count, 0) AS accepted_measured_uplift_period_count,
        COALESCE(pr.candidate_material_seasonal_period_count, 0) AS candidate_material_seasonal_period_count,
        COALESCE(pr.accepted_material_period_baskets_total, CAST(0 AS DOUBLE))
            AS accepted_material_period_baskets_total,
        COALESCE(pr.accepted_material_period_sales_total, CAST(0 AS DOUBLE))
            AS accepted_material_period_sales_total,
        COALESCE(pr.accepted_positive_incremental_baskets_total, CAST(0 AS DOUBLE))
            AS accepted_positive_incremental_baskets_total,
        COALESCE(pr.accepted_positive_incremental_sales_total, CAST(0 AS DOUBLE))
            AS accepted_positive_incremental_sales_total,
        pr.seasonality_period_id_array,
        pr.seasonality_period_occasion_key_array,
        pr.seasonality_period_year_array,
        pr.seasonality_period_phase_array,
        pr.seasonality_period_sold_flag_array,
        pr.seasonality_period_actual_baskets_array,
        pr.seasonality_period_actual_basket_weight_array,
        pr.seasonality_period_incremental_baskets_array,
        pr.seasonality_period_incremental_basket_weight_array,
        pr.seasonality_period_actual_sales_array,
        pr.seasonality_period_actual_sales_weight_array,
        pr.seasonality_period_incremental_sales_array,
        pr.seasonality_period_incremental_sales_weight_array,
        pr.seasonality_period_material_sales_flag_array,
        pr.seasonality_period_measured_uplift_flag_array,
        pr.seasonality_period_basket_share_index_array,
        pr.seasonality_period_z_score_array,
        pr.seasonality_period_feature_array,
        pr.seasonality_period_vector_source,
        pr.seasonality_period_sales_normalisation,
        CASE WHEN g.occasion_attribution_status = 'CONFLICTING_PRODUCT_IDENTITY' THEN 1 ELSE 0 END
            AS occasion_identity_conflict_flag
    FROM lab_customer_value_analytics.seasonality_graph_tpnb_features g
    JOIN lab_customer_value_analytics.seasonality_v2_6_theme_output t
      ON g.tpnb = t.tpnb
    LEFT JOIN lab_customer_value_analytics.seasonality_graph_tpnb_identity i
      ON g.tpnb = i.tpnb
    LEFT JOIN lab_customer_value_analytics.seasonality_graph_v2_tpnb_repeatability r
      ON g.tpnb = r.tpnb
    LEFT JOIN lab_customer_value_analytics.seasonality_graph_v2_tpnb_data_state d
      ON g.tpnb = d.tpnb
    LEFT JOIN lab_customer_value_analytics.seasonality_graph_v2_tpnb_period_rollup pr
      ON g.tpnb = pr.tpnb
),
signal_source AS (
    SELECT
        b.*,
        CASE WHEN b.accepted_occasion_count > 0 THEN 1 ELSE 0 END AS accepted_named_seasonal_flag,
        CASE
            WHEN b.source_review_required_flag = 1
              OR b.candidate_held_occasion_count > 0
                THEN 1 ELSE 0
        END AS reviewed_or_held_seasonal_signal_flag,
        CASE
            WHEN b.explicit_seasonal_identity_flag = 1
             AND (
                    b.source_availability_pattern IN ('Recurring seasonal', 'One-off limited edition')
                 OR COALESCE(b.strongest_peak_median_basket_share, 0) >= p.identity_peak_share_candidate_min
                 OR (
                        LOWER(COALESCE(b.time_of_year_pattern, '')) LIKE '%peak%'
                        AND LOWER(COALESCE(b.time_of_year_pattern, '')) NOT LIKE '%low volume%'
                        AND COALESCE(b.best_8_week_share_pct, 0) >= 100.0 * p.identity_peak_share_candidate_min
                    )
                 )
                THEN 1 ELSE 0
        END AS identity_supported_candidate_flag,
        COALESCE(
            b.repeated_peak_season,
            b.strongest_observed_peak_season,
            UPPER(NULLIF(b.peak_season, ''))
        ) AS observed_graph_peak_season
    FROM base b
    CROSS JOIN prm p
),
share_basis AS (
    SELECT
        s.*,
        CASE
            WHEN s.accepted_named_seasonal_flag = 1
              OR s.reviewed_or_held_seasonal_signal_flag = 1
                THEN CASE
                    WHEN COALESCE(s.seasonal_basket_share_pct, 0) > 0
                        THEN s.seasonal_basket_share_pct
                    ELSE COALESCE(s.best_8_week_share_pct, 0)
                END
            WHEN s.repeated_time_of_year_pattern_flag = 1
                THEN 100.0 * COALESCE(s.repeated_peak_median_basket_share, 0)
            WHEN s.identity_supported_candidate_flag = 1
                THEN CASE
                    WHEN s.strongest_peak_median_basket_share IS NOT NULL
                        THEN 100.0 * s.strongest_peak_median_basket_share
                    ELSE COALESCE(s.best_8_week_share_pct, 0)
                END
            ELSE CAST(0 AS DOUBLE)
        END AS effective_seasonal_basket_share_pct_v2,
        CASE
            WHEN s.accepted_named_seasonal_flag = 1
              OR s.reviewed_or_held_seasonal_signal_flag = 1
                THEN CASE
                    WHEN COALESCE(s.seasonal_sales_share_pct, 0) > 0
                        THEN s.seasonal_sales_share_pct
                    ELSE COALESCE(s.best_8_week_share_pct, 0)
                END
            WHEN s.repeated_time_of_year_pattern_flag = 1
                THEN 100.0 * COALESCE(
                    s.repeated_peak_median_sales_share,
                    s.repeated_peak_median_basket_share,
                    CAST(0 AS DOUBLE)
                )
            WHEN s.identity_supported_candidate_flag = 1
                THEN CASE
                    WHEN s.strongest_peak_median_sales_share IS NOT NULL
                        THEN 100.0 * s.strongest_peak_median_sales_share
                    ELSE COALESCE(s.best_8_week_share_pct, 0)
                END
            ELSE CAST(0 AS DOUBLE)
        END AS effective_seasonal_sales_share_pct_v2,
        CASE
            WHEN s.accepted_named_seasonal_flag = 1
              OR s.reviewed_or_held_seasonal_signal_flag = 1
                THEN 'NAMED_OCCASION_PHASES'
            WHEN s.repeated_time_of_year_pattern_flag = 1
                THEN 'REPEATED_BROAD_SEASON_R45_2Y'
            WHEN s.identity_supported_candidate_flag = 1
                THEN 'IDENTITY_WITH_OBSERVED_PATTERN_PROXY'
            ELSE 'NO_SEASONAL_SHARE'
        END AS seasonal_share_measure_type
    FROM signal_source s
),
general_basis AS (
    SELECT
        s.*,
        CASE
            WHEN s.repeated_time_of_year_pattern_flag = 1
             AND s.accepted_named_seasonal_flag = 0
             AND s.reviewed_or_held_seasonal_signal_flag = 0
                THEN s.repeat_off_peak_baskets
            WHEN s.accepted_named_seasonal_flag = 1
              OR s.reviewed_or_held_seasonal_signal_flag = 1
                THEN s.named_general_baskets
            WHEN s.identity_supported_candidate_flag = 1
                THEN s.total_baskets * (100.0 - s.effective_seasonal_basket_share_pct_v2) / 100.0
            ELSE s.total_baskets
        END AS effective_general_baskets_v2,
        CASE
            WHEN s.repeated_time_of_year_pattern_flag = 1
             AND s.accepted_named_seasonal_flag = 0
             AND s.reviewed_or_held_seasonal_signal_flag = 0
                THEN s.repeat_off_peak_sales
            WHEN s.accepted_named_seasonal_flag = 1
              OR s.reviewed_or_held_seasonal_signal_flag = 1
                THEN s.named_general_sales
            WHEN s.identity_supported_candidate_flag = 1
                THEN s.total_sales * (100.0 - s.effective_seasonal_sales_share_pct_v2) / 100.0
            ELSE s.total_sales
        END AS effective_general_sales_v2,
        CASE
            WHEN s.repeated_time_of_year_pattern_flag = 1
             AND s.accepted_named_seasonal_flag = 0
             AND s.reviewed_or_held_seasonal_signal_flag = 0
                THEN s.repeat_off_peak_active_week_positions
            ELSE s.general_active_week_positions
        END AS effective_general_active_week_positions_v2,
        CASE
            WHEN s.repeated_time_of_year_pattern_flag = 1
             AND s.accepted_named_seasonal_flag = 0
             AND s.reviewed_or_held_seasonal_signal_flag = 0
                THEN s.repeat_off_peak_active_quarters
            ELSE s.general_active_quarters
        END AS effective_general_active_quarters_v2,
        CASE
            WHEN s.repeated_time_of_year_pattern_flag = 1
             AND s.accepted_named_seasonal_flag = 0
             AND s.reviewed_or_held_seasonal_signal_flag = 0
                THEN s.repeat_off_peak_active_days
            ELSE s.general_active_days
        END AS effective_general_active_days_v2
    FROM share_basis s
),
flags AS (
    SELECT
        g.*,
        CASE
            WHEN g.accepted_named_seasonal_flag = 1
              OR g.reviewed_or_held_seasonal_signal_flag = 1
              OR g.repeated_time_of_year_pattern_flag = 1
              OR g.identity_supported_candidate_flag = 1
                THEN 1 ELSE 0
        END AS seasonal_pattern_flag_v2,
        CASE
            WHEN g.effective_general_baskets_v2 / NULLIF(g.total_baskets, 0) >= p.general_basket_share_min
             AND g.effective_general_active_week_positions_v2 >= p.general_active_week_positions_min
             AND g.effective_general_active_quarters_v2 >= p.general_active_quarters_min
             AND g.effective_general_baskets_v2 >= p.general_baskets_min
                THEN 1 ELSE 0
        END AS general_sales_flag_v2,
        CASE
            WHEN g.measured_seasonal_uplift_flag = 1
              OR (
                    g.accepted_named_seasonal_flag = 1
                    AND g.source_product_seasonality = 'Uplift seasonal'
                 )
                THEN 1 ELSE 0
        END AS measured_seasonal_uplift_flag_v2
    FROM general_basis g
    CROSS JOIN prm p
),
derived_flags AS (
    SELECT
        f.*,
        CASE
            WHEN f.source_product_seasonality = 'Specifically seasonal'
             AND f.accepted_named_seasonal_flag = 1
                THEN 1
            WHEN f.seasonal_pattern_flag_v2 = 1
             AND (
                    f.effective_seasonal_basket_share_pct_v2 >= 100.0 * p.repeated_pattern_seasonal_led_share_min
                 OR f.general_sales_flag_v2 = 0
                 )
                THEN 1
            ELSE 0
        END AS seasonal_led_flag_v2,
        CASE
            WHEN f.seasonal_pattern_flag_v2 = 1
             AND f.general_sales_flag_v2 = 1
                THEN 1 ELSE 0
        END AS seasonality_plus_general_flag_v2,
        CASE
            WHEN f.seasonal_pattern_flag_v2 = 0
             AND f.general_sales_flag_v2 = 1
                THEN 1 ELSE 0
        END AS general_only_flag_v2
    FROM flags f
    CROSS JOIN prm p
),
classified AS (
    SELECT
        f.*,
        CASE
            WHEN f.accepted_named_seasonal_flag = 1
             AND f.general_sales_flag_v2 = 1
                THEN 'SEASONALITY_PLUS_GENERAL'
            WHEN f.accepted_named_seasonal_flag = 1
                THEN 'SEASONAL_LED'

            WHEN f.reviewed_or_held_seasonal_signal_flag = 1
                THEN 'SEASONAL_PATTERN_CANDIDATE'

            WHEN f.repeated_time_of_year_pattern_flag = 1
             AND f.general_sales_flag_v2 = 1
                THEN 'SEASONALITY_PLUS_GENERAL'
            WHEN f.repeated_time_of_year_pattern_flag = 1
                THEN 'SEASONAL_LED'

            WHEN f.identity_supported_candidate_flag = 1
                THEN 'SEASONAL_PATTERN_CANDIDATE'

            WHEN f.source_product_seasonality IN ('Regular', 'Occasion-suppressed')
             AND f.general_sales_flag_v2 = 1
                THEN 'GENERAL_ONLY'

            WHEN f.source_product_seasonality = 'Insufficient evidence'
             AND f.general_sales_flag_v2 = 1
                THEN 'GENERAL_PATTERN_CANDIDATE'

            ELSE 'LOW_INFORMATION_NODE'
        END AS graph_behavior_class,
        CASE
            WHEN f.accepted_named_seasonal_flag = 1
                THEN 'CONFIRMED'
            WHEN f.reviewed_or_held_seasonal_signal_flag = 1
                THEN 'PROVISIONAL'
            WHEN f.repeated_time_of_year_pattern_flag = 1
                THEN 'SUPPORTED'
            WHEN f.source_product_seasonality IN ('Regular', 'Occasion-suppressed')
             AND f.general_sales_flag_v2 = 1
             AND f.seasonal_pattern_flag_v2 = 0
                THEN 'CONFIRMED'
            WHEN f.identity_supported_candidate_flag = 1
              OR (
                    f.source_product_seasonality = 'Insufficient evidence'
                    AND f.general_sales_flag_v2 = 1
                 )
                THEN 'PROVISIONAL'
            ELSE 'LOW_INFORMATION'
        END AS graph_evidence_tier,
        CASE
            WHEN f.accepted_named_seasonal_flag = 1 THEN 1
            WHEN f.source_product_seasonality IN ('Regular', 'Occasion-suppressed')
             AND f.general_sales_flag_v2 = 1
             AND f.seasonal_pattern_flag_v2 = 0 THEN 1
            ELSE 0
        END AS graph_training_label_flag,
        CASE
            WHEN f.accepted_named_seasonal_flag = 1 THEN 'ACCEPTED_NAMED_ANNUAL_SEASONALITY'
            WHEN f.reviewed_or_held_seasonal_signal_flag = 1 THEN 'HELD_OR_REVIEWED_SEASONAL_SIGNAL'
            WHEN f.repeated_time_of_year_pattern_flag = 1 THEN 'REPEATED_TIME_OF_YEAR_PATTERN_R45_2Y'
            WHEN f.identity_supported_candidate_flag = 1 THEN 'EXPLICIT_IDENTITY_WITH_OBSERVED_OR_RANGE_PATTERN'
            WHEN f.source_product_seasonality IN ('Regular', 'Occasion-suppressed')
             AND f.general_sales_flag_v2 = 1 THEN 'CONFIRMED_DISTRIBUTED_GENERAL_SALES'
            WHEN f.source_product_seasonality = 'Insufficient evidence'
             AND f.general_sales_flag_v2 = 1 THEN 'PROVISIONAL_DISTRIBUTED_GENERAL_SALES'
            ELSE 'NO_RELIABLE_BEHAVIOUR_YET'
        END AS graph_behavior_reason
    FROM derived_flags f
),
interpreted AS (
    SELECT
        c.*,
        CASE
            WHEN c.occasion_identity_conflict_flag = 1
                THEN 'CONFLICTING_NAMED_OCCASION'
            WHEN c.explicit_seasonal_identity_flag = 0
                THEN 'NO_EXPLICIT_SEASONAL_IDENTITY'
            WHEN c.seasonal_pattern_flag_v2 = 0
                THEN 'NO_SEASONAL_ALIGNMENT'
            WHEN c.observed_graph_peak_season IS NULL
                THEN 'SEASONAL_IDENTITY_WITHOUT_RELIABLE_PEAK_SEASON'
            WHEN ARRAY_CONTAINS(c.identity_season_array, c.observed_graph_peak_season)
                THEN 'ALIGNED_WITHIN_IDENTITY_SEASON'
            WHEN (
                    c.observed_graph_peak_season = 'AUTUMN'
                    AND ARRAY_CONTAINS(c.identity_season_array, 'WINTER')
                 ) OR (
                    c.observed_graph_peak_season = 'WINTER'
                    AND ARRAY_CONTAINS(c.identity_season_array, 'SPRING')
                 ) OR (
                    c.observed_graph_peak_season = 'SPRING'
                    AND ARRAY_CONTAINS(c.identity_season_array, 'SUMMER')
                 ) OR (
                    c.observed_graph_peak_season = 'SUMMER'
                    AND ARRAY_CONTAINS(c.identity_season_array, 'AUTUMN')
                 )
                THEN 'ALIGNED_BEFORE_IDENTITY_SEASON'
            WHEN (
                    c.observed_graph_peak_season = 'SPRING'
                    AND ARRAY_CONTAINS(c.identity_season_array, 'WINTER')
                 ) OR (
                    c.observed_graph_peak_season = 'SUMMER'
                    AND ARRAY_CONTAINS(c.identity_season_array, 'SPRING')
                 ) OR (
                    c.observed_graph_peak_season = 'AUTUMN'
                    AND ARRAY_CONTAINS(c.identity_season_array, 'SUMMER')
                 ) OR (
                    c.observed_graph_peak_season = 'WINTER'
                    AND ARRAY_CONTAINS(c.identity_season_array, 'AUTUMN')
                 )
                THEN 'ALIGNED_AFTER_IDENTITY_SEASON'
            ELSE 'ALIGNED_TO_UNRELATED_SEASON'
        END AS identity_timing_alignment_v2,
        CASE
            WHEN c.season_before_flag = 1 AND c.season_event_flag = 1 AND c.season_after_flag = 1
                THEN 'BEFORE_EVENT_AND_AFTER'
            WHEN c.season_before_flag = 1 AND c.season_event_flag = 1
                THEN 'BEFORE_AND_EVENT'
            WHEN c.season_event_flag = 1 AND c.season_after_flag = 1
                THEN 'EVENT_AND_AFTER'
            WHEN c.season_before_flag = 1 AND c.season_after_flag = 1
                THEN 'BEFORE_AND_AFTER'
            WHEN c.season_before_flag = 1 THEN 'BEFORE_ONLY'
            WHEN c.season_event_flag = 1 THEN 'EVENT_ONLY'
            WHEN c.season_after_flag = 1 THEN 'AFTER_ONLY'
            WHEN c.seasonal_pattern_flag_v2 = 1 THEN 'SEASONAL_PERIOD_ONLY'
            ELSE 'NO_SEASONAL_PATTERN'
        END AS seasonal_timing_profile_v2
    FROM classified c
)
SELECT
    -- Theme membership and product hierarchy
    i.tpnb,
    i.product_description,
    i.hierarchy_department_name,
    i.hierarchy_section_name,
    i.hierarchy_class_name,
    i.subcategory_code,
    i.subcategory_name,
    CONCAT(i.subcategory_code, '|', i.graph_behavior_class) AS theme_node_key,
    CONCAT(
        COALESCE(i.subcategory_name, i.subcategory_code),
        ' - ',
        CASE i.graph_behavior_class
            WHEN 'SEASONAL_LED' THEN 'Seasonal-led'
            WHEN 'SEASONALITY_PLUS_GENERAL' THEN 'Seasonality + General'
            WHEN 'GENERAL_ONLY' THEN 'General only'
            WHEN 'SEASONAL_PATTERN_CANDIDATE' THEN 'Seasonal pattern candidate'
            WHEN 'GENERAL_PATTERN_CANDIDATE' THEN 'General pattern candidate'
            ELSE 'Low-information node'
        END
    ) AS theme_node_display_name,
    p.feature_as_of_date AS as_of_date,

    -- Final behaviour, evidence and model-use fields
    i.graph_behavior_class,
    i.graph_behavior_reason,
    i.graph_evidence_tier,
    i.graph_data_state,
    i.graph_training_label_flag,
    CAST(i.graph_training_label_flag AS DOUBLE) AS graph_training_label_weight,
    1 AS graph_node_inclusion_flag,
    CASE WHEN i.graph_behavior_class IN ('SEASONAL_LED', 'SEASONALITY_PLUS_GENERAL', 'GENERAL_ONLY')
        THEN 1 ELSE 0 END AS theme_behavior_flag,
    CASE WHEN i.graph_behavior_class IN ('SEASONAL_LED', 'SEASONALITY_PLUS_GENERAL', 'GENERAL_ONLY')
              AND i.graph_evidence_tier = 'CONFIRMED'
        THEN 1 ELSE 0 END AS confirmed_commercial_theme_flag,

    -- Non-exclusive business flags
    i.seasonal_pattern_flag_v2 AS seasonality_flag,
    i.seasonal_pattern_flag_v2 AS seasonal_pattern_flag,
    i.measured_seasonal_uplift_flag_v2 AS measured_seasonal_uplift_flag,
    i.seasonal_led_flag_v2 AS seasonal_led_flag,
    i.general_sales_flag_v2 AS general_sales_flag,
    i.seasonality_plus_general_flag_v2 AS seasonality_plus_general_flag,
    i.general_only_flag_v2 AS general_only_flag,
    i.occasion_suppressed_flag,

    -- Before / event / after timing flags
    CASE WHEN i.seasonal_pattern_flag_v2 = 1 THEN i.season_before_flag ELSE 0 END AS season_before_flag,
    CASE WHEN i.seasonal_pattern_flag_v2 = 1 THEN i.buildup_flag ELSE 0 END AS buildup_flag,
    CASE WHEN i.seasonal_pattern_flag_v2 = 1 THEN i.pre_event_flag ELSE 0 END AS pre_event_flag,
    CASE WHEN i.seasonal_pattern_flag_v2 = 1 THEN i.season_event_flag ELSE 0 END AS season_event_flag,
    CASE WHEN i.seasonal_pattern_flag_v2 = 1 THEN i.season_after_flag ELSE 0 END AS season_after_flag,
    i.post_event_clearance_flag,
    i.seasonal_period_only_flag,
    i.seasonal_timing_profile_v2 AS seasonal_timing_profile,

    -- Original annual audit fields: never overwritten
    i.source_product_seasonality,
    i.source_seasonality_evidence,
    i.source_availability_pattern,
    i.source_insufficient_evidence_flag,
    i.source_review_required_flag,
    i.source_held_occasion_count,
    i.source_held_occasion_list,
    i.graph_feature_assignment_status AS graph_v1_assignment_status,

    -- Product history and data-state details
    i.first_sale_date,
    i.last_sale_date,
    i.observable_history_days,
    i.observed_sale_span_days,
    i.days_since_last_sale,

    -- Identity and occasion attribution
    i.explicit_seasonal_identity_flag,
    i.hard_seasonal_identity_flag,
    i.explicit_identity_count,
    i.explicit_identity_occasion_list,
    i.explicit_identity_family_list,
    i.explicit_identity_season_list,
    i.matched_identity_patterns,
    i.identity_timing_alignment_v2 AS identity_timing_alignment,
    i.occasion_attribution_status,
    i.occasion_identity_conflict_flag,

    -- Accepted and candidate occasions
    i.accepted_occasion_count,
    i.accepted_occasion_list,
    i.candidate_held_occasion_count,
    i.candidate_held_occasion_list,
    i.primary_occasion_key,
    i.primary_occasion,
    i.secondary_occasion,
    i.primary_occasion_weight_pct,
    i.secondary_occasion_weight_pct,
    i.seasonal_peak_count AS named_seasonal_peak_count,
    CASE
        WHEN i.accepted_occasion_count + i.candidate_held_occasion_count >= 2
          OR i.max_years_same_peak_season >= 2
            THEN 1 ELSE 0
    END AS multi_seasonal_peak_flag,

    -- Repeated time-of-year evidence
    i.usable_complete_years,
    i.max_years_same_peak_season,
    i.strongest_observed_peak_season,
    i.repeated_peak_season,
    i.strongest_peak_median_basket_share,
    i.repeated_peak_median_basket_share,
    i.strongest_peak_median_sales_share,
    i.repeated_peak_median_sales_share,
    i.observed_peak_season_list,
    i.maximum_single_year_peak_share,
    i.repeated_time_of_year_pattern_flag,
    i.repeated_time_of_year_seasonal_led_flag,

    -- Existing timing and broad profile
    i.shopping_start_days_before,
    i.shopping_peak_days_before,
    i.shopping_end_days_before,
    i.shopping_window_days,
    i.peak_season,
    i.time_of_year_pattern,
    i.best_8_week_share_pct,

    -- Normalised product-level demand and value features
    i.total_baskets,
    i.total_sales,
    i.avg_annual_sales,
    i.active_week_positions,
    i.active_quarters,
    i.effective_seasonal_basket_share_pct_v2 AS seasonal_basket_share_pct,
    100.0 - i.effective_seasonal_basket_share_pct_v2 AS general_basket_share_pct,
    i.effective_seasonal_sales_share_pct_v2 AS seasonal_sales_share_pct,
    100.0 - i.effective_seasonal_sales_share_pct_v2 AS general_sales_share_pct,
    i.seasonal_share_measure_type,
    i.effective_general_baskets_v2 AS general_baskets,
    i.effective_general_sales_v2 AS general_sales,
    i.effective_general_active_week_positions_v2 AS general_active_week_positions,
    i.effective_general_active_quarters_v2 AS general_active_quarters,
    i.effective_general_active_days_v2 AS general_active_days,
    i.seasonal_incremental_baskets,
    i.seasonal_incremental_sales,
    i.source_occasion_dependency_pct,
    i.max_seasonal_basket_share_index,
    i.max_seasonal_z_score,

    -- Normalised seasonal sales and demand across every occasion-year-phase period.
    -- Arrays are aligned by position; use seasonality_period_id_array as the key.
    i.period_feature_row_count,
    i.accepted_period_row_count,
    i.accepted_material_seasonal_period_count,
    i.accepted_measured_uplift_period_count,
    i.candidate_material_seasonal_period_count,
    i.accepted_material_period_baskets_total,
    i.accepted_material_period_sales_total,
    i.accepted_positive_incremental_baskets_total,
    i.accepted_positive_incremental_sales_total,
    i.seasonality_period_id_array,
    i.seasonality_period_occasion_key_array,
    i.seasonality_period_year_array,
    i.seasonality_period_phase_array,
    i.seasonality_period_sold_flag_array,
    i.seasonality_period_actual_baskets_array,
    i.seasonality_period_actual_basket_weight_array,
    i.seasonality_period_incremental_baskets_array,
    i.seasonality_period_incremental_basket_weight_array,
    i.seasonality_period_actual_sales_array,
    i.seasonality_period_actual_sales_weight_array,
    i.seasonality_period_incremental_sales_array,
    i.seasonality_period_incremental_sales_weight_array,
    i.seasonality_period_material_sales_flag_array,
    i.seasonality_period_measured_uplift_flag_array,
    i.seasonality_period_basket_share_index_array,
    i.seasonality_period_z_score_array,
    i.seasonality_period_feature_array,
    i.seasonality_period_vector_source,
    i.seasonality_period_sales_normalisation,

    -- Periodic events remain separate from annual flags
    i.periodic_high_response_flag,
    i.periodic_medium_evidence_flag,
    i.periodic_review_evidence_flag,
    i.periodic_high_event_count,
    i.periodic_medium_event_count,
    i.periodic_high_event_list,
    i.periodic_medium_event_list,
    i.periodic_review_event_list,
    i.periodic_high_incremental_baskets,
    i.periodic_high_incremental_sales
FROM interpreted i
CROSS JOIN prm p;


-- =====================================================================================
-- 6. SUBCLASS x BEHAVIOUR THEME NODES
--
-- These rows are ready to receive pooled GNN embeddings later. Candidate and low-
-- information nodes stay in the graph, but confirmed_commercial_theme_node_flag tells
-- downstream users which nodes are ready for business-facing Theme reporting.
-- =====================================================================================
DROP TABLE IF EXISTS lab_customer_value_analytics.seasonality_graph_v2_theme_nodes;

CREATE TABLE lab_customer_value_analytics.seasonality_graph_v2_theme_nodes
USING PARQUET AS
WITH base AS (
    SELECT *
    FROM lab_customer_value_analytics.seasonality_graph_v2_tpnb_features
),
occasion_counts AS (
    SELECT
        subcategory_code,
        graph_behavior_class,
        primary_occasion,
        COUNT(*) AS products,
        ROW_NUMBER() OVER (
            PARTITION BY subcategory_code, graph_behavior_class
            ORDER BY COUNT(*) DESC, primary_occasion
        ) AS rn
    FROM base
    WHERE primary_occasion IS NOT NULL AND TRIM(primary_occasion) <> ''
    GROUP BY subcategory_code, graph_behavior_class, primary_occasion
),
dominant_occasion AS (
    SELECT
        subcategory_code,
        graph_behavior_class,
        primary_occasion AS dominant_primary_occasion
    FROM occasion_counts
    WHERE rn = 1
),
peak_counts AS (
    SELECT
        subcategory_code,
        graph_behavior_class,
        COALESCE(repeated_peak_season, strongest_observed_peak_season, UPPER(peak_season)) AS peak_season,
        COUNT(*) AS products,
        ROW_NUMBER() OVER (
            PARTITION BY subcategory_code, graph_behavior_class
            ORDER BY COUNT(*) DESC,
                     COALESCE(repeated_peak_season, strongest_observed_peak_season, UPPER(peak_season))
        ) AS rn
    FROM base
    WHERE COALESCE(repeated_peak_season, strongest_observed_peak_season, UPPER(peak_season)) IS NOT NULL
    GROUP BY
        subcategory_code,
        graph_behavior_class,
        COALESCE(repeated_peak_season, strongest_observed_peak_season, UPPER(peak_season))
),
dominant_peak AS (
    SELECT
        subcategory_code,
        graph_behavior_class,
        peak_season AS dominant_peak_season
    FROM peak_counts
    WHERE rn = 1
),
agg AS (
    SELECT
        theme_node_key,
        MAX(theme_node_display_name) AS theme_node_display_name,
        subcategory_code,
        MAX(subcategory_name) AS subcategory_name,
        graph_behavior_class,
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
        100.0 * SUM(season_before_flag) / NULLIF(COUNT(*), 0) AS products_with_before_season_pct,
        100.0 * SUM(season_event_flag) / NULLIF(COUNT(*), 0) AS products_with_event_season_pct,
        100.0 * SUM(season_after_flag) / NULLIF(COUNT(*), 0) AS products_with_after_season_pct,
        100.0 * SUM(measured_seasonal_uplift_flag) / NULLIF(COUNT(*), 0)
            AS products_with_measured_uplift_pct,
        100.0 * SUM(seasonal_led_flag) / NULLIF(COUNT(*), 0) AS seasonal_led_product_pct,
        100.0 * SUM(general_sales_flag) / NULLIF(COUNT(*), 0) AS general_sales_product_pct,
        100.0 * SUM(periodic_high_response_flag) / NULLIF(COUNT(*), 0) AS periodic_high_product_pct,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(NULLIF(primary_occasion, '')))) AS primary_occasion_list,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(NULLIF(graph_evidence_tier, '')))) AS evidence_tier_list,
        MAX(as_of_date) AS as_of_date
    FROM base
    GROUP BY theme_node_key, subcategory_code, graph_behavior_class
)
SELECT
    a.theme_node_key,
    a.theme_node_display_name,
    a.subcategory_code,
    a.subcategory_name,
    a.graph_behavior_class,
    CASE WHEN a.graph_behavior_class IN ('SEASONAL_LED', 'SEASONALITY_PLUS_GENERAL', 'GENERAL_ONLY')
        THEN 1 ELSE 0 END AS theme_behavior_node_flag,
    CASE WHEN a.graph_behavior_class IN ('SEASONAL_LED', 'SEASONALITY_PLUS_GENERAL', 'GENERAL_ONLY')
              AND a.confirmed_tpnb_count > 0
        THEN 1 ELSE 0 END AS confirmed_commercial_theme_node_flag,
    a.tpnb_count,
    a.confirmed_tpnb_count,
    a.supported_tpnb_count,
    a.provisional_tpnb_count,
    a.low_information_tpnb_count,
    a.training_label_tpnb_count,
    a.total_baskets,
    a.total_sales,
    a.basket_weighted_seasonal_share_pct,
    a.basket_weighted_general_share_pct,
    a.avg_repeated_peak_median_share,
    a.avg_max_seasonal_basket_share_index,
    a.avg_max_seasonal_z_score,
    a.products_with_before_season_pct,
    a.products_with_event_season_pct,
    a.products_with_after_season_pct,
    a.products_with_measured_uplift_pct,
    a.seasonal_led_product_pct,
    a.general_sales_product_pct,
    a.periodic_high_product_pct,
    d.dominant_primary_occasion,
    p.dominant_peak_season,
    a.primary_occasion_list,
    a.evidence_tier_list,
    a.as_of_date
FROM agg a
LEFT JOIN dominant_occasion d
  ON a.subcategory_code = d.subcategory_code
 AND a.graph_behavior_class = d.graph_behavior_class
LEFT JOIN dominant_peak p
  ON a.subcategory_code = p.subcategory_code
 AND a.graph_behavior_class = p.graph_behavior_class;


-- =====================================================================================
-- END OF BUILD
-- Run seasonality_graph_features_v2_validation.sql immediately after this file.
-- =====================================================================================
