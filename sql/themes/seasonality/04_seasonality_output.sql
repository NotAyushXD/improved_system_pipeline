DROP VIEW IF EXISTS lab_customer_value_analytics.seasonality_graph_tpnb_nodes_model_ready;

CREATE VIEW lab_customer_value_analytics.seasonality_graph_tpnb_nodes_model_ready AS
SELECT
    -- -----------------------------------------------------------------------------
    -- Node identity and hierarchy
    -- -----------------------------------------------------------------------------
    n.tpnb AS node_id,
    'TPNB' AS node_type,
    n.tpnb,
    n.product_description,
    n.hierarchy_department_name,
    n.hierarchy_section_name,
    n.hierarchy_class_name,
    n.subcategory_code AS source_subcategory_code,
    n.subcategory_name AS source_subcategory_name,
    n.effective_subcategory_code,
    n.effective_subcategory_name,
    n.theme_hierarchy_level,
    n.missing_subclass_flag,
    n.final_theme_node_key AS theme_node_key,
    n.final_theme_node_display_name AS theme_node_display_name,
    n.commercial_theme_eligible_flag,
    n.confirmed_commercial_theme_eligible_flag,

    -- -----------------------------------------------------------------------------
    -- Final behaviour and evidence controls
    -- -----------------------------------------------------------------------------
    n.graph_behavior_class,
    n.graph_behavior_reason,
    n.graph_evidence_tier,
    n.graph_data_state,
    n.graph_training_label_flag,
    n.graph_training_label_weight,
    n.graph_node_inclusion_flag,
    n.source_product_seasonality,
    n.source_seasonality_evidence,
    n.source_availability_pattern,
    n.source_insufficient_evidence_flag,
    n.source_review_required_flag,

    -- -----------------------------------------------------------------------------
    -- Core seasonal/general behaviour flags
    -- -----------------------------------------------------------------------------
    n.seasonality_flag,
    n.seasonal_pattern_flag,
    n.measured_seasonal_uplift_flag,
    n.seasonal_led_flag,
    n.general_sales_flag,
    n.seasonality_plus_general_flag,
    n.general_only_flag,
    n.occasion_suppressed_flag,

    -- -----------------------------------------------------------------------------
    -- Seasonal timing flags
    -- -----------------------------------------------------------------------------
    n.season_before_flag,
    n.buildup_flag,
    n.pre_event_flag,
    n.season_event_flag,
    n.season_after_flag,
    n.post_event_clearance_flag,
    n.seasonal_period_only_flag,
    n.seasonal_timing_profile,
    n.shopping_start_days_before,
    n.shopping_peak_days_before,
    n.shopping_end_days_before,
    n.shopping_window_days,

    -- -----------------------------------------------------------------------------
    -- Seasonal identity and occasion attribution
    -- -----------------------------------------------------------------------------
    n.explicit_seasonal_identity_flag,
    n.hard_seasonal_identity_flag,
    n.explicit_identity_count,
    n.explicit_identity_occasion_list,
    n.explicit_identity_family_list,
    n.explicit_identity_season_list,
    n.matched_identity_patterns,
    n.identity_timing_alignment,
    n.occasion_attribution_status,
    n.occasion_identity_conflict_flag,
    n.accepted_occasion_count,
    n.accepted_occasion_list,
    n.candidate_held_occasion_count,
    n.candidate_held_occasion_list,
    n.primary_occasion_key,
    n.primary_occasion,
    n.secondary_occasion,
    n.primary_occasion_weight_pct,
    n.secondary_occasion_weight_pct,
    n.named_seasonal_peak_count,
    n.multi_seasonal_peak_flag,

    -- -----------------------------------------------------------------------------
    -- Repeated time-of-year pattern features
    -- -----------------------------------------------------------------------------
    n.usable_complete_years,
    n.max_years_same_peak_season,
    n.strongest_observed_peak_season,
    n.repeated_peak_season,
    n.strongest_peak_median_basket_share,
    n.repeated_peak_median_basket_share,
    n.strongest_peak_median_sales_share,
    n.repeated_peak_median_sales_share,
    n.observed_peak_season_list,
    n.maximum_single_year_peak_share,
    n.repeated_time_of_year_pattern_flag,
    n.repeated_time_of_year_seasonal_led_flag,
    n.peak_season,
    n.time_of_year_pattern,
    n.best_8_week_share_pct,

    -- -----------------------------------------------------------------------------
    -- Product scale, lifecycle and availability
    -- -----------------------------------------------------------------------------
    n.first_sale_date,
    n.last_sale_date,
    n.observable_history_days,
    n.observed_sale_span_days,
    n.days_since_last_sale,
    n.total_baskets,
    n.total_sales,
    n.avg_annual_sales,
    n.active_week_positions,
    n.active_quarters,
    n.general_active_week_positions,
    n.general_active_quarters,
    n.general_active_days,

    -- -----------------------------------------------------------------------------
    -- Normalised seasonal/general shares and their quality controls
    -- -----------------------------------------------------------------------------
    n.seasonal_basket_share_pct_raw,
    n.general_basket_share_pct_raw,
    n.final_seasonal_basket_share_pct,
    n.final_general_basket_share_pct,
    n.seasonal_sales_share_pct_raw,
    n.general_sales_share_pct_raw,
    n.final_seasonal_sales_share_pct,
    n.final_general_sales_share_pct,
    n.sales_share_quality_status,
    n.sales_share_feature_available_flag,
    n.seasonal_share_measure_type,
    n.general_baskets,
    n.general_sales,
    n.seasonal_incremental_baskets,
    n.seasonal_incremental_sales,
    n.source_occasion_dependency_pct,
    n.max_seasonal_basket_share_index,
    n.max_seasonal_z_score,

    -- -----------------------------------------------------------------------------
    -- Fixed P01-P12 period vectors used as node embedding inputs
    -- -----------------------------------------------------------------------------
    n.period_sold_flag_array_12,
    n.period_observed_year_count_array_12,
    n.period_sold_year_count_array_12,
    n.period_actual_baskets_array_12,
    n.period_actual_sales_array_12,
    n.period_actual_basket_weight_array_12,
    n.period_actual_sales_weight_array_12,
    n.period_positive_excess_baskets_array_12,
    n.period_positive_excess_sales_array_12,
    n.period_positive_excess_basket_weight_array_12,
    n.period_positive_excess_sales_weight_array_12,
    n.period_named_incremental_baskets_array_12,
    n.period_named_incremental_sales_array_12,
    n.period_named_incremental_basket_weight_array_12,
    n.period_named_incremental_sales_weight_array_12,
    n.period_seasonality_basket_weight_array_12,
    n.period_seasonality_sales_weight_array_12,
    n.period_seasonal_flag_array_12,
    n.period_basket_rate_index_array_12,
    n.period_sales_rate_index_array_12,
    n.period_basket_penetration_index_array_12,
    n.model_period_feature_array_12,
    n.final_period_vector_order,
    n.final_period_normalisation_scope,

    -- -----------------------------------------------------------------------------
    -- Periodic / one-off event features kept separate from annual seasonality
    -- -----------------------------------------------------------------------------
    n.periodic_high_response_flag,
    n.periodic_medium_evidence_flag,
    n.periodic_review_evidence_flag,
    n.periodic_high_event_count,
    n.periodic_medium_event_count,
    n.periodic_high_event_list,
    n.periodic_medium_event_list,
    n.periodic_review_event_list,
    n.periodic_high_incremental_baskets,
    n.periodic_high_incremental_sales,

    n.as_of_date
FROM lab_customer_value_analytics.seasonality_graph_tpnb_nodes_final n;