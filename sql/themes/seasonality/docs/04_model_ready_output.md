# Script 04: the model-ready output (what Python consumes)

File: [`04_seasonality_output.sql`](../04_seasonality_output.sql)

Creates one **view**, `lab_customer_value_analytics.seasonality_graph_tpnb_nodes_model_ready`, which is a plain `SELECT` of 151 columns (150 from the node table plus the constant `node_type`) from `seasonality_graph_tpnb_nodes_final` (script 03). No filtering, no new logic. It fixes the **contract** with the Python side.

- **Grain:** one row per TPNB (every sold product).
- **Primary key:** `tpnb` (also exposed as `node_id`).
- **Export:** materialise the view and save as Parquet in `data/themes/raw/` (proposed name `seasonality_graph_tpnb_nodes_model_ready.parquet`).
- **Renames in the view:** `tpnb` is also `node_id`; `subcategory_code/name` become `source_subcategory_code/name`; `final_theme_node_key` -> `theme_node_key`; `final_theme_node_display_name` -> `theme_node_display_name`. `node_type` is the constant `'TPNB'`.
- **Types:** flags are integers 0/1; `*_list` fields are comma-separated strings (possibly empty); `*_array_12` fields are arrays of 12 numbers (P01 first); `model_period_feature_array_12` is an array of 12 structs; `*_pct` fields are 0-100 unless a column says otherwise; dates are `DATE`.

> **Before you run this script, read [known_issues.md](known_issues.md) issue 1.** As written, the view selects two columns that no script in 01-03 produces.

## Column dictionary

### Node identity and hierarchy

| Column | Meaning |
|---|---|
| `node_id`, `tpnb`, `node_type` | Product key; `node_type = 'TPNB'` |
| `product_description` | Customer-friendly description from the product master |
| `hierarchy_department_name`, `hierarchy_section_name`, `hierarchy_class_name` | Commercial hierarchy names |
| `source_subcategory_code`, `source_subcategory_name` | Subclass as in the product master; **may be missing** (script 03 turns it into `UNKNOWN_SUBCLASS` in the intermediate table) |
| `effective_subcategory_code`, `effective_subcategory_name` | Subclass, or the fallback level's code/name when missing (`CLASS_FALLBACK:<...>`, `SECTION_FALLBACK:<...>`, `DEPARTMENT_FALLBACK:<...>`, `UNKNOWN_SUBCLASS`) |
| `theme_hierarchy_level` | `SUBCLASS`, `CLASS_FALLBACK`, `SECTION_FALLBACK`, `DEPARTMENT_FALLBACK`, `UNKNOWN` |
| `missing_subclass_flag` | 1 if the source subclass was missing |
| `theme_node_key` | `<effective_subcategory_code>\|<graph_behavior_class>`. **The theme this product belongs to** |
| `theme_node_display_name` | `"<name> - <class label>"` for display |
| `commercial_theme_eligible_flag` | Hierarchy level is SUBCLASS and class is SEASONAL_LED / SEASONALITY_PLUS_GENERAL / GENERAL_ONLY |
| `confirmed_commercial_theme_eligible_flag` | The above and evidence tier CONFIRMED. **Business-ready themes** |

### Final behaviour and evidence controls

| Column | Meaning |
|---|---|
| `graph_behavior_class` | `SEASONAL_LED`, `SEASONALITY_PLUS_GENERAL`, `GENERAL_ONLY`, `SEASONAL_PATTERN_CANDIDATE`, `GENERAL_PATTERN_CANDIDATE`, `LOW_INFORMATION_NODE` (rules in [02](02_graph_features.md)) |
| `graph_behavior_reason` | `ACCEPTED_NAMED_ANNUAL_SEASONALITY`, `HELD_OR_REVIEWED_SEASONAL_SIGNAL`, `REPEATED_TIME_OF_YEAR_PATTERN_R45_2Y`, `EXPLICIT_IDENTITY_WITH_OBSERVED_OR_RANGE_PATTERN`, `CONFIRMED_DISTRIBUTED_GENERAL_SALES`, `PROVISIONAL_DISTRIBUTED_GENERAL_SALES`, `NO_RELIABLE_BEHAVIOUR_YET` |
| `graph_evidence_tier` | `CONFIRMED`, `SUPPORTED`, `PROVISIONAL`, `LOW_INFORMATION` |
| `graph_data_state` | Data-quality description (`MATURE_MULTI_YEAR_HISTORY`, `COLD_START_NEW_PRODUCT_UNDER_12_MONTHS`, `SHORT_LIVED_OR_DELISTED_RANGE`, `LOW_VOLUME_LONG_TAIL`, `INTERMITTENT_OR_RANGE_LIMITED`, `ONE_OFF_RANGE_NO_REPEAT_TEST`, `ATTRIBUTION_CONFLICT_OR_REVIEW`, `SEASONAL_IDENTITY_WITHOUT_CONFIRMED_BEHAVIOUR`, `SUFFICIENT_VOLUME_NO_STABLE_ANNUAL_PATTERN`, `OTHER_LIMITED_EVIDENCE`) |
| `graph_training_label_flag` | 1 only for accepted named seasonality and confirmed general-only products. **The only rows that may be used as supervised labels** |
| `graph_training_label_weight` | The flag as a number (1.0 or 0.0) |
| `graph_node_inclusion_flag` | Always 1 |
| `source_product_seasonality` | Script 01's own annual label, unchanged: Specifically seasonal / Uplift seasonal / Occasion-suppressed / Regular / Review required / Insufficient evidence |
| `source_seasonality_evidence` | `V2_6_FINAL_ANNUAL_POLICY`, `V2_6_REVIEW_QUEUE`, `TPNB`, `SUBCLASS (inherited)` or `NONE` |
| `source_availability_pattern` | `Year-round`, `Recurring seasonal`, `One-off limited edition`, `Launched during period`, `Delisted during period`, `Intermittent` |
| `source_insufficient_evidence_flag` | Source label is Insufficient evidence |
| `source_review_required_flag` | Source label is Review required |

### Core behaviour flags (non-exclusive)

| Column | Meaning |
|---|---|
| `seasonality_flag`, `seasonal_pattern_flag` | Any seasonal evidence route (accepted, held/reviewed, repeated time-of-year, or identity-supported) |
| `measured_seasonal_uplift_flag` | A phase showed measured uplift, or accepted with source Uplift seasonal |
| `seasonal_led_flag` | Mostly seasonal (accepted Specifically, or seasonal share >= 50%, or no material general sales) |
| `general_sales_flag` | Material sales outside seasonal periods |
| `seasonality_plus_general_flag` | Seasonal and general |
| `general_only_flag` | General and no seasonal pattern |
| `occasion_suppressed_flag` | At least one occasion where sales are significantly *lower* |

### Seasonal timing

| Column | Meaning |
|---|---|
| `season_before_flag` | Material seasonal demand in a BUILDUP or PRE_EVENT phase |
| `buildup_flag`, `pre_event_flag` | In BUILDUP / in PRE_EVENT specifically |
| `season_event_flag`, `season_after_flag` | In EVENT / POST_EVENT |
| `post_event_clearance_flag` | At least 10% of POST sales were on price drop |
| `seasonal_period_only_flag` | Seasonal but with no before/event/after phase flag |
| `seasonal_timing_profile` | Combination label (`BEFORE_EVENT_AND_AFTER`, `BEFORE_AND_EVENT`, `EVENT_AND_AFTER`, `BEFORE_AND_AFTER`, `BEFORE_ONLY`, `EVENT_ONLY`, `AFTER_ONLY`, `SEASONAL_PERIOD_ONLY`, `NO_SEASONAL_PATTERN`) |
| `shopping_start_days_before`, `shopping_peak_days_before`, `shopping_end_days_before` | For the primary occasion: median days before the event when customers start / peak / finish buying |
| `shopping_window_days` | Length of that shopping window |

Timing is only meaningful where `seasonal_pattern_flag = 1` and the product has an accepted primary occasion; otherwise it is null or zero.

### Seasonal identity and occasion attribution

| Column | Meaning |
|---|---|
| `explicit_seasonal_identity_flag` | The product or hierarchy text matches an identity rule (MINCE PIE, HALLOWEEN...) |
| `hard_seasonal_identity_flag` | Matched at least one HARD rule |
| `explicit_identity_count` | Distinct occasions matched |
| `explicit_identity_occasion_list`, `explicit_identity_family_list`, `explicit_identity_season_list` | Matched occasions, families, seasons |
| `matched_identity_patterns` | The words that matched |
| `identity_timing_alignment` | Does the observed peak fall in the identity season (`ALIGNED_WITHIN_IDENTITY_SEASON`, `ALIGNED_BEFORE_...`, `ALIGNED_AFTER_...`, `ALIGNED_TO_UNRELATED_SEASON`, `SEASONAL_IDENTITY_WITHOUT_RELIABLE_PEAK_SEASON`, `NO_SEASONAL_ALIGNMENT`, `NO_EXPLICIT_SEASONAL_IDENTITY`, `CONFLICTING_NAMED_OCCASION`) |
| `occasion_attribution_status` | `CONFIDENT_ACCEPTED_OCCASION`, `AMBIGUOUS_OR_HELD_NAMED_OCCASION`, `CONFLICTING_PRODUCT_IDENTITY`, `SEASONAL_PERIOD_ONLY`, `NO_ACCEPTED_OCCASION` |
| `occasion_identity_conflict_flag` | The product's wording points to a different occasion than the statistics |
| `accepted_occasion_count`, `accepted_occasion_list` | Occasions accepted by the final annual policy |
| `candidate_held_occasion_count`, `candidate_held_occasion_list` | Held occasions that still show a seasonal signal |
| `primary_occasion_key`, `primary_occasion` | The accepted occasion with the largest annual incremental sales |
| `secondary_occasion` | Second largest |
| `primary_occasion_weight_pct`, `secondary_occasion_weight_pct` | Each as a percent of the product's total accepted annual incremental sales |
| `named_seasonal_peak_count` | Distinct occasions with a seasonal phase |
| `multi_seasonal_peak_flag` | 2+ accepted/held occasions, or 2+ years with the same peak season |

### Repeated time-of-year pattern

| Column | Meaning |
|---|---|
| `usable_complete_years` | Complete years with >= 104 baskets and >= 8 active weeks |
| `max_years_same_peak_season` | Most years sharing the same broad peak season |
| `strongest_observed_peak_season`, `repeated_peak_season` | Most frequent peak season; the same only if it recurs in 2+ years (WINTER, SPRING, SUMMER, AUTUMN) |
| `strongest_peak_median_basket_share`, `repeated_peak_median_basket_share` | Median share (0-1) of a year's baskets falling in that season |
| `strongest_peak_median_sales_share`, `repeated_peak_median_sales_share` | Same by sales |
| `observed_peak_season_list` | All seasons that were a peak in any usable year |
| `maximum_single_year_peak_share` | Highest single-year peak share (0-1) |
| `repeated_time_of_year_pattern_flag` | 2+ usable years, 2+ with the same peak, median share >= 0.45 |
| `repeated_time_of_year_seasonal_led_flag` | Same with median share >= 0.50 |
| `peak_season` | Season (by calendar month: Dec-Feb Winter, Mar-May Spring, Jun-Aug Summer, otherwise Autumn) of the product's single strongest week of the year. Note this differs from the week-number-based seasons used for the repeated pattern |
| `time_of_year_pattern` | `Low volume`, `Strong <season> peak`, `<Season> peak`, `Even through the year` |
| `best_8_week_share_pct` | Share (0-100) of the product's weekly index in its best 8 consecutive weeks. Flat = 15.4 |

### Scale, lifecycle and availability

| Column | Meaning |
|---|---|
| `first_sale_date`, `last_sale_date` | First and last day with a sale |
| `observable_history_days` | Data end date minus first sale |
| `observed_sale_span_days` | Last sale minus first sale |
| `days_since_last_sale` | Data end date minus last sale |
| `total_baskets`, `total_sales` | Whole-window totals |
| `avg_annual_sales` | Average yearly sales over 2023-2025 years with sales |
| `active_week_positions` | Distinct weeks of year (1-52) with a sale |
| `active_quarters` | Distinct calendar quarters with a sale |
| `general_active_week_positions`, `general_active_quarters`, `general_active_days` | The same counted **only outside** the seasonal periods (or off the repeated peak season) |

### Seasonal vs general shares and quality

| Column | Meaning |
|---|---|
| `seasonal_basket_share_pct_raw`, `general_basket_share_pct_raw` | Seasonal share of baskets (0-100, can fall outside in edge cases) and 100 minus it |
| `final_seasonal_basket_share_pct`, `final_general_basket_share_pct` | Raw values clamped to 0-100. **Use these** |
| `seasonal_sales_share_pct_raw`, `general_sales_share_pct_raw` | Same by sales |
| `final_seasonal_sales_share_pct`, `final_general_sales_share_pct` | Clamped |
| `sales_share_quality_status`, `sales_share_feature_available_flag` | **Not produced by scripts 01-03** (see known issues) |
| `seasonal_share_measure_type` | How the seasonal share was measured: `NAMED_OCCASION_PHASES`, `REPEATED_BROAD_SEASON_R45_2Y`, `IDENTITY_WITH_OBSERVED_PATTERN_PROXY`, `NO_SEASONAL_SHARE` |
| `general_baskets`, `general_sales` | Baskets and sales outside seasonal periods (or off the repeated peak) |
| `seasonal_incremental_baskets`, `seasonal_incremental_sales` | Positive extra demand above baseline in seasonal phases (accepted occasions if any, otherwise held ones) |
| `source_occasion_dependency_pct` | Incremental occasion sales as % of average annual sales, capped at 100 (script 01) |
| `max_seasonal_basket_share_index`, `max_seasonal_z_score` | Strongest share index and z-score across the product's seasonal phases |

### Fixed P01-P12 vectors (node embedding inputs)

All are **arrays of exactly 12 numbers**, index 1 = P01 = January (provisional calendar-month scheme), covering all years in the window and normalised **within the product**. Zero where the product has no data for a period.

| Column | Meaning |
|---|---|
| `period_sold_flag_array_12` | 1 if the product sold in that month in any year |
| `period_observed_year_count_array_12` | Number of years of the window in which that month exists (up to 4; the current year may be partial) |
| `period_sold_year_count_array_12` | Years in which the product sold in that month |
| `period_actual_baskets_array_12`, `period_actual_sales_array_12` | Total baskets / sales in that month across all years |
| `period_actual_basket_weight_array_12`, `period_actual_sales_weight_array_12` | Share of the product's total (sums to 1) |
| `period_positive_excess_baskets_array_12`, `period_positive_excess_sales_array_12` | Above an even spread over active days (values >= 0) |
| `period_positive_excess_basket_weight_array_12`, `period_positive_excess_sales_weight_array_12` | Their shares (sum to 1 if any excess) |
| `period_named_incremental_baskets_array_12`, `period_named_incremental_sales_array_12` | Extra demand attributed to accepted named occasions, mapped to the month of each day |
| `period_named_incremental_basket_weight_array_12`, `period_named_incremental_sales_weight_array_12` | Their shares |
| `period_seasonality_basket_weight_array_12`, `period_seasonality_sales_weight_array_12` | **The seasonality profile**: named incremental share if the product has any, else repeated-pattern excess share, else all zeros |
| `period_seasonal_flag_array_12` | 1 where that month carries seasonal demand |
| `period_basket_rate_index_array_12`, `period_sales_rate_index_array_12` | Mean actual / active-day expectation (1.0 = average rate) |
| `period_basket_penetration_index_array_12` | Mean ratio of the month's market penetration to the year's |
| `model_period_feature_array_12` | Array of 12 structs holding the same features per period (`period_number`, `sold_flag`, `observed_year_count`, `sold_year_count`, `actual_baskets`, `actual_sales`, weights, `positive_excess_*`, `named_incremental_*`, `seasonality_*_weight`, `seasonal_flag`, rate indices, `basket_penetration_index`) |
| `final_period_vector_order` | `P01_TO_P12_ALL_OBSERVED_YEARS` |
| `final_period_normalisation_scope` | `WITHIN_TPNB_FULL_ANALYSIS_WINDOW` |

### Periodic and one-off events (kept separate from annual seasonality)

| Column | Meaning |
|---|---|
| `periodic_high_response_flag` | At least one periodic event rated HIGH (final v2.10 rating) |
| `periodic_medium_evidence_flag`, `periodic_review_evidence_flag` | At least one MEDIUM / REVIEW |
| `periodic_high_event_count`, `periodic_medium_event_count` | Distinct HIGH / MEDIUM events |
| `periodic_high_event_list`, `periodic_medium_event_list`, `periodic_review_event_list` | Event names |
| `periodic_high_incremental_baskets`, `periodic_high_incremental_sales` | Total trend-adjusted extra baskets / sales for HIGH events |

Never read these as recurring seasonality. They are observed responses to events such as the World Cup, without causal claim.

### Housekeeping

| Column | Meaning |
|---|---|
| `as_of_date` | Date script 02 ran |

## Using it in Python (practical notes)

- Validate that every `*_array_12` has length 12 and `period_*` arrays have no nulls.
- Group by `theme_node_key` for themes. Keep only `confirmed_commercial_theme_eligible_flag = 1` for business reporting.
- For supervised learning, restrict to rows with `graph_training_label_flag = 1`; these are the only rows the SQL treats as label-grade (accepted named seasonality and confirmed general-only). The SQL does not define the target column; decide that in the modelling step. Candidate classes must not be used as labels.
- Treat `seasonal_pattern_flag` and `general_sales_flag` as independent features.
- Many text lists are empty strings rather than null.
- Expect most products to have no accepted occasion (all occasion fields null or empty, vectors showing everyday sales).
