# Table catalog (100 distinct objects, 103 build statements)

Generated from the SQL: the *Reads* and *Read by* columns come from parsing every `CREATE TABLE` / `CREATE VIEW` statement, so they show the true dependencies. Descriptions are hand-written. `seasonality_` is dropped from names for width; every object is in `lab_customer_value_analytics`.

**Legend for Status**

- `EXPORT`: the view the Python pipeline consumes.
- `FINAL`: a canonical output of a stage.
- `intermediate`: read by at least one later table.
- `terminal`: nothing in these four scripts reads it (diagnostic, audit, or superseded).

External sources: `sales.invoice_line` (script 01 step 1) and `product.product` (step 2). Nothing else is read from outside `lab_customer_value_analytics`.


## Script 01: statistical engine (72 statements)

| # | Object | Line | Grain | Status | What it holds | Reads | Read by |
|---|---|---|---|---|---|---|---|
| 1 | `v2_params` | 99 | grain: 1 row | intermediate | Single row of tunable thresholds for the base engine. | (none) | `v2_calendar_days`, `v2_buildup_calibration`, `v2_occurrences`, `v2_availability_spells`, `v2_occasion_metrics`, `v2_entity_profile`, `v2_2_occasion_metrics`, `v2_6_price_drop_periods`, `v2_6_timing_daily`, `v2_6_product_timing`, `v2_6_occasion_timing`, `v2_6_product_occasion_profile`, `graph_tpnb_occasion_phase_features` |
| 2 | `v2_daily_fact` | 143 | day x entity | intermediate | Daily baskets/items/sales/units per TPNB, market (ALL), outlet and channel. Only read of raw sales. | `sales.invoice_line` | `v2_product_dim`, `v2_calendar_days`, `v2_buildup_calibration`, `v2_entity_daily`, `graph_tpnb_model_period_features` |
| 3 | `v2_product_dim` | 286 | TPNB | intermediate | One product-master row per sold TPNB with hierarchy and subclass entity id. | `product.product`, `v2_daily_fact` | `v2_buildup_calibration`, `v2_entity_daily`, `v2_occasion_metrics`, `v2_tpnb_summary`, `v2_validation_labels`, `v2_2_product_occasion_evidence`, `v2_4_product_occasion_policy`, `v2_6_occasion_weights`, `v2_5_periodic_trend_response`, `v2_6_periodic_confidence`, `v2_7_periodic_confidence` |
| 4 | `v2_occasion_config` | 404 | occasion | intermediate | Hand-edited occasion settings: family, windows, baseline, build-up search, recurrence. | (none) | `v2_buildup_calibration`, `v2_occurrences`, `v2_6_timing_daily`, `v2_6_occasion_weights`, `v2_4_periodic_matched_response` |
| 5 | `v2_event_calendar` | 514 | occasion x year | intermediate | Dated occurrences of each occasion for 2023-2026. | (none) | `v2_buildup_calibration`, `v2_occurrences` |
| 6 | `v2_calendar_days` | 847 | day | intermediate | Market totals and low-traffic flag per day. | `v2_daily_fact`, `v2_params` | `v2_buildup_calibration`, `v2_occurrences`, `v2_date_roles`, `v2_occasion_windows`, `v2_weekly_profile`, `v2_entity_profile`, `v2_2_market_context`, `v2_6_timing_daily`, `v2_6_product_timing`, `v2_6_occasion_weights`, `v2_4_periodic_matched_response`, `v2_5_periodic_trend_response`, `graph_v2_tpnb_year_peak_profile`, `graph_v2_tpnb_repeatability`, `graph_model_period_calendar` |
| 7 | `v2_buildup_calibration` | 950 | occasion x year | intermediate | Data-driven start of shopping per occasion-year; decides which occasions get a build-up window. | `v2_calendar_days`, `v2_daily_fact`, `v2_event_calendar`, `v2_occasion_config`, `v2_params`, `v2_product_dim` | `v2_occurrences` |
| 8 | `v2_occurrences` | 1463 | occurrence | intermediate | Windows and baseline ranges per occurrence, plus BUILDUP occurrences. | `v2_buildup_calibration`, `v2_calendar_days`, `v2_event_calendar`, `v2_occasion_config`, `v2_params` | `v2_date_roles`, `v2_occasion_windows`, `v2_entity_profile`, `v2_2_occasion_metrics`, `v2_2_product_occasion_evidence`, `v2_4_subclass_parent_shape`, `v2_5_parent_metadata`, `v2_6_timing_daily`, `v2_6_product_timing`, `v2_6_occasion_timing`, `v2_6_occasion_weights`, `v2_4_periodic_matched_response`, `v2_7_periodic_confidence` |
| 9 | `v2_date_roles` | 1590 | occurrence x date | intermediate | Role (PRE/EVENT/POST/BASE/YEAR) of each date for each completed occurrence. | `v2_calendar_days`, `v2_occurrences` | `v2_occasion_windows`, `v2_occasion_metrics`, `v2_entity_profile`, `v2_2_market_context`, `v2_2_absolute_metrics` |
| 10 | `v2_occasion_windows` | 1715 | occurrence | intermediate | Calendar-side weekday totals per occurrence and overlap map. | `v2_calendar_days`, `v2_date_roles`, `v2_occurrences` | `v2_occasion_metrics`, `v2_2_absolute_metrics`, `v2_2_occasion_metrics` |
| 11 | `v2_entity_daily` | 1961 | day x entity | intermediate | Daily baskets/sales/units for TPNBs and subclasses. | `v2_daily_fact`, `v2_product_dim` | `v2_availability_spells`, `v2_occasion_metrics`, `v2_weekly_profile`, `v2_entity_profile`, `v2_2_absolute_metrics`, `v2_6_price_drop_periods`, `v2_6_timing_daily`, `v2_4_periodic_matched_response`, `v2_5_periodic_trend_response`, `graph_tpnb_occasion_phase_features`, `graph_tpnb_features`, `graph_v2_tpnb_year_peak_profile`, `graph_v2_tpnb_repeatability`, `graph_tpnb_model_period_features` |
| 12 | `v2_availability_spells` | 2008 | entity x spell | intermediate | Runs of days an entity was on sale. | `v2_entity_daily`, `v2_params` | `v2_occasion_metrics`, `v2_weekly_profile`, `v2_entity_profile`, `v2_2_absolute_metrics`, `v2_6_timing_daily`, `v2_6_product_timing`, `v2_4_periodic_matched_response`, `v2_5_periodic_trend_response`, `graph_tpnb_occasion_phase_features` |
| 13 | `v2_occasion_metrics` | 2075 | entity x occurrence | intermediate | Core tests: expected vs actual, share index, z, verdict per entity and occasion. | `v2_availability_spells`, `v2_date_roles`, `v2_entity_daily`, `v2_occasion_windows`, `v2_params`, `v2_product_dim` | `v2_tpnb_summary`, `v2_2_absolute_metrics`, `v2_2_occasion_metrics`, `graph_tpnb_occasion_phase_features` |
| 14 | `v2_weekly_profile` | 2785 | entity x week | intermediate | Share of items per week of year; week index. | `v2_availability_spells`, `v2_calendar_days`, `v2_entity_daily` | `v2_entity_profile` |
| 15 | `v2_entity_profile` | 2899 | entity | intermediate | Time-of-year, day-of-week, availability and volume profile. | `v2_availability_spells`, `v2_calendar_days`, `v2_date_roles`, `v2_entity_daily`, `v2_occurrences`, `v2_params`, `v2_weekly_profile` | `v2_tpnb_summary`, `v2_2_product_occasion_evidence`, `v2_4_subclass_parent_shape` |
| 16 | `v2_tpnb_summary` | 3153 | TPNB | intermediate | All base flags rolled up to one row per TPNB, with subclass inheritance. | `v2_entity_profile`, `v2_occasion_metrics`, `v2_product_dim` | `v2_2_tpnb_summary` |
| 17 | `v2_validation_labels` | 3370 | TPNB | intermediate | Keyword seed list of known seasonal and everyday products. | `v2_product_dim` | `v2_4_review_sample` |
| 18 | `v2_2_params` | 3483 | 1 row | intermediate | v2.2 thresholds (abnormal trading, supported single occurrence). | (none) | `v2_2_market_context`, `v2_2_occasion_metrics`, `v2_2_product_occasion_evidence` |
| 19 | `v2_2_market_context` | 3506 | occurrence | intermediate | Is the market in an abnormal low-traffic regime in each core window. | `v2_2_params`, `v2_calendar_days`, `v2_date_roles` | `v2_2_absolute_metrics`, `v2_2_occasion_metrics` |
| 20 | `v2_2_absolute_metrics` | 3631 | entity x occurrence | intermediate | Absolute-demand index for candidates on abnormal windows. | `v2_2_market_context`, `v2_availability_spells`, `v2_date_roles`, `v2_entity_daily`, `v2_occasion_metrics`, `v2_occasion_windows` | `v2_2_occasion_metrics` |
| 21 | `v2_2_occasion_metrics` | 3882 | entity x occurrence | intermediate | v2.1 metrics plus mission-shift guard and revised verdict. | `v2_2_absolute_metrics`, `v2_2_market_context`, `v2_2_params`, `v2_occasion_metrics`, `v2_occasion_windows`, `v2_occurrences`, `v2_params` | `v2_2_product_occasion_evidence`, `v2_3_parent_signal_features`, `v2_3_periodic_response` |
| 22 | `v2_2_product_occasion_evidence` | 4135 | TPNB x parent occasion | intermediate | Event+build-up verdict per product and parent occasion; supported single-occurrence evidence; confidence. | `v2_2_occasion_metrics`, `v2_2_params`, `v2_entity_profile`, `v2_occurrences`, `v2_product_dim` | `v2_2_tpnb_summary`, `v2_2_responder_products`, `v2_3_product_occasion_evidence`, `v2_3_periodic_response` |
| 23 | `v2_2_tpnb_summary` | 4401 | TPNB | intermediate | Product roll-up of v2.2 evidence. | `v2_2_product_occasion_evidence`, `v2_tpnb_summary` | `v2_3_tpnb_summary` |
| 24 | `v2_2_responder_products` | 4508 | TPNB x occasion | terminal (unused downstream) | Candidate responder list (superseded). | `v2_2_product_occasion_evidence` | (none) |
| 25 | `v2_3_params` | 4561 | 1 row | intermediate | v2.3 thresholds (dominance, launch ramp, confidence, periodic). | (none) | `v2_3_product_occasion_evidence`, `v2_3_periodic_response` |
| 26 | `v2_3_parent_signal_features` | 4592 | entity x parent occasion | intermediate | Share and rank of each parent occasion in an entity's positive signal. | `v2_2_occasion_metrics` | `v2_3_product_occasion_evidence` |
| 27 | `v2_3_product_occasion_evidence` | 4690 | TPNB x parent occasion | intermediate | Refined supported evidence with dominance and launch-ramp guards; HIGH/MEDIUM/REVIEW. | `v2_2_product_occasion_evidence`, `v2_3_params`, `v2_3_parent_signal_features` | `v2_3_tpnb_summary`, `v2_4_product_occasion_policy` |
| 28 | `v2_3_tpnb_summary` | 4833 | TPNB | intermediate | Product roll-up of v2.3. | `v2_2_tpnb_summary`, `v2_3_product_occasion_evidence` | `v2_4_tpnb_summary` |
| 29 | `v2_3_periodic_response` | 4935 | TPNB x event x year | terminal (unused downstream) | Early periodic-event prototype (superseded). | `v2_2_occasion_metrics`, `v2_2_product_occasion_evidence`, `v2_3_params` | (none) |
| 30 | `v2_4_params` | 5005 | 1 row | intermediate | v2.4 thresholds (shape rescue, year-round override, review sample). | (none) | `v2_4_subclass_parent_shape`, `v2_4_product_occasion_policy`, `v2_4_review_sample` |
| 31 | `v2_4_subclass_parent_shape` | 5024 | subclass x parent occasion | intermediate | Does a subclass's seasonal shape match the occasion's season. | `v2_4_params`, `v2_entity_profile`, `v2_occurrences` | `v2_4_product_occasion_policy` |
| 32 | `v2_4_product_occasion_policy` | 5083 | TPNB x parent occasion | intermediate | Production status and STRICT/BALANCED/BROAD acceptance per product and occasion. | `v2_3_product_occasion_evidence`, `v2_4_params`, `v2_4_subclass_parent_shape`, `v2_product_dim` | `v2_4_tpnb_summary`, `v2_4_responder_products_strict`, `v2_4_responder_products_balanced`, `v2_4_review_sample`, `v2_5_product_occasion_policy` |
| 33 | `v2_4_tpnb_summary` | 5229 | TPNB | intermediate | Product roll-up under the three scenarios. | `v2_3_tpnb_summary`, `v2_4_product_occasion_policy` | `v2_5_tpnb_summary` |
| 34 | `v2_4_responder_products_strict` | 5309 | TPNB x occasion | terminal (unused downstream) | STRICT responder list (not used downstream). | `v2_4_product_occasion_policy` | (none) |
| 35 | `v2_4_responder_products_balanced` | 5323 | TPNB x occasion | terminal (unused downstream) | BALANCED responder list (not used downstream). | `v2_4_product_occasion_policy` | (none) |
| 36 | `v2_4_review_sample` | 5341 | TPNB x occasion (sample) | terminal (unused downstream) | Stratified deterministic sample for human review. | `v2_4_params`, `v2_4_product_occasion_policy`, `v2_validation_labels` | (none) |
| 37 | `v2_5_params` | 5400 | 1 row | intermediate | v2.5 thresholds (minor/major occasion guards). | (none) | `v2_5_product_occasion_policy` |
| 38 | `v2_5_parent_metadata` | 5417 | parent occasion | intermediate | Occasion name, family, major flag and typical month. | `v2_occurrences` | `v2_5_product_occasion_policy` |
| 39 | `v2_5_product_occasion_policy` | 5435 | TPNB x parent occasion | intermediate | Guarded acceptance of the STRICT set (identity and major/minor rules). | `v2_4_product_occasion_policy`, `v2_5_params`, `v2_5_parent_metadata` | `v2_5_tpnb_summary`, `v2_5_responder_products_annual`, `v2_6_product_occasion_policy` |
| 40 | `v2_5_tpnb_summary` | 5634 | TPNB | intermediate | Product roll-up after guards. | `v2_4_tpnb_summary`, `v2_5_product_occasion_policy` | `v2_6_tpnb_summary` |
| 41 | `v2_5_responder_products_annual` | 5683 | TPNB x occasion | terminal (unused downstream) | Guarded responder list (superseded by v2.6). | `v2_5_product_occasion_policy` | (none) |
| 42 | `v2_6_annual_params` | 5729 | 1 row | intermediate | v2.6 soft-affinity thresholds. | (none) | `v2_6_product_occasion_policy` |
| 43 | `v2_6_affinity_rules` | 5747 | rule | intermediate | Editable HARD/SOFT occasion-affinity keyword rules. | (none) | `v2_6_product_occasion_policy` |
| 44 | `v2_6_product_occasion_policy` | 5831 | TPNB x parent occasion | FINAL | FINAL annual decision (accepted_final_annual) per product and occasion. | `v2_5_product_occasion_policy`, `v2_6_affinity_rules`, `v2_6_annual_params` | `v2_6_responder_products_annual`, `v2_6_tpnb_summary`, `v2_6_product_occasion_profile`, `graph_annual_relationships` |
| 45 | `v2_6_responder_products_annual` | 6009 | TPNB x occasion | FINAL | FINAL accepted annual responders. Drives all timing work. | `v2_6_product_occasion_policy` | `v2_6_price_drop_periods`, `v2_6_timing_daily`, `v2_6_occasion_weights`, `v2_7_periodic_confidence` |
| 46 | `v2_6_tpnb_summary` | 6035 | TPNB | intermediate | Product roll-up of the final annual decision. | `v2_5_tpnb_summary`, `v2_6_product_occasion_policy` | `v2_6_theme_output` |
| 47 | `v2_6_price_drop_periods` | 6125 | TPNB x period | intermediate | Price-inferred promotion periods for accepted products. | `v2_6_responder_products_annual`, `v2_entity_daily`, `v2_params` | `v2_6_timing_daily`, `v2_6_product_timing`, `v2_6_theme_output`, `graph_tpnb_occasion_phase_features` |
| 48 | `v2_6_timing_daily` | 6249 | TPNB x occasion x year x day | intermediate | Expected vs actual baskets per day for accepted products around each occasion. | `v2_6_price_drop_periods`, `v2_6_responder_products_annual`, `v2_availability_spells`, `v2_calendar_days`, `v2_entity_daily`, `v2_occasion_config`, `v2_occurrences`, `v2_params` | `v2_6_product_timing`, `v2_6_occasion_daily_curve`, `graph_tpnb_model_period_features` |
| 49 | `v2_6_product_timing` | 6588 | TPNB x occasion x year | intermediate | Shopping start/mid/end/peak, range landing and price-drop alignment. | `v2_6_price_drop_periods`, `v2_6_timing_daily`, `v2_availability_spells`, `v2_calendar_days`, `v2_occurrences`, `v2_params` | `v2_6_occasion_timing`, `v2_6_product_occasion_profile` |
| 50 | `v2_6_occasion_daily_curve` | 6755 | occasion x year x day | intermediate | Daily demand and promotion curve per occasion. | `v2_6_timing_daily` | `v2_6_occasion_timing` |
| 51 | `v2_6_occasion_timing` | 6820 | occasion x year | intermediate | Customers' timing vs build-up, range landing and price drops. | `v2_6_occasion_daily_curve`, `v2_6_product_timing`, `v2_occurrences`, `v2_params` | `v2_6_occasion_weights` |
| 52 | `v2_6_occasion_weights` | 7031 | occasion | intermediate | Occasion importance weight and tier. | `v2_6_occasion_timing`, `v2_6_responder_products_annual`, `v2_calendar_days`, `v2_occasion_config`, `v2_occurrences`, `v2_product_dim` | `v2_6_product_occasion_profile` |
| 53 | `v2_6_product_occasion_profile` | 7186 | TPNB x occasion | FINAL | FINAL accepted product-occasion profile with timing and weights. | `v2_6_occasion_weights`, `v2_6_product_occasion_policy`, `v2_6_product_timing`, `v2_params` | `v2_6_theme_output` |
| 54 | `v2_6_theme_output` | 7323 | TPNB | FINAL | FINAL annual theme output, one row per TPNB. Main input to script 02. | `v2_6_price_drop_periods`, `v2_6_product_occasion_profile`, `v2_6_tpnb_summary` | `graph_tpnb_identity`, `graph_tpnb_features`, `graph_v2_tpnb_repeatability`, `graph_v2_tpnb_data_state`, `graph_v2_tpnb_features` |
| 55 | `v2_4_periodic_params` | 7531 | 1 row | intermediate | Periodic matched-calendar thresholds. | (none) | `v2_4_periodic_matched_response` |
| 56 | `v2_4_periodic_matched_response` | 7546 | occurrence x TPNB | intermediate | Periodic event vs same calendar dates in other years. | `v2_4_periodic_params`, `v2_availability_spells`, `v2_calendar_days`, `v2_entity_daily`, `v2_occasion_config`, `v2_occurrences` | `v2_5_periodic_trend_response` |
| 57 | `v2_5_periodic_params` | 7837 | 1 row | intermediate | Trend-adjustment thresholds. | (none) | `v2_5_periodic_trend_response` |
| 58 | `v2_5_periodic_trend_response` | 7856 | occurrence x TPNB | intermediate | Matched response corrected for pre-event trend. | `v2_4_periodic_matched_response`, `v2_5_periodic_params`, `v2_availability_spells`, `v2_calendar_days`, `v2_entity_daily`, `v2_product_dim` | `v2_6_periodic_confidence`, `v2_7_periodic_confidence` |
| 59 | `v2_6_periodic_params` | 8153 | 1 row | intermediate | HIGH/MEDIUM confidence thresholds. | (none) | `v2_6_periodic_confidence` |
| 60 | `v2_6_periodic_confidence` | 8168 | TPNB x event x year | intermediate | First confidence rating of periodic responses. | `v2_5_periodic_trend_response`, `v2_6_periodic_params`, `v2_product_dim` | `v2_6_periodic_event_summary` |
| 61 | `v2_6_periodic_event_summary` | 8343 | event x year x rating | terminal (unused downstream) | Event-level summary of the v2.6 rating. | `v2_6_periodic_confidence` | (none) |
| 62 | `v2_7_periodic_params` | 8388 | 1 row | intermediate | Same thresholds (v2.7). | (none) | `v2_7_periodic_confidence` |
| 63 | `v2_7_periodic_confidence` | 8403 | TPNB x event x year | intermediate | Token-safe matching plus annual-overlap hold. | `v2_5_periodic_trend_response`, `v2_6_responder_products_annual`, `v2_7_periodic_params`, `v2_occurrences`, `v2_product_dim` | `v2_7_periodic_event_summary`, `v2_8_periodic_confidence` |
| 64 | `v2_7_periodic_event_summary` | 8723 | event x year x rating | terminal (unused downstream) | Event-level summary (v2.7). | `v2_7_periodic_confidence` | (none) |
| 65 | `v2_8_periodic_params` | 8766 | 1 row | intermediate | Same thresholds (v2.8). | (none) | `v2_8_periodic_confidence` |
| 66 | `v2_8_periodic_confidence` | 8781 | TPNB x event x year | intermediate | Food/beverage scope for viewing missions. | `v2_7_periodic_confidence`, `v2_8_periodic_params` | `v2_8_periodic_event_summary`, `v2_9_periodic_confidence` |
| 67 | `v2_8_periodic_event_summary` | 8935 | event x year x rating | terminal (unused downstream) | Event-level summary (v2.8). | `v2_8_periodic_confidence` | (none) |
| 68 | `v2_9_periodic_params` | 8982 | 1 row | intermediate | Same thresholds (v2.9). | (none) | `v2_9_periodic_confidence` |
| 69 | `v2_9_periodic_confidence` | 8997 | TPNB x event x year | intermediate | Category-safe viewing missions. | `v2_8_periodic_confidence`, `v2_9_periodic_params` | `v2_9_periodic_event_summary`, `v2_10_periodic_confidence` |
| 70 | `v2_9_periodic_event_summary` | 9255 | event x year x rating | terminal (unused downstream) | Event-level summary (v2.9). | `v2_9_periodic_confidence` | (none) |
| 71 | `v2_10_periodic_confidence` | 9296 | TPNB x event x year | FINAL | FINAL periodic rating with annual-identity guard. Read by script 02. | `v2_9_periodic_confidence` | `v2_10_periodic_event_summary`, `graph_tpnb_features` |
| 72 | `v2_10_periodic_event_summary` | 9385 | event x year x rating | FINAL | FINAL event-level summary. | `v2_10_periodic_confidence` | (none) |

## Script 02: graph features (14 statements)

| # | Object | Line | Grain | Status | What it holds | Reads | Read by |
|---|---|---|---|---|---|---|---|
| 1 | `graph_params` | 50 | 1 row | intermediate | Graph v1 thresholds. | (none) | `graph_tpnb_occasion_phase_features`, `graph_tpnb_features` |
| 2 | `graph_identity_rules` | 82 | rule | intermediate | Editable seasonal-identity keyword dictionary. | (none) | `graph_tpnb_identity` |
| 3 | `graph_tpnb_identity` | 139 | TPNB | intermediate | Identity evidence per product. | `graph_identity_rules`, `v2_6_theme_output` | `graph_tpnb_features`, `graph_v2_tpnb_features` |
| 4 | `graph_annual_relationships` | 219 | TPNB x parent occasion | intermediate | Accepted and held product-occasion relationships. | `v2_6_product_occasion_policy` | `graph_tpnb_occasion_phase_features` |
| 5 | `graph_tpnb_occasion_phase_features` | 270 | TPNB x occasion x year x phase | intermediate | Phase-level (build-up/pre/event/post) demand features. | `graph_annual_relationships`, `graph_params`, `v2_6_price_drop_periods`, `v2_availability_spells`, `v2_entity_daily`, `v2_occasion_metrics`, `v2_params` | `graph_tpnb_features`, `graph_v2_tpnb_period_features` |
| 6 | `graph_tpnb_features` | 694 | TPNB | intermediate | Graph v1 node features. | `graph_params`, `graph_tpnb_identity`, `graph_tpnb_occasion_phase_features`, `v2_10_periodic_confidence`, `v2_6_theme_output`, `v2_entity_daily` | `graph_v2_tpnb_data_state`, `graph_v2_tpnb_features` |
| 7 | `graph_v2_params` | 1166 | 1 row | intermediate | Graph v2 thresholds. | (none) | `graph_v2_tpnb_year_peak_profile`, `graph_v2_tpnb_repeatability`, `graph_v2_tpnb_data_state`, `graph_v2_tpnb_features` |
| 8 | `graph_v2_tpnb_year_peak_profile` | 1199 | TPNB x year | intermediate | Peak broad season for each complete year. | `graph_v2_params`, `v2_calendar_days`, `v2_entity_daily` | `graph_v2_tpnb_repeatability` |
| 9 | `graph_v2_tpnb_repeatability` | 1308 | TPNB | intermediate | Does the peak season repeat across years. | `graph_v2_params`, `graph_v2_tpnb_year_peak_profile`, `v2_6_theme_output`, `v2_calendar_days`, `v2_entity_daily` | `graph_v2_tpnb_data_state`, `graph_v2_tpnb_features` |
| 10 | `graph_v2_tpnb_period_features` | 1482 | TPNB x occasion x year x phase | intermediate | Renamed phase features. | `graph_tpnb_occasion_phase_features` | `graph_v2_tpnb_period_rollup` |
| 11 | `graph_v2_tpnb_period_rollup` | 1562 | TPNB | intermediate | Aligned arrays over all occasion-year-phase periods. | `graph_v2_tpnb_period_features` | `graph_v2_tpnb_features` |
| 12 | `graph_v2_tpnb_data_state` | 1817 | TPNB | intermediate | Data-quality state per product. | `graph_tpnb_features`, `graph_v2_params`, `graph_v2_tpnb_repeatability`, `v2_6_theme_output` | `graph_v2_tpnb_features` |
| 13 | `graph_v2_tpnb_features` | 1910 | TPNB | FINAL | FINAL v2 node features with class and tier. Read by script 03. | `graph_tpnb_features`, `graph_tpnb_identity`, `graph_v2_params`, `graph_v2_tpnb_data_state`, `graph_v2_tpnb_period_rollup`, `graph_v2_tpnb_repeatability`, `v2_6_theme_output` | `graph_v2_theme_nodes`, `graph_tpnb_model_period_features`, `graph_tpnb_nodes` |
| 14 | `graph_v2_theme_nodes` | 2524 | theme node | terminal (unused downstream) | Theme-node aggregates (replaced by script 03). | `graph_v2_tpnb_features` | (none) |

## Script 03: final node build (13 distinct objects from 16 statements)

| # | Object | Line | Grain | Status | What it holds | Reads | Read by |
|---|---|---|---|---|---|---|---|
| 1 | `graph_final_params` | 18 | 1 row | intermediate | Window, period scheme, pair-support thresholds. | (none) | `graph_model_period_calendar`, `graph_tpnb_nodes`, `graph_tpnb_share_quality` |
| 2 | `graph_model_period_calendar` | 46 | date | intermediate | Date to P01-P12 mapping (calendar months). | `graph_final_params`, `v2_calendar_days` | `graph_model_period_summary`, `graph_tpnb_model_period_features` |
| 3 | `graph_model_period_summary` | 91 | period | terminal (unused downstream) | Period summary for validation. | `graph_model_period_calendar` | (none) |
| 4 | `graph_tpnb_model_period_features` | 137 | TPNB x year x period | intermediate | Per-product monthly features by year. | `graph_model_period_calendar`, `graph_v2_tpnb_features`, `v2_6_timing_daily`, `v2_daily_fact`, `v2_entity_daily` | `graph_tpnb_model_period_vectors`, `graph_tpnb_period_profile` |
| 5 | `graph_tpnb_model_period_vectors` | 429 | TPNB x year | intermediate | 12-element vectors by year. | `graph_tpnb_model_period_features` | `graph_tpnb_nodes` |
| 6 | `graph_tpnb_nodes` | 534 | TPNB | intermediate | First node table (superseded by nodes_final). | `graph_final_params`, `graph_tpnb_model_period_vectors`, `graph_v2_tpnb_features` | `graph_tpnb_share_quality`, `graph_tpnb_nodes_final` |
| 7 | `graph_theme_nodes_final` | 1325 | theme node | terminal (unused downstream) | Final theme nodes (built twice; second wins). | `graph_tpnb_nodes_final` | (none) |
| 8 | `graph_theme_node_period_features` | 1374 | theme x period | intermediate | Theme monthly features (built twice; second wins, different grain). | `graph_tpnb_nodes_final`, `graph_tpnb_period_profile` | `graph_theme_node_period_vectors` |
| 9 | `graph_theme_node_period_vectors` | 1417 | theme | terminal (unused downstream) | Theme 12-element vectors (built twice; second wins). | `graph_theme_node_period_features` | (none) |
| 10 | `graph_tpnb_period_profile` | 942 | TPNB x period | intermediate | All-years monthly profile per product. | `graph_tpnb_model_period_features` | `graph_theme_node_period_features`, `graph_tpnb_period_profile_vectors` |
| 11 | `graph_tpnb_period_profile_vectors` | 1063 | TPNB | intermediate | All-years 12-element vectors per product. | `graph_tpnb_period_profile` | `graph_tpnb_nodes_final` |
| 12 | `graph_tpnb_share_quality` | 1150 | TPNB | terminal (unused downstream) | Audit of products with out-of-range shares. | `graph_final_params`, `graph_tpnb_nodes` | (none) |
| 13 | `graph_tpnb_nodes_final` | 1201 | TPNB | FINAL | FINAL node table. Script 04 reads it. | `graph_tpnb_nodes`, `graph_tpnb_period_profile_vectors` | `graph_theme_nodes_final`, `graph_theme_node_period_features`, `graph_tpnb_nodes_model_ready` |

## Script 04: model-ready view (1 statement)

| # | Object | Line | Grain | Status | What it holds | Reads | Read by |
|---|---|---|---|---|---|---|---|
| 1 | `graph_tpnb_nodes_model_ready` | 3 | TPNB | EXPORT | VIEW exported to Python. | `graph_tpnb_nodes_final` | (none) |

## Cross-script dependencies (the minimum set that must survive between scripts)

- Script 2 reads from script 1: `v2_10_periodic_confidence`, `v2_6_price_drop_periods`, `v2_6_product_occasion_policy`, `v2_6_theme_output`, `v2_availability_spells`, `v2_calendar_days`, `v2_entity_daily`, `v2_occasion_metrics`, `v2_params`
- Script 3 reads from script 1: `v2_6_timing_daily`, `v2_calendar_days`, `v2_daily_fact`, `v2_entity_daily`
- Script 3 reads from script 2: `graph_v2_tpnb_features`

Everything else in script 01 can in principle be dropped after script 03 completes, but keep it while debugging, because script 01 is by far the largest build and later scripts' numbers trace back to it.

## Statements that appear twice

`graph_theme_nodes_final`, `graph_theme_node_period_features` and `graph_theme_node_period_vectors` are each created twice in script 03 (section B then section C). The SQL's `DROP TABLE IF EXISTS` makes the later one win. Their grain and columns differ; see [03_final_node_build.md](03_final_node_build.md).
