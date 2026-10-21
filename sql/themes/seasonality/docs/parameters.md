# Parameters: every tunable threshold and where it lives

Each script keeps its thresholds in small one-row "params" tables near the top of each part, so you can tune behaviour without touching logic. Change a value **and re-run that script from that point onward** (later tables read the earlier ones).

| Params table | Script and line | Controls | Detail |
|---|---|---|---|
| `seasonality_v2_params` | 01, line 99 | Base engine tests and windows | [01 part A](01_seasonality_themes.md#0-parameters) |
| `seasonality_v2_2_params` | 01, line 3483 | Abnormal low-traffic regime and supported single occurrence | [01 parts B-F](01_seasonality_themes.md#parameters-seasonality_v2_2_params) |
| `seasonality_v2_3_params` | 01, line 4561 | Dominance, launch ramp, confidence, periodic prototype | [01 parts B-F](01_seasonality_themes.md#parameters-seasonality_v2_3_params) |
| `seasonality_v2_4_params` | 01, line 5005 | Shape rescue, year-round override, review sample | [01 parts B-F](01_seasonality_themes.md#parameters-seasonality_v2_4_params) |
| `seasonality_v2_5_params` | 01, line 5400 | Major/minor occasion guards | [01 parts B-F](01_seasonality_themes.md#parameters-seasonality_v2_5_params) |
| `seasonality_v2_6_annual_params` | 01, line 5729 | Soft affinity thresholds | [01 parts B-F](01_seasonality_themes.md#parameters-seasonality_v2_6_annual_params) |
| `seasonality_v2_4_periodic_params` | 01, line 7531 | Matched-calendar control | [01 parts H-N](01_seasonality_themes.md#script-01-parts-h-to-n-periodic-and-one-off-events-v24-to-v210) |
| `seasonality_v2_5_periodic_params` | 01, line 7837 | Pre-event trend adjustment | [01 parts H-N](01_seasonality_themes.md#script-01-parts-h-to-n-periodic-and-one-off-events-v24-to-v210) |
| `seasonality_v2_6/7/8/9_periodic_params` | 01, lines 8153 / 8388 / 8766 / 8982 | HIGH / MEDIUM confidence (identical in all four versions) | [01 parts H-N](01_seasonality_themes.md#script-01-parts-h-to-n-periodic-and-one-off-events-v24-to-v210) |
| `seasonality_graph_params` | 02, line 50 | Graph v1 general-sales and phase materiality | [02](02_graph_features.md) |
| `seasonality_graph_v2_params` | 02, line 1166 | Repeated pattern, general sales, data state | [02](02_graph_features.md) |
| `seasonality_graph_final_params` | 03, line 18 | Window, period scheme, pair support | [03](03_final_node_build.md) |

Hand-edited **reference tables** (not params, but also meant to be edited):

| Table | Script and line | What to edit |
|---|---|---|
| `seasonality_v2_occasion_config` | 01, line 404 | Occasions, windows, baselines, build-up search, recurrence |
| `seasonality_v2_event_calendar` | 01, line 514 | Occasion dates (a literal list; plus computed and supplement dates) |
| `seasonality_v2_6_affinity_rules` | 01, line 5747 | Occasion-affinity keywords (HARD/SOFT) for minor occasions |
| `seasonality_graph_identity_rules` | 02, line 82 | Seasonal identity keywords |
| `seasonality_v2_validation_labels` rules | 01, line 3372 | Seed keywords for validation |

## Constants that are NOT in a params table

These are written directly in the logic. Changing them means editing the SQL.

### Dates and years
| Constant | Where | Effect |
|---|---|---|
| `2023-01-01` and `2027-01-01` | 01 step 1; 03 params | Data window |
| Event years 2023-2026 | 01 step 4 (`event_year BETWEEN 2023 AND 2026`) | Calendar range |
| `sales_2023`, `sales_2024`, `sales_2025`, `sales_2026_ytd` | 01 step 13 | Year columns |
| `avg_annual_sales` over 2023-2025; `sales_trend` = 2025 vs 2024 (+/-15%) | 01 step 24 | Output volume fields |
| `DATE '2023-01-01'` used to turn a week number into a month | 01 step 13 | Season label of the peak week |
| `CURRENT_DATE()` | 01 step 1 and step 5; 02 `feature_as_of_date` | Cuts off future dates and stamps `as_of_date`; re-running on another day can change results |

### Test logic
| Constant | Where | Effect |
|---|---|---|
| 0.5 / 0.9 / 1.1 (inside-share and share-ratio) | 01 step 11 attribution | Which overlapping occasion gets credit |
| 0.8 (on-sale share), 150 days, 28 days | 01 step 13 `availability_pattern` | Year-round / Recurring seasonal / Launched / Delisted rules |
| 2.0 and 1.4 | 01 step 13 `time_of_year_pattern` | Strong peak vs peak |
| 3.0 (z) and 8/52 | 01 step 13 | Day-of-week significance; flat share of 8 weeks |
| Event length <= 14 days | 01 step 11 | Which occasions can be Occasion-suppressed |
| 80th percentile, current + 2 prior months | 01 step 17 | Reference price for price drops |
| 28 days after data start | 01 step 19 | Range-landing guard |
| Importance tiers at cumulative 50% and 80% | 01 step 22 | Tier 1 / 2 / 3 |
| 1.15 / 0.85 | 01 step 24 | `sales_trend` |
| Division `HOME & CLOTHING` special case | 01 v2.2, v2.4 | Stricter support for non-food seasonal ranges |
| Foods divisions list (WINES & SPIRITS, AMBIENT DRY GROCERY, FRESH FOODS, BREAD/BAKERY & REST.) | 01 v2.8, v2.9 | Scope for periodic viewing missions |
| +/- 3 days search for control dates | 01 periodic H, I | Matched-calendar control |
| 20 baskets for the identity-with-Recurring-seasonal route | 02 v1.1 | Minimum for an identity-supported pattern |
| Season definitions: Winter weeks 49-52 and 1-8, Spring 9-21, Summer 22-34, Autumn 35-48 | 02 v2 | Broad seasons for repeated patterns |

### Performance
| Constant | Where | Effect |
|---|---|---|
| 32 hash buckets | 01 step 1 | Spreads each day over tasks |
| `REPARTITION(400, entity_id)` | 01 step 11 and absolute metrics | Partition count; tune to cluster |

## Tuning guidance

| If you want... | Change |
|---|---|
| More/fewer products flagged seasonal | `uplift_index_min`, `z_min`, `specific_ratio_min`, `min_year_share` in `seasonality_v2_params` |
| Longer or shorter windows around one occasion | its row in `seasonality_v2_occasion_config` |
| A new occasion | add it to the event calendar **and** the config |
| Stop an occasion getting a build-up | set its `buildup_search_days` to `NULL` |
| Stricter/looser acceptance of weak evidence | the v2.3-v2.5 shares and ranks |
| Credit a product to a minor occasion | add a HARD rule to `seasonality_v2_6_affinity_rules` (preferred over loosening thresholds) |
| Treat a word as a seasonal identity | add it to `seasonality_graph_identity_rules` |
| Official commercial periods instead of months | replace `seasonality_graph_model_period_calendar` in script 03 |
