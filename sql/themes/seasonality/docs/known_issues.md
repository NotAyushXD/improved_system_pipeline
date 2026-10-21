# Known issues, gotchas and open questions

Found while documenting the SQL. Nothing here has been changed in the SQL; each item says what was observed and what the options are. Severity: **High** = will break or mislead, **Medium** = needs care, **Low** = housekeeping.

## High

### 1. Script 04 selects two columns that no script produces
`04_seasonality_output.sql` selects `n.sales_share_quality_status` and `n.sales_share_feature_available_flag` from `seasonality_graph_tpnb_nodes_final`.

How this was checked: the 149 distinct `n.<column>` names in script 04 were each searched for in script 03; 147 appear, these two do not. A search of script 02 also finds neither. They are not created in scripts 01, 02 or 03 of this repo.

Expected consequence: the view creation fails with a column-not-found error, *unless* the version of `seasonality_graph_tpnb_nodes_final` in your warehouse was built by a different script that does create them.

Options: (a) remove the two lines from script 04, (b) add the two columns to script 03 (a sensible definition is a status derived from `share_clamped_flag`/`share_out_of_bounds_flag` and a flag that the share is available when the product has a seasonal pattern), or (c) locate the missing script. Also check whether the Python code expects them.

### 2. Time-window values are hard-coded to 2023-2026
The data window, event calendar, year-specific sales columns (`sales_2023`...`sales_2026_ytd`), `avg_annual_sales` (2023-2025) and `sales_trend` (2025 vs 2024) are literals. When the window moves they must be edited by hand; see [parameters.md](parameters.md). Until then, new years are silently ignored.

### 3. The event calendar contains unverified dates
Rows with `date_origin = SUPPLEMENT_V2_VERIFY` (Lunar New Year, Diwali, Ramadan, Eid, Holi, Passover, Hanukkah, Wimbledon, Six Nations, Grand National, FA Cup Final) were looked up manually and the SQL comments ask for confirmation. Wrong dates produce wrong windows and baselines for those occasions. Some occasions also lack dates for some years, which reduces the number of eligible years.

## Medium

### 4. Three tables are built twice in script 03 with different grain
`seasonality_graph_theme_nodes_final`, `seasonality_graph_theme_node_period_features` and `seasonality_graph_theme_node_period_vectors` are created in section B (per theme x year) and again in section C (all years). A full run leaves the section C version, but an interrupted run can leave the old shape. See [03](03_final_node_build.md).

### 5. Promotion and range-landing dates are inferred
Promotions come from the average price paid falling at least 10% below a reference. If `sales.invoice_line.amount` is before Clubcard or multi-buy discounts, those promotions are invisible. Range landing comes from the first sale in a spell, not a ranging system. The output labels these `PRICE_INFERRED_FROM_AVERAGE_PRICE_PAID` and `OBSERVED_SALES_SPELL`. Treat timing comparisons against price drops as indicative.

### 6. P01-P12 are calendar months, not Tesco commercial periods
Everything called "period" in scripts 02-04 uses January to December until the official calendar is supplied. Vectors will change meaning if the calendar table is replaced, so any model trained on them needs retraining.

### 7. Two different "season" definitions
- `peak_season`, `time_of_year_pattern`, `best_8_week_share_pct` (script 01 step 13): season of the product's strongest week by calendar month (Dec-Feb Winter, Mar-May Spring...).
- `repeated_peak_season`, `strongest_observed_peak_season` (script 02): broad seasons by week number (Winter weeks 49-52 and 1-8, Spring 9-21, Summer 22-34, Autumn 35-48), per complete year.

They can disagree for the same product. Use each only for its own purpose.

### 8. Incomplete year (2026 year-to-date)
Incomplete years cannot pass the annual "specific" concentration test and are excluded from the repeated time-of-year logic (complete years only), but they do count toward local uplift evidence and toward the all-years P01-P12 vectors. Re-running at a different date changes the vectors and `as_of_date`.

### 9. Script 02 drops products missing from the theme output
`seasonality_graph_v2_tpnb_features` inner-joins `seasonality_v2_6_theme_output`. Any TPNB without a theme row is absent from every later table. In practice the theme output has every sold TPNB, so this should be a no-op, but a row-count check between `seasonality_v2_tpnb_summary`, `seasonality_v2_6_theme_output` and `seasonality_graph_tpnb_nodes_final` is advisable.

### 10. Periodic results look like annual ones but mean something else
`seasonality_v2_10_periodic_confidence` carries columns from every earlier version (`periodic_response_confidence`, `..._v2_9`, `..._v2_10`). Only the `_v2_10` columns are final. Periodic flags in the output (`periodic_*`) must not feed annual seasonality or be read as causal.

### 11. Run-time dependence on `CURRENT_DATE()`
Steps that stamp `as_of_date`/`feature_as_of_date` and cap the data window use today's date, so re-running another day is not byte-identical.

## Low (housekeeping)

12. Script 01's header lists a statement `seasonality_v2_promo_periods`; the real table is `seasonality_v2_6_price_drop_periods`.
13. Script 01's header and the Part A comments refer to "16 scoring queries" that are not in the file.
14. Script 01's end note and script 02's end note reference `seasonality_themes_final_post_run_validation.sql` and `seasonality_graph_features_v2_validation.sql`, which are not in this repo. These validations are currently unavailable; consider adding them next to the build scripts.
15. Script 03's prerequisite comments name `00_graph_model_periods_and_params.sql`, `10_graph_model_periods_and_params.sql` and `11_graph_tpnb_period_base.sql`; those are now sections of the same file. Script 01 references the v2.2-v2.6 files by their old file names, now merged into it.
16. Several tables are produced and never read: `seasonality_v2_2_responder_products`, `v2_3_periodic_response`, `v2_4_responder_products_strict/balanced`, `v2_4_review_sample`, `v2_5_responder_products_annual`, the periodic event summaries, `graph_v2_theme_nodes`, `graph_model_period_summary`, `graph_tpnb_share_quality`. They are useful for review and audit; see the Status column in [table_catalog.md](table_catalog.md).
17. The names carry version numbers (`v2_6`, `v2_10`, `graph_v2`) that do not match the file name (`...v2_10.sql` for script 01, `...v2_1...` for script 02). Version labels are historical; the run order in the README is the source of truth.
18. `seasonality_v2_validation_labels` is a keyword seed, not ground truth.

## Open questions for the data owners
1. What is the official Tesco commercial period calendar (for P01-P12)?
2. Is there a certified promotion calendar or ranging table that can replace the price/spell inference?
3. Who confirms the `SUPPLEMENT_V2_VERIFY` dates?
4. Should products without a subclass ever be commercial themes? Today they are grouped by fallback level and never commercially eligible.
5. Where do `sales_share_quality_status` / `sales_share_feature_available_flag` come from (issue 1)?
6. How should the data window roll forward (a new `analysis_end` and year columns)?
