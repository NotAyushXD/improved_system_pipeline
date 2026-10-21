# Themes SQL

Themes group products (TPNBs) by *how and when they sell*. Today there is one theme family, **seasonality**. Other families can be added as sibling folders.

## Sub-modules and run order

| Order | Folder | What it produces | Status |
|---|---|---|---|
| 1 | [`seasonality/`](seasonality/README.md) | One row per product saying whether it is seasonal, for which occasion, when customers shop, how much of its sales depend on the occasion, and a 12-month profile. Products map to themes (`<subclass>\|<behaviour class>`). | Documented |

There is only one sub-module, so nothing needs sequencing across folders yet. When more are added, record here whether they depend on each other.

## Reading order

1. This page for orientation.
2. [`seasonality/README.md`](seasonality/README.md): the four scripts to run in order, what to export, key concepts, FAQ.
3. [`seasonality/docs/`](seasonality/docs/): the detailed references.

| Document | Read it when you need to... |
|---|---|
| [00_concepts_and_glossary](seasonality/docs/00_concepts_and_glossary.md) | learn the vocabulary |
| [01 parts A](seasonality/docs/01_seasonality_themes.md#script-01-part-a-the-base-engine-v21-statements-0-15) | understand how seasonality is measured |
| [01 parts B-F](seasonality/docs/01_seasonality_themes.md#script-01-parts-b-to-f-annual-policy-layers-v22-to-v26) | understand why an occasion was accepted or held |
| [01 part G](seasonality/docs/01_seasonality_themes.md#script-01-part-g-shopping-timing-weights-and-the-annual-theme-output-statements-17-24) | understand shopping timing, price drops and occasion weights |
| [01 parts H-N](seasonality/docs/01_seasonality_themes.md#script-01-parts-h-to-n-periodic-and-one-off-events-v24-to-v210) | understand one-off events like the World Cup |
| [02_graph_features](seasonality/docs/02_graph_features.md) | understand behaviour classes and evidence tiers |
| [03_final_node_build](seasonality/docs/03_final_node_build.md) | understand 12-month vectors and final nodes |
| [04_model_ready_output](seasonality/docs/04_model_ready_output.md) | look up a column in the exported file |
| [table_catalog](seasonality/docs/table_catalog.md) | see every table and what feeds it |
| [parameters](seasonality/docs/parameters.md) | change a threshold, occasion or keyword |
| [known_issues](seasonality/docs/known_issues.md) | see problems and open questions |

## What leaves this folder

| Final output | Exported to | Consumed by |
|---|---|---|
| `lab_customer_value_analytics.seasonality_graph_tpnb_nodes_model_ready` (view from `04_seasonality_output.sql`) | `data/themes/raw/` as Parquet | `src/cie/themes` |

Theme outputs later feed need states, NBA and opportunities.

> Before running, see [known_issues #1](seasonality/docs/known_issues.md): script 04 currently references two columns that earlier scripts do not create.
