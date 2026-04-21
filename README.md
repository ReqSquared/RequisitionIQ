# RequisitionIQ

A config-driven decision intelligence framework for Talent Acquisition.

## The Framework

RequisitionIQ is built on a five-stage methodology:

**Read** (Assumption): Signals, models, raw data, KPIs/SLAs. What do we think we know?

**Engineer** (Structure): Explore, modify, classify, cluster, group. How do we organize what we have?

**Qualify** (Proof): Variance within, variance between, similarity. Is what we found statistically real?

**Investigate** (Conflict): Voting system, veto system. When signals disagree, who wins?

**Quantify** (Action): Human-readable output for action. What does the person actually do with this?


The framework is delivered across four levels:

| Level | Name | Purpose | Status |
|-------|------|---------|--------|
| 1 | Historic Context | Define normal before detecting abnormal | Built (this repo) |
| 2 | Context Intelligence | Infer and predict the outcome of requisitions | Future coursework |
| 3 | Pipeline Health | You can't manage requisitions when you can't monitor | North Star |
| 4 | Advanced Intelligence | Learn from requisition reality | Only what is learned in class |

## This Repository: Level 1

This codebase is Level 1: Historic Context. It takes one filled requisition and compares it against its peer group across multiple context lenses (recruiter, org, company) using z-scores, survival percentiles, and company goal thresholds. The same req gets a different signal depending on which lens you look through. That is the point.

Level 1 is descriptive statistics. Not predictive. Not prescriptive. The engine produces signals. What you do with them is yours.

## The Pipeline

`requisition_iq.py` runs five steps, top to bottom:

1. Build the observation dataframe (current quarter reqs with schema config signals)
2. Build sample population dataframes (prior quarter fills, grouped by context lens, sample size per config top_n)
3. Expand observations into every possible context lens (recruiter x country x job_group x job_family x job_level, and every subset)
4. Z-score each observation against its sample population group
5. Add survival percentiles from sample population groups

Each step produces a named, inspectable dataframe. Nothing is hidden.

## How Context Works

A single requisition gets compared across every context lens defined in the config. How many lenses qualify depends on the data. If a groupby combination meets the configured `top_n` sample threshold, it becomes a valid context lens. If it does not, the engine moves to the next broader grouping.

Context lens hierarchy examples from `config/metric_config.yml`:

- Recruiter context (top_n: 42): recruiter + country + job_group + job_family + job_level (most specific) down to recruiter alone (broadest)
- Country context (top_n: 240): country + job_group + job_family + job_level down to country alone
- Job group context (top_n: 480): job_group + job_family + job_level down to job_group alone

The z-score tells you how this req compares to that specific peer group. A req can be "faster than this recruiter's normal" and "slower than the org" at the same time. Both are true. Both are useful.

The number of valid context lenses per req varies entirely based on the data meeting the configured thresholds.

## Signal System

Signal levels are schema-driven. Each metric defines its own z-score thresholds, direction, floor, and ceiling in `config/schema_filled_reqs.yml`. Default thresholds:

| Signal | Threshold | Meaning |
|--------|-----------|---------|
| Normal | z < 0.85 | Within expected range |
| Watch | z < 1.15 | Early deviation |
| Monitor | z < 1.96 | Significant deviation |
| Inspect | z > 1.96 | Requires attention |

Direction matters: lower time-to-fill is good (direction = -1), higher review rate is good (direction = 1). These are not hardcoded. Change the schema, change the signals.

## Example Output: One Requisition, Four Metrics, Seven Context Lenses

This is what the engine produces for a single req across context lenses that met the configured `top_n` threshold:

| ID | Context ID | Metric | Value | Z-Score | Mean | SD | Survival Range |
|----|-----------|--------|-------|---------|------|----|----------------|
| RIQ-0014 | org_UK_Tech_Engineering_Entry | applicants_total | 89 | 1.11 | 50 | 36 | 19 to 77 |
| RIQ-0014 | org_UK_Tech_Engineering | applicants_total | 89 | 0.52 | 67 | 42 | 27 to 99 |
| RIQ-0014 | org_UK | applicants_total | 89 | 0.48 | 69 | 41 | 38 to 94 |
| RIQ-0014 | company_Tech_Engineering_Entry | applicants_total | 89 | 1.15 | 52 | 32 | 28 to 69 |
| RIQ-0014 | company_Tech | applicants_total | 89 | 0.58 | 64 | 43 | 25 to 96 |
| RIQ-0014 | org_UK_Tech_Engineering_Entry | days_to_first_screen | 15 | -0.39 | 18 | 8 | 11 to 24 |
| RIQ-0014 | org_UK | days_to_first_screen | 15 | -0.22 | 17 | 7 | 10 to 22 |
| RIQ-0014 | company_Tech_Engineering_Entry | days_to_first_screen | 15 | -0.07 | 16 | 8 | 7 to 21 |
| RIQ-0014 | org_UK_Tech_Engineering_Entry | percent_reviewed_in_x_days | 62% | 0.44 | 47% | 34% | 15% to 80% |
| RIQ-0014 | org_UK | percent_reviewed_in_x_days | 62% | 0.55 | 42% | 35% | 14% to 68% |
| RIQ-0014 | company_Tech_Engineering_Entry | percent_reviewed_in_x_days | 62% | 0.91 | 33% | 31% | 8% to 53% |
| RIQ-0014 | org_UK_Tech_Engineering_Entry | time_to_fill | 62 | -0.92 | 68 | 7 | 62 to 72 |
| RIQ-0014 | org_UK | time_to_fill | 62 | 0.20 | 58 | 23 | 39 to 76 |
| RIQ-0014 | company_Tech_Engineering_Entry | time_to_fill | 62 | -1.48 | 74 | 8 | 68 to 79 |
| RIQ-0014 | company_Tech | time_to_fill | 62 | 0.16 | 60 | 15 | 47 to 70 |

The same value (62 days time-to-fill) scores -1.48 against company_Tech_Engineering_Entry (faster than that peer group) and +0.20 against org_UK (slightly slower). Context changes the signal. That is the entire thesis.

Primary benchmarks (most specific context lens per group) and secondary benchmarks (broader context lenses) are kept separate in the output. Primary drives decisions. Secondary provides stability and confirmation.

## Files

| File | Role |
|------|------|
| `requisition_iq.py` | Pipeline orchestrator. Runs steps 1 through 5. |
| `iq.py` | Pipeline functions. All statistical logic lives here. |
| `etl_functions.py` | Shared ETL functions. Lineage columns, groupby, z-scores, survival bins. |
| `data_importer.py` | Config loader. Reads YAML, loads CSVs, applies data types, resolves pipeline configs. |
| `config/schema_filled_reqs.yml` | Column schema with data types, descriptions, z-score thresholds, direction, floor/ceiling. |
| `config/metric_config.yml` | Context lens definitions (which groupby combinations to run) and global metric selections. |
| `config/dataframe.yml` | Pipeline configuration. Which data source, which quarter is observation vs population, which schema and cluster config to use. |

## Documentation

| Document | What it covers |
|----------|---------------|
| `docs/schema_iq.yml` | Full data dictionary for every column the engine produces. Statistical integrity prerequisites. Sampling conventions. Signal output definitions. Start here if you want to understand the output. |
| `docs/RequisitionIQ_Level1.docx` | Language-agnostic methodology walkthrough. Design constraints, data requirements, clustering model (linear backoff fallback), z-score calculation, and the logic behind the context lens hierarchy. |

## Author

Victor Alberts
[reqsquared.com](https://reqsquared.com)

Copyright (C) ReqSquared. All Rights Reserved.
