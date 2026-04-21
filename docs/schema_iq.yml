# schema_iq.yml

# ==============================================================================
# STATISTICAL INTEGRITY PREREQUISITES (TA_DQ CONTRACT)
# ==============================================================================
# IMPORTANT: This scoring engine assumes the input dataframe has been pre-filtered 
# by a Data Quality layer. If these conditions are met, the N=40 (CLT) 
# and Z-score calculations are statistically valid.
#
# 1. ONE-TO-ONE INTEGRITY (Anti-Clumping):
#    Input must be restricted to "Unique Search" requisitions (openings_total = 1).
#    "One-to-Many" (Class/Volume) hiring creates massive data clumps of identical
#    values. Including them violates the assumption of Independent Observations 
#    and collapses the standard deviation, making Z-scores meaningless.
#
# 2. TRUE TIME-TO-EVENT (Variance Validity):
#    Requisitions must represent a competitive search process.
#    - EXCLUDE: "Evergreen" requisitions (never close, infinite duration).
#    - EXCLUDE: "Pre-Identified/Admin" fills (Time-to-Fill < 2 days).
#    Including "Zero Variance" administrative events artificially punishes 
#    legitimate searches by shrinking the "Normal" range.
#
# 3. SAMPLE STABILITY (CLT Threshold):
#    The engine relies on the Central Limit Theorem (N >= 40).
#    Groups with N < 40 are statistically unstable. The hierarchical loop
#    must be allowed to expand to broader groups 
#    (e.g., Recruiter -> Country -> Org) until a stable N >= 40 pool is found.
# ==============================================================================

notes:
  identity_convention: >
    Columns prefixed with 'lineage_' describe the comparison population used to build statistical context.
    Columns prefixed with 'unique_id_' describe the individual observation being evaluated against that comparison population.

  sampling_convention: >
    lineage_top_n controls sampling.
    If lineage_top_n is a number, the comparison population is the most recent N observations per group (after sorting).
    If lineage_top_n is null/None, the comparison population is the full available population for that group (no sampling).



# ---------- lineage and sampling controls

lineage_sort_column: >
  Column used to order records before sampling. Used only when lineage_top_n is set, so the "most recent N" can be selected.

lineage_top_n: >
  Sample size parameter (N). If set, the comparison population is limited to N observations per group.
  If null/None, no sampling is applied and the full group population is used.

lineage_timestamp: >
  Timestamp when this lineage snapshot was generated. Useful for audit and reproducibility.

lineage_cluster_name: >
  Label for the cluster definition used (the specific grouping recipe that produced this comparison population).

lineage_cluster_group: >
  High-level comparison lens that the cluster belongs to (for example recruiter, segment, or company).

lineage_cluster_group_level: >
  Relative level within the cluster_group hierarchy (more specific to more general within that group).

lineage_column_level: >
  Number of grouping dimensions used to define the cluster. Higher numbers mean more specific grouping.

lineage_dict_name: >
  Identifier for the cluster instance produced by the lineage recipe, typically combining group and columns.



# ---------- Comparison population statistics (cluster level)

lineage_unique_id_mean: >
  Mean (average) metric value computed from the comparison population for this cluster.

lineage_unique_id_std: >
  Standard deviation computed from the comparison population for this cluster.

lineage_unique_id_survival_25th: >
  25th percentile landmark for this metric within the comparison population (first quartile threshold).

lineage_unique_id_survival_50th: >
  50th percentile landmark (median) for this metric within the comparison population.

lineage_unique_id_survival_75th: >
  75th percentile landmark for this metric within the comparison population (third quartile threshold).



# ---------- Global context across clusters (for a given metric)

total_lineage_mean: >
  Mean of lineage_unique_id_mean across all clusters for the same metric (global cluster baseline).

total_lineage_std: >
  Standard deviation of lineage_unique_id_mean across all clusters for the same metric.

total_lineage_z_score: >
  Z-score of this clusters lineage_unique_id_mean relative to the distribution of clusters for the same metric.
  This supports cluster-to-cluster comparison and similarity space construction.



# ---------- Observation-level comparison (the requisition being evaluated)

unique_id_z_score: >
  Z-score of the individual observations metric value relative to its comparison population
  (uses lineage_unique_id_mean and lineage_unique_id_std for that cluster).

unique_id_z_score_positive_only: >
  Absolute value of unique_id_z_score. Measures magnitude of deviation without direction.



# ---------- Signal outputs derived from observation vs population

signal_survival_unique_id: >
  Survival-position signal for the individual observation relative to the cluster survival landmarks.
  Encoded as a simple low/mid/high position (e.g. 0.25 / 0.50 / 0.75) without assuming direction.

metric_direction_unique_id: >
  Direction setting for the metric. Indicates whether higher values are better, lower values are better,
  or the metric is non-directional.

bin_unique_id_z_score_direction: >
  Directional correctness indicator for the individual observation.
  Encodes whether the deviation is in the expected direction (+1), unexpected direction (-1), or neutral (0).



# ---------- Thresholds and severity encoding

z_score_normal: >
  Z-score magnitude threshold defining the "normal" range for this metric.

z_score_watch: >
  Z-score magnitude threshold defining an early warning range for this metric.

z_score_monitor: >
  Z-score magnitude threshold defining a more serious monitoring range for this metric.

z_score_inspect: >
  Z-score magnitude threshold defining the highest inspection range for this metric.

z_score_severity: >
  Discrete severity bucket derived from absolute z-score magnitude using the thresholds above
  (for example 0=normal, 1=watch, 2=monitor, 3=inspect).



# ---------- Floor and ceiling goal constraints

lineage_metric_value_floor: >
  Minimum acceptable value for the metric, pulled from schema/config for the metric.

lineage_metric_value_ceiling: >
  Maximum acceptable value for the metric, pulled from schema/config for the metric.

unique_id_met_floor_ceiling: >
  Binary indicator: 1 if the individual observations metric value falls within [floor, ceiling], else 0.

