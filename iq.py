# iq.py

import streamlit as st
import pandas as pd
import numpy as np

import etl_functions as efunc

# ------------------------------ REQ_IQ_CLUSTER_DATAFRAME
# Benchmarking analytical lens: requisition → recruiter, segment, company metrics
# Outputs follow naming standard: req_iq_cluster_1_[operation]_[container]
# lineage_* tracking enables audit trail through ETL pipeline



# ---------- mean_sd_dict_of_df
# Builds dictionary of mean/sd DataFrames for each cluster level
# top_n=None means "use config value", pass explicit value to override

@st.cache_data
def new_metric_target_column_df(df, metrics):

    concat_metric_target_column_df = {}

    for metric in metrics:

        dict_name = f"{metric}_schema_config_df"

        tmp_df = df.copy()

        tmp_df["target_column"] = metric

        tmp_df["target_column_value"] = tmp_df[metric]

        concat_metric_target_column_df[dict_name] = tmp_df

    return pd.concat(concat_metric_target_column_df)

@st.cache_data
def new_lineage_df_req_iq_cluster(df, target_column, req_iq_cluster_list_x, unique_id, top_n=None, sort_column=None, ascending_equals=None, use_config_top_n=True):

    concat_req_iq_cluster_df = {}

    for cluster in req_iq_cluster_list_x:

        dict_name = f"{cluster.cluster_group}_{cluster.columns}"

        # Use config top_n if use_config_top_n=True and no override provided, otherwise use passed top_n
        effective_top_n = cluster.config_top_n if (use_config_top_n and top_n is None) else top_n

        tmp_df = efunc.new_group_by_top_n_columns_df(
        df,
        cluster.columns,
        sort_column,
        effective_top_n,
        ascending_equals,
        unique_id
        )

        tmp_df = efunc.add_lineage_columns(tmp_df,
                                           cluster.cluster_name,
                                           cluster.cluster_group,
                                           cluster.columns,
                                           cluster.column_level,
                                           sort_column,
                                           effective_top_n,
                                           target_column
                                           )

        concat_req_iq_cluster_df[dict_name] = tmp_df

    return pd.concat(concat_req_iq_cluster_df)



# ---------- Z_SCORE_MATRIX
# use_config_top_n=True: use per-profile top_n from YAML config
# use_config_top_n=False: ignore config, use top_n param (None = no limit)
@st.cache_data
def new_lineage_df_top_n(df, metrics, req_iq_cluster_list_x, unique_id, schema, sort_column=None, ascending_equals=None, top_n=None, use_config_top_n=True):

    concat_top_n_df = {}

    for metric in metrics:

        dict_name = f"{metric}_top_n_df"
        top_n_df = new_lineage_df_req_iq_cluster(df, metric, req_iq_cluster_list_x, unique_id, top_n, sort_column, ascending_equals, use_config_top_n)
        concat_top_n_df[dict_name] = top_n_df

    return pd.concat(concat_top_n_df)



# ---------- SURVIVAL_CURVE_MATRIX
@st.cache_data
def new_lineage_df_survival(df, metrics):

    concat_survival_df = {}

    for metric in metrics:

        dict_name = f"{metric}_survival_df"

        survival_df = df[df["lineage_target_column"]==metric]

        survival_df = efunc.new_lineage_df_group_by_cumulative_count_running_percent(
            survival_df,
            metric,
            "lineage_unique_id"
            )

        survival_df = efunc.classify_lineage_df_survival_quarter_bins(survival_df, "percent", metric)

        survival_df = pd.pivot_table(
            survival_df,
            values=metric,
            index=["lineage_unique_id","lineage_target_column"],
            columns=["bin_survival"],
            aggfunc="max")

        concat_survival_df[dict_name] = survival_df

    return pd.concat(concat_survival_df)

# ---------- add_to_df

def add_positive_z_scores_to_df(df, target_column):

    df[f"{target_column}_positive_only"] = np.abs(df[target_column])

    return df

def add_schema_config_floor_ceiling_and_goal_to_df(df, schema, metric_config, target_column):
    
    df["schema_config_goal_floor"] = df[target_column].map(
        {metric: schema["columns"][metric]["floor"] for metric in metric_config}
    )
    
    df["schema_config_goal_ceiling"] = df[target_column].map(
        {metric: schema["columns"][metric]["ceiling"] for metric in metric_config}
    )

    df["schema_config_goal_range"] = df["schema_config_goal_ceiling"] - df["schema_config_goal_floor"]

    df["schema_config_goal_mean"] = (df["schema_config_goal_floor"] + df["schema_config_goal_ceiling"]) / 2

    df["schema_config_goal_sd"] = df["schema_config_goal_range"] / 4

    return df

def add_schema_config_goal_met_to_df(df, target_metric_value):
    """
    Adds schema_config_goal_met: whether value is within floor/ceiling range.
    """
    df["schema_config_goal_met"] = (df[target_metric_value] >= df["schema_config_goal_floor"]) & (df[target_metric_value] <= df["schema_config_goal_ceiling"])

    return df

def add_schema_config_goal_direction_label_to_df(df, z_score_column, direction_column):
    """
    Adds schema_config_goal_direction_label: correct_direction, wrong_direction, or no_direction.
    - direction=0 -> no_direction
    - direction and z_score same sign -> correct_direction
    - direction and z_score opposite sign -> wrong_direction
    """
    df["schema_config_goal_direction_label"] = np.select(
        [
            df[direction_column] == 0,
            ((df[direction_column] >= 0) & (df[z_score_column] >= 0)) | ((df[direction_column] < 0) & (df[z_score_column] < 0))
        ],
        ["no_direction", "correct_direction"],
        default="wrong_direction"
    )

    return df

def add_schema_config_direction_to_df(df, schema, metric_config, target_column):
    
    df["schema_config_direction"] = df[target_column].map(
    {metric: schema["columns"][metric]["direction"] for metric in metric_config}
    )

    return df

def add_schema_config_z_score_thresholds(df, schema, target_column):

    metrics = df[target_column].unique()
    
    df["schema_config_z_score_normal"] = df[target_column].map(
        {metric: schema["columns"][metric]["z_score_normal"] for metric in metrics}
    )
    df["schema_config_z_score_watch"] = df[target_column].map(
        {metric: schema["columns"][metric]["z_score_watch"] for metric in metrics}
    )
    df["schema_config_z_score_monitor"] = df[target_column].map(
        {metric: schema["columns"][metric]["z_score_monitor"] for metric in metrics}
    )
    df["schema_config_z_score_inspect"] = df[target_column].map(
        {metric: schema["columns"][metric]["z_score_inspect"] for metric in metrics}
    )
    
    return df

def add_bin_z_score(df, target_column):

    z = df[target_column]

    df[f"bin_{target_column}"] = np.select(
        [
            z <= df["schema_config_z_score_normal"],
            z <= df["schema_config_z_score_watch"],
            z <= df["schema_config_z_score_monitor"]
        ],
        [0, 1, 2],
        default=3
    )

    return df

def add_bin_z_score_label(df, target_column):

    bin_col = f"bin_{target_column}"

    df[f"bin_{target_column}_label"] = df[bin_col].map({
        0: "normal",
        1: "watch",
        2: "monitor",
        3: "inspect"
    })

    return df

def add_bin_z_score_signed(df, bin_column, direction_label_column):
    """
    Creates signed bin for NPS-like promoter score.
    - wrong_direction: negative bin (-3, -2, -1, 0)
    - correct_direction: positive bin (0, 1, 2, 3)
    - no_direction: excluded (NaN)

    Net promoter score = count(3, 2) - count(-3, -2)
    """
    df[f"{bin_column}_signed"] = np.select(
        [
            df[direction_label_column] == "no_direction",
            df[direction_label_column] == "wrong_direction",
            df[direction_label_column] == "correct_direction"
        ],
        [
            np.nan,
            -df[bin_column],
            df[bin_column]
        ],
        default=np.nan
    )

    return df

def add_signal_label_to_df(df, bin_label_column, direction_label_column):
    """
    Adds signal_label: three-category classification for decision analytics.

    - best_direction: correct_direction + inspect (strongly outperforming peers)
    - wrong_direction: wrong_direction + (inspect OR monitor) (strongly underperforming peers)
    - rest_direction: everything else (within expected range / noise)

    Note: best_direction is relative to peers, not absolute. A "best_direction"
    requisition could still have poor absolute metrics (e.g., 300 day time_to_fill).
    """
    df["signal_label"] = np.select(
        [
            (df[direction_label_column] == "correct_direction") & (df[bin_label_column] == "inspect"),
            (df[direction_label_column] == "wrong_direction") & (df[bin_label_column].isin(["inspect", "monitor"]))
        ],
        ["best_direction", "wrong_direction"],
        default="rest_direction"
    )

    return df

def add_lineage_benchmark_level_to_df(df, unique_id):
    """
    Flag hierarchical benchmark levels for layered comparison.

    - primary_level: most granular (highest lineage_column_level)
    - secondary_level: one level less granular than primary
    - all_other_levels: everything less granular than secondary
    - no_other_level: when only one level exists (no comparison possible)

    Enables comparisons:
    - primary vs secondary: is the immediate parent context different?
    - primary vs all_other_levels: how does granular compare to broad?
    """
    group_cols = [unique_id, "lineage_cluster_group"]

    # Get max and second max levels per group
    max_level = df.groupby(group_cols)["lineage_column_level"].transform("max")

    # Get second highest level (max of values less than max)
    def get_second_max(group):
        levels = group["lineage_column_level"].unique()
        levels_below_max = levels[levels < group["lineage_column_level"].max()]
        if len(levels_below_max) > 0:
            return levels_below_max.max()
        return np.nan

    second_max_level = df.groupby(group_cols).apply(get_second_max).reset_index()
    second_max_level.columns = [*group_cols, "second_max_level"]
    df = df.merge(second_max_level, on=group_cols, how="left")

    # Count distinct levels per group
    level_count = df.groupby(group_cols)["lineage_column_level"].transform("nunique")

    df["lineage_benchmark"] = np.select(
        [
            df["lineage_column_level"] == max_level,
            (level_count == 1),
            (level_count == 2) & (df["lineage_column_level"] != max_level),
            df["lineage_column_level"] == df["second_max_level"],
        ],
        [
            "primary_level",
            "no_other_level",
            "secondary_level",
            "secondary_level",
        ],
        default="all_other_levels"
    )

    # Clean up temp column
    df = df.drop(columns=["second_max_level"])

    return df


# ------------------------------ STEP FUNCTIONS (called by orchestrators)

def step_1_build_observation_df(observations_df, global_metrics_selected, schema):
    """Step 1: Build observation DataFrame (one row per unique_id + metric) with schema config, z-scores, bins, and signals."""

    ##### INITIAL DATAFRAME
    metric_config_observations_df = new_metric_target_column_df(observations_df, global_metrics_selected)

    ##### ADD SCHEMA CONFIG COLUMNS
    metric_config_observations_df = add_schema_config_z_score_thresholds(metric_config_observations_df, schema, "target_column")
    metric_config_observations_df = add_schema_config_floor_ceiling_and_goal_to_df(metric_config_observations_df, schema, global_metrics_selected, "target_column")
    metric_config_observations_df = add_schema_config_direction_to_df(metric_config_observations_df, schema, global_metrics_selected, "target_column")

    ##### ADD Z_SCORE COLUMNS
    metric_config_observations_df = efunc.add_z_score_to_df(metric_config_observations_df, "target_column_value", "schema_config_goal_mean", "schema_config_goal_sd", "schema_config_goal_z_score")
    metric_config_observations_df = add_positive_z_scores_to_df(metric_config_observations_df, "schema_config_goal_z_score")

    metric_config_observations_df = add_bin_z_score(metric_config_observations_df, "schema_config_goal_z_score_positive_only")
    metric_config_observations_df = add_bin_z_score_label(metric_config_observations_df, "schema_config_goal_z_score_positive_only")

    ##### ADD SCHEMA CONFIG SIGNALS
    metric_config_observations_df = add_schema_config_goal_met_to_df(metric_config_observations_df, "target_column_value")
    metric_config_observations_df = add_schema_config_goal_direction_label_to_df(metric_config_observations_df, "schema_config_goal_z_score", "schema_config_direction")

    return metric_config_observations_df


def step_2_build_sample_population_dfs(population_df, global_metrics_selected, cluster_list_of_lists, unique_id, schema, sort_by_for_top_n):
    """Step 2: Build sample population DataFrames. Returns (mean_sd_df, survival_df) grouped by lineage_unique_id."""

    ##### INITIAL DATAFRAME
    sample_population_df = new_lineage_df_top_n(
        population_df,
        global_metrics_selected,
        cluster_list_of_lists,
        unique_id,
        schema,
        sort_by_for_top_n,
        False
    )

    ##### ADD Z_SCORE COLUMNS
    sample_population_df = efunc.new_mean_sd_df(sample_population_df, "lineage_metric_value", ["lineage_unique_id", "lineage_target_column"])
    sample_population_df = efunc.add_z_score_to_df(sample_population_df, "lineage_metric_value")
    sample_population_df = sample_population_df.rename(columns={"mean": "lineage_unique_id_mean", "sd": "lineage_unique_id_sd", "z_score": "sample_z_score"})

    ##### GROUPBY LINEAGE_UNIQUE_ID & CREATE MEAN/SD DF
    sample_population_groupby_mean_sd_df = sample_population_df.groupby(["lineage_unique_id", "lineage_target_column"])[["lineage_unique_id_mean", "lineage_unique_id_sd"]].mean().reset_index()

    ##### GROUPBY LINEAGE_UNIQUE_ID & CREATE SURVIVAL DF
    sample_population_groupby_survival_df = new_lineage_df_survival(sample_population_df, global_metrics_selected)
    sample_population_groupby_survival_df = sample_population_groupby_survival_df.rename(columns={
        "bin_survival_25th": "lineage_unique_id_survival_25th",
        "bin_survival_50th": "lineage_unique_id_survival_50th",
        "bin_survival_75th": "lineage_unique_id_survival_75th",
        "bin_survival_100th": "lineage_unique_id_survival_100th"
    })

    return sample_population_groupby_mean_sd_df, sample_population_groupby_survival_df


def step_3_build_observations_with_all_lineage_ids(observations_df, global_metrics_selected, cluster_list_of_lists, unique_id, schema):
    """Step 3: Build observations with all possible lineage_unique_id's (to then add sample population group metrics later)."""

    ##### INITIAL DATAFRAME
    observations_with_all_lineage_ids_df = new_lineage_df_top_n(
        observations_df,
        global_metrics_selected,
        cluster_list_of_lists,
        unique_id,
        schema,
        unique_id,
        False,
        top_n=None,
        use_config_top_n=False
    )
    observations_with_all_lineage_ids_df = add_schema_config_direction_to_df(observations_with_all_lineage_ids_df, schema, global_metrics_selected, "lineage_target_column")
    observations_with_all_lineage_ids_df = add_schema_config_z_score_thresholds(observations_with_all_lineage_ids_df, schema, "lineage_target_column")

    # drop lineage_top_n since we passed None and False
    observations_with_all_lineage_ids_df = observations_with_all_lineage_ids_df.drop(columns=['lineage_top_n'])

    return observations_with_all_lineage_ids_df


def step_4_add_z_scores_from_sample_population(observations_with_all_lineage_ids_df, sample_population_mean_sd_df, unique_id):
    """Step 4: Add z-scores to observations based on sample population groups."""

    ##### MERGE WITH SAMPLE POPULATION MEAN/SD
    df = pd.merge(observations_with_all_lineage_ids_df, sample_population_mean_sd_df, how="right", on=["lineage_unique_id", "lineage_target_column"])

    ##### ADD COLUMNS
    df = efunc.add_z_score_to_df(df, "lineage_metric_value", "lineage_unique_id_mean", "lineage_unique_id_sd", "lineage_unique_id_z_score")
    df = add_positive_z_scores_to_df(df, "lineage_unique_id_z_score")
    df = add_bin_z_score(df, "lineage_unique_id_z_score_positive_only")
    df = add_bin_z_score_label(df, "lineage_unique_id_z_score_positive_only")
    df = add_lineage_benchmark_level_to_df(df, unique_id)
    df = add_schema_config_goal_direction_label_to_df(df, "lineage_unique_id_z_score", "schema_config_direction")
    df = add_bin_z_score_signed(df, "bin_lineage_unique_id_z_score_positive_only", "schema_config_goal_direction_label")
    df = add_signal_label_to_df(df, "bin_lineage_unique_id_z_score_positive_only_label", "schema_config_goal_direction_label")

    columns_to_keep = (
        [unique_id]
        + efunc.get_lineage_column_names(df)
        + efunc.get_columns_starting_with(df, "bin_")
        + efunc.get_columns_starting_with(df, "schema_config_")
        + efunc.get_columns_starting_with(df, "signal_")
    )

    df = efunc.keep_columns_only(df, columns_to_keep)

    return df


def step_5_add_survival_from_sample_population(observations_with_all_lineage_ids_df, sample_population_survival_df, unique_id):
    """Step 5: Add survival percentiles to observations based on sample population groups."""

    df = pd.merge(observations_with_all_lineage_ids_df, sample_population_survival_df, how="right", on=["lineage_unique_id", "lineage_target_column"])

    columns_to_keep = (
        [unique_id]
        + efunc.get_lineage_column_names(df)
    )

    df = efunc.keep_columns_only(df, columns_to_keep)

    return df