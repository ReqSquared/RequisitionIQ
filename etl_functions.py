# etl_functions.py

import pandas as pd
import numpy as np

# lineage_* standard: metadata columns tracking data provenance through ETL pipeline
# Format: lineage_[attribute] = [transformation context]
def add_lineage_columns(df, 
                        lineage_cluster_name,  
                        lineage_cluster_group, 
                        lineage_columns, 
                        lineage_column_level,
                        sort_column=None, 
                        top_n=None,
                        lineage_target_column=None) -> pd.DataFrame:
    
    if df.empty:
        df["lineage_unique_id"] = None

    else:
        df["lineage_cluster_name"] = lineage_cluster_name
        df["lineage_unique_id"] = lineage_cluster_group + "_" + df[lineage_columns].astype(str).agg("_".join, axis=1)
        df["lineage_cluster_group"] = lineage_cluster_group
        df["lineage_cluster_group_level"] = lineage_column_level
        df["lineage_column_level"] = len(lineage_columns)
        df["lineage_target_column"] = lineage_target_column
        df["lineage_metric_value"] = df[lineage_target_column]
        df["lineage_dict_name"] = f"{lineage_cluster_group}_{lineage_columns}"   
     
    if sort_column is not None:
        df["lineage_sort_column"] = sort_column
    else:
        df["lineage_sort_column"] = None        

    if top_n is not None:
        df["lineage_top_n"] = top_n
    else:
        df["lineage_top_n"] = None

    df["lineage_timestamp"] = pd.Timestamp.now()

    return df


def get_lineage_column_names(df):

    lineage_column_names = [column for column in df.columns if column.startswith("lineage_")]

    return lineage_column_names

def get_columns_starting_with(df, prefix):

    return [col for col in df.columns if col.startswith(prefix)]

def drop_lineage_columns_except(df, keep_columns):
    """
    Drops all lineage_* columns except those specified in keep_columns.
    Explicit function for lineage column management - use with caution.

    Args:
        df: DataFrame with lineage_* columns
        keep_columns: List of lineage column names to keep

    Returns:
        DataFrame with only specified lineage columns retained
    """
    all_lineage_cols = get_lineage_column_names(df)
    cols_to_drop = [col for col in all_lineage_cols if col not in keep_columns]

    return df.drop(columns=cols_to_drop, errors='ignore')

def keep_columns_only(df, keep_columns):
    """
    Keeps only specified columns. Pass everything you want to keep.

    Args:
        df: DataFrame to filter
        keep_columns: List of column names to keep

    Returns:
        DataFrame with only specified columns
    """
    cols_to_keep = [col for col in keep_columns if col in df.columns]

    return df[cols_to_keep]

def new_group_by_top_n_columns_df(
        df: pd.DataFrame,
        on_columns: list[str],
        sort_column: str,
        top_n: int,
        ascending_equals: bool,
        unique_id: str

) -> pd.DataFrame:
    
    # Filter to groups meeting top_n threshold, then sample top_n records per group
    df = df.sort_values(by=sort_column, ascending=ascending_equals)

    df["count"] = df.groupby(on_columns)[unique_id].transform("count")

    if top_n is not None:

        df = df[df["count"] >= top_n]

        df["cumulative_count"] = df.groupby(on_columns)[unique_id].transform("cumcount")

        df = df[df["cumulative_count"] < top_n]

    return df

def new_mean_sd_df(df, target_column, lineage_unique_id):
    # Adds mean/sd columns to original DataFrame (preserves row-level data)

    if 'mean' not in df.columns:
    
        df["mean"] = df.groupby(lineage_unique_id)[target_column].transform("mean")

    if 'sd' not in df.columns:

        df["sd"] = df.groupby(lineage_unique_id)[target_column].transform("std")

    return df

def add_z_score_to_df(df, target_column, mean_name="mean", sd_name="sd", column_name="z_score"):
    # Standardizes target_column relative to group mean/sd

    df[column_name] = np.where(df[sd_name] == 0, 0, (df[target_column] - df[mean_name]) / df[sd_name])

    return df

def new_lineage_df_group_by_cumulative_count_running_percent(df, target_column, lineage_unique_id):
    # Builds distribution across target_column values with cumulative percent within each group

    # Get top_n per group: use lineage_top_n if available, otherwise use actual count
    if df["lineage_top_n"].notna().any():
        top_n_map = df.groupby(lineage_unique_id)["lineage_top_n"].first()
    else:
        top_n_map = df.groupby(lineage_unique_id)[target_column].count()

    df = df.groupby(lineage_unique_id)[target_column].value_counts().reset_index(name="count")
    df = df.sort_values(by=[lineage_unique_id,target_column], ascending=[True,True]).reset_index(drop=True)
    df["cum_count"] = df.groupby([lineage_unique_id], observed=True)["count"].cumsum()

    # Map top_n back and calculate percent using each group's own top_n
    df["top_n"] = df[lineage_unique_id].map(top_n_map)
    df["percent"] = df["cum_count"] / df["top_n"]

    return df

def classify_lineage_df_survival_quarter_bins(df, percent_column, lineage_target_column):

    bin_1 = f"bin_survival_25th"
    bin_2 = f"bin_survival_50th"
    bin_3 = f"bin_survival_75th"
    bin_4 = f"bin_survival_100th"

    df["bin_survival"] = pd.cut(df[percent_column], bins=[0, 0.25, 0.5, 0.75, 1], 
                                labels=[f"{bin_1}",
                                        f"{bin_2}",
                                        f"{bin_3}", 
                                        f"{bin_4}"]
                                )

    df["lineage_target_column"] = lineage_target_column

    return df