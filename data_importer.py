# data_importer.py

import streamlit as st
import pandas as pd
import yaml

# ==============================================================
# MAIN
# ==============================================================

def main():
    get_base_data()
    get_configs()
    get_req_iq_cluster_columns()

if __name__ == "__main__":
    main()

# ==============================================================
# GET_BASE_DATA
# ==============================================================

@st.cache_data
def get_base_data():
    raw_open_reqs = pd.read_csv("data/open_reqs.csv")
    raw_filled_reqs = pd.read_csv("data/filled_reqs.csv")
    raw_federal_hiring = pd.read_csv("data/federal_hiring.csv", encoding="cp1252")

    schema_federal_hiring, schema_open_reqs, schema_filled_reqs, metric_config = get_configs()

    etl_open_reqs, etl_filled_reqs, etl_federal_hiring = apply_data_types_to_data(
        raw_open_reqs, schema_open_reqs,
        raw_filled_reqs, schema_filled_reqs,
        raw_federal_hiring, schema_federal_hiring
    )

    return {
        "open_reqs": etl_open_reqs,
        "filled_reqs": etl_filled_reqs,
        "federal_hiring": etl_federal_hiring,
        "schema_open_reqs": schema_open_reqs,
        "schema_filled_reqs": schema_filled_reqs,
        "schema_federal_hiring": schema_federal_hiring,
        "metric_config": metric_config
    }

# ==============================================================
# ETL_RAW_REQ_DATA
# ==============================================================

def apply_data_types_to_data(raw_open_reqs, schema_open_reqs, raw_filled_reqs, schema_filled_reqs, raw_federal_hiring, schema_federal_hiring):

    for schema_level_1, schema_level_2 in schema_open_reqs["columns"].items():
        raw_open_reqs[schema_level_1] = raw_open_reqs[schema_level_1].astype(schema_level_2["dtype"])

    for schema_level_1, schema_level_2 in schema_filled_reqs["columns"].items():
        raw_filled_reqs[schema_level_1] = raw_filled_reqs[schema_level_1].astype(schema_level_2["dtype"])

    for schema_level_1, schema_level_2 in schema_federal_hiring["columns"].items():
        raw_federal_hiring[schema_level_1] = raw_federal_hiring[schema_level_1].astype(schema_level_2["dtype"])

    return raw_open_reqs, raw_filled_reqs, raw_federal_hiring

# ==============================================================
# GET_CONFIGS
# ==============================================================

def get_configs():
    with open("config/schema_open_reqs.yml", 'r') as f:
        schema_open_reqs = yaml.safe_load(f)

    with open("config/schema_filled_reqs.yml", 'r') as f:
        schema_filled_reqs = yaml.safe_load(f)

    with open("config/schema_federal_hiring.yml", 'r') as f:
        schema_federal_hiring = yaml.safe_load(f)

    with open("config/metric_config.yml", 'r') as f:
        metric_config = yaml.safe_load(f)

    return schema_federal_hiring, schema_open_reqs, schema_filled_reqs, metric_config

# ==============================================================
# ---------------------- REQ_IQ_CLUSTER_1 ----------------------
# ==============================================================

from collections import namedtuple

# NamedTuple = a regular tuple, but each position has a name
# You can still do config[0], config[1] etc. — it's just a tuple
# But now you can also do config.cluster_name, config.cluster_group etc.
ClusterConfig = namedtuple("ClusterConfig", [
    "cluster_name",     # e.g. "req_iq_cluster_1" — which YAML block this came from
    "cluster_group",    # e.g. "recruiter_context" — which grouping profile
    "columns",          # e.g. ["recruiter", "country"] — the groupby columns
    "column_level",     # e.g. 2 — position in the hierarchy (from enumerate)
    "config_top_n"      # e.g. 42 — per-profile top_n from YAML, or None
])

def get_req_iq_cluster_columns(req_iq_cluster_1, req_iq_cluster_1_name):
    """
    Supports two YAML formats:
    1. Simple list: recruiter: [["col1", "col2"], ...]
    2. Dict with top_n: recruiter_profile: {top_n: 40, lineage_columns: [[...], ...]}

    Returns tuple: (cluster_name, profile_name, columns, level, profile_top_n)
    """
    tmp_req_iq_list_of_lists = []

    for i, (req_iq_cluster_name, req_iq_clusters) in enumerate(req_iq_cluster_1):

        # Handle dict format (with top_n and lineage_columns)
        if isinstance(req_iq_clusters, dict):
            profile_top_n = req_iq_clusters.get("top_n")
            cluster_list = req_iq_clusters.get("lineage_columns", [])
        else:
            # Handle simple list format
            profile_top_n = None
            cluster_list = req_iq_clusters

        assert isinstance(cluster_list, list), f"{req_iq_cluster_name} must be a list of groupings"

        for cluster in cluster_list:
            assert isinstance(cluster, list), f"Grouping for {req_iq_cluster_name} must be a list of column names"
            tmp_req_iq_list_of_lists.append(ClusterConfig(
                cluster_name=req_iq_cluster_1_name,
                cluster_group=req_iq_cluster_name,
                columns=cluster,
                column_level=i,
                config_top_n=profile_top_n
            ))

    return tmp_req_iq_list_of_lists

# ==============================================================
# GET_DATAFRAME_CONFIG
# ==============================================================

def get_dataframe_config(config_name):
    """
    Reads config/dataframe.yml and resolves a named pipeline config
    into ready-to-use variables: observations_df, population_df, schema,
    unique_id, global_metrics_selected, cluster_list_of_lists, sort_by_for_top_n.

    Calls get_base_data() internally (cached, so CSVs only load once).
    """
    with open("config/dataframe.yml", 'r') as f:
        all_configs = yaml.safe_load(f)

    base_data = get_base_data()
    config = all_configs[config_name]
    metric_config = base_data["metric_config"]

    ##### DATAFRAMES (observation & population)
    data = base_data[config["data_source"]]

    observations_config = config.get("observations")
    if observations_config:
        observations_df = data[data[observations_config["column"]] == observations_config["equals"]]
    else:
        observations_df = data

    population_config = config.get("population")
    if population_config:
        population_df = data[data[population_config["column"]] != population_config["not_equals"]]
    else:
        population_df = None

    ##### SCHEMA_SELECTION
    schema = base_data[config["schema"]]
    unique_id = config["unique_id"]

    ##### METRIC_CONFIG
    global_metrics_selected = metric_config["global_metrics"][config["global_metrics"]]

    ##### CLUSTER CONFIG
    cluster_config_name = config["cluster_config"]
    cluster_config = metric_config[cluster_config_name].items()
    cluster_list_of_lists = get_req_iq_cluster_columns(cluster_config, cluster_config_name)

    ##### ADDITIONAL CONFIG
    sort_by_for_top_n = config.get("sort_by_for_top_n")

    return {
        "observations_df": observations_df,
        "population_df": population_df,
        "schema": schema,
        "unique_id": unique_id,
        "global_metrics_selected": global_metrics_selected,
        "cluster_list_of_lists": cluster_list_of_lists,
        "sort_by_for_top_n": sort_by_for_top_n
    }

