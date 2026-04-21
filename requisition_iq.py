# requisition_iq.py

import streamlit as st

import data_importer as dimp
import iq



# ------------------------------ STREAMLIT
st.set_page_config(
layout="wide", # Enables wide mode
page_title="RequisitionIQ", # Sets the browser tab title
page_icon="🎯" # Sets a favicon or emoji
)

# ------------------------------ LOAD CONFIG
config = dimp.get_dataframe_config("filled_req_iq_cluster_1_df")

observations_df = config["observations_df"]
population_df = config["population_df"]
schema = config["schema"]
unique_id = config["unique_id"]
global_metrics_selected = config["global_metrics_selected"]
cluster_list_of_lists = config["cluster_list_of_lists"]
sort_by_for_top_n = config["sort_by_for_top_n"]

# ------------------------------ #1: BUILD OBSERVATION DATAFRAME
metric_config_observations_df = iq.step_1_build_observation_df(observations_df, global_metrics_selected, schema)

# ------------------------------ #2: BUILD SAMPLE POPULATION DATAFRAMES
sample_population_groupby_lineage_unique_id_mean_sd_df, sample_population_groupby_lineage_unique_id_survival_df = iq.step_2_build_sample_population_dfs(
    population_df, global_metrics_selected, cluster_list_of_lists, unique_id, schema, sort_by_for_top_n
)

# ------------------------------ #3: BUILD OBSERVATIONS WITH ALL POSSIBLE LINEAGE_UNIQUE_ID's
observations_with_all_possible_lineage_unique_ids_df = iq.step_3_build_observations_with_all_lineage_ids(
    observations_df, global_metrics_selected, cluster_list_of_lists, unique_id, schema
)

# ------------------------------ #4: ADD Z-SCORES TO OBSERVATIONS BASED ON SAMPLE POPULATION GROUPS
observations_with_sample_population_z_scores_df = iq.step_4_add_z_scores_from_sample_population(
    observations_with_all_possible_lineage_unique_ids_df, sample_population_groupby_lineage_unique_id_mean_sd_df, unique_id
)

# ------------------------------ #5: ADD SURVIVAL PERCENTILES TO OBSERVATIONS BASED ON SAMPLE POPULATION GROUPS
observations_with_sample_population_group_survival_df = iq.step_5_add_survival_from_sample_population(
    observations_with_all_possible_lineage_unique_ids_df, sample_population_groupby_lineage_unique_id_survival_df, unique_id
)

