"""
This is the main config file for Ver.
All properties of this config file should be written without using _ so they are easy to "get"
"""
from pathlib import Path

# Overlap parameters
join_overlap_th = 0.4

# Schema similarity parameters
max_distance_schema_similarity = 10

# Serde parameters
serdepath = "./data"
signcollectionfile = "sigcolfile.pickle"
graphfile = "graphfile.pickle"
graphcachedfile = "graphcachedfile.pickle"
datasetcolsfile = "datasetcols.pickle"
simrankfile = "simrankfile.pickle"
jgraphfile = "jgraphfile.pickle"

# DB connection
db_host = 'localhost'
db_port = '9200'

###########
## minhash
###########
k = 512

###########
## DoD
###########
separator = ','
join_chunksize = 1000
memory_limit_join_processing = 0.6  # 60% of total memory

####################################
## New, after refactoring, configs


ver_base_path = Path(__file__).parent

##########
## Input Data
##########

input_data_type = "json"
text_csv_delimiter = ','
unified_profile_schema_name = "profile_schema.yml"
profile_schema_name = "dindex_builder/profile_index_schema_duckdb.txt"
graph_schema_name = "dindex_builder/graph_index_schema_kuzu.txt"
fts_schema_name = "dindex_builder/fts_index_schema_duckdb.txt"

##########
## DIndex
##########

profile_index = "duckdb"
content_index = "simpleminhash"
fts_index = "duckdb"
graph_index = "kuzu"

# Profile index config
profile_table_name = "profiles"
profile_duckdb_database_name = "profiles"

# FTS index configs
fts_data_table_name = "fts_data"
fts_index_column = "data"
fts_duckdb_database_name = "profiles"  # naming it the same as profile_duckdb_database_name places all in one instance

# content index configs
minhash_lsh_threshold = 0.7
minhash_lsh_num_perm = 512

# graph index configs
graph_kuzu_database_name = "graph_index"
