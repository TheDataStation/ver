import config
from dindex_store.discovery_index import load_dindex
from aurum_api.algebra import AurumAPI
from qbe_module.query_by_example import ExampleColumn, QueryByExample
from qbe_module.materializer import Materializer
from tqdm import tqdm
import os
import json

cnf = {setting: getattr(config, setting) for setting in dir(config)
        if setting.islower() and len(setting) > 2 and setting[:2] != "__"}

dindex = load_dindex(cnf)
print("Loading DIndex...OK")

api = AurumAPI(dindex)
print("created aurum api")

# QBE interface
qbe = QueryByExample(api)

"""
Specify an example query
"""
example_columns = [ExampleColumn(attr='school_name', examples=["Ogden International High School", "University of Chicago - Woodlawn"]),
                   ExampleColumn(attr='school type', examples=["Charter", "Neighborhood"]),
                    ExampleColumn(attr='level', examples=["Level 1", "Level 2+"])
                ]

"""
Find candidate columns
"""
candidate_list = qbe.find_candidate_columns(example_columns, cluster_prune=True)

"""
Display candidate columns (for debugging purpose)
"""
for i, candidate in enumerate(candidate_list):
    print('column {}: found {} candidate columns'.format(format(i), len(candidate)))
    for col in candidate:
        print(col.to_str())

cand_groups, tbl_cols = qbe.find_candidate_groups(candidate_list)

"""
Find join graphs
"""
join_graphs = qbe.find_join_graphs_for_cand_groups(cand_groups)
print(f"number of join graphs: {len(join_graphs)}")
"""
Display join graphs (for debugging purpose)
"""
for i, join_graph in enumerate(join_graphs):
    print(f"----join graph {i}----")
    join_graph.display()


"""
Materialize join graphs
"""
data_path = './demo_dataset/' # path where the raw data is stored
output_path = './output/' # path to store the output views
max_num_views = 200 # how many views you want to materialize
sep = ',' # csv separator

if not os.path.exists(output_path):
    os.makedirs(output_path)
materializer = Materializer(data_path, tbl_cols, 200, sep)

result = []

j = 0
for join_graph in tqdm(join_graphs):
    """
    a join graph can produce multiple views because different columns are projected
    """
    df_list = materializer.materialize_join_graph(join_graph)
    for df in df_list:
        if len(df) != 0:
            metadata = {}
            metadata["join_graph"] = join_graph.to_str()
            metadata["columns_proj"] = list(df.columns)
            with open(f"./{output_path}/view{j}.json", "w") as outfile:
                json.dump(metadata, outfile)
            j += 1
            print("non empty view", j)
            new_cols = []
            k = 1
            for col in df.columns:
                new_col = col.split(".")[-1]
                if new_col in new_cols:
                    new_col += str(k)
                    k += 1
                new_cols.append(new_col)
            df.columns = new_cols
            df.to_csv(f"./{output_path}/view{j}.csv", index=False)
           
    if j >= max_num_views:
        break
           
print("valid views", j)