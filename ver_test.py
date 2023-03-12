from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler
from algebra import API
from qbe_module.column_selection import ColumnSelection
from qbe_module.query_by_example import ExampleColumn, QueryByExample
from qbe_module.materializer import Materializer
from tqdm import tqdm

"""
path to store the aurum graph index
"""
graph_path = '/home/cc/opendata_large_graph/'
# graph_path = '/home/cc/chicago_open_data_graph/'

"""
path to store the raw data
"""
data_path = '/home/cc/opendata_cleaned/'
# data_path = '/home/cc/chicago_open_data/'

store_client = StoreHandler()
network = fieldnetwork.deserialize_network(graph_path)
aurum_api = API(network=network, store_client=store_client)

# QBE interface
qbe = QueryByExample(aurum_api)

"""
Specify an example query
"""
example_columns = [ExampleColumn(attr='', examples=["Ogden International High School", "University of Chicago - Woodlawn"]),
                   ExampleColumn(attr='', examples=["International Baccalaureate (IB)", "General Education"]),
                    ExampleColumn(attr='', examples=["Level 1", "Level 2+"])]

"""
Find candidate columns
"""
candidate_list = qbe.find_candidate_columns(example_columns, cluster_prune=True)
for i, candidate in enumerate(candidate_list):
    print('column {}: found {} candidate columns'.format(format(i), len(candidate)))

"""
Find join graphs
"""
join_graphs = qbe.find_join_graphs_between_candidate_columns(candidate_list, order_chain_only=True)
print("found {} join graphs".format(len(join_graphs)))
for i, join_graph in enumerate(join_graphs[:10]):
    print("----join graph {}----".format(i))
    join_graph.display()


"""
Materialize join graphs
"""
materializer = Materializer(data_path, 200)

result = []

j = 0
for join_graph in tqdm(join_graphs):
    """
    a join graph can produce multiple views because different columns are projected
    """
    df_list = materializer.materialize_join_graph(join_graph)
    for df in df_list:
        if len(df) != 0:
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
            df.to_csv(f"./test_views2/view{j}.csv", index=False)

print("valid views", j)