from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler
from algebra import API
from qbe_module.column_selection import ColumnSelection
from qbe_module.query_by_example import ExampleColumn, QueryByExample
from qbe_module.materializer import Materializer
from tqdm import tqdm

graph_path = '/home/cc/chicago_open_data_graph/'
store_client = StoreHandler()
network = fieldnetwork.deserialize_network(graph_path)
aurum_api = API(network=network, store_client=store_client)

# example_columns = [ExampleColumn(attr='', examples=['60615']),
# ExampleColumn(attr='', examples=['Kenwood Academy']),
# ExampleColumn(attr='', examples=['Blackstone'])]

# example_columns = [ExampleColumn(attr='', examples=[60615, 60637]),
#                    ExampleColumn(attr='', examples=['Kenwood High School', 'Hyde Park High School']),
#                 #    ExampleColumn(attr='', examples=['Kenwood HS', 'Hyde Park HS']),
#                    ExampleColumn(attr='', examples=['Blackstone', 'Coleman'])]
# example_columns = [ExampleColumn(attr='', examples=["Albany Park", "Austin", "Douglas"]),
#                    ExampleColumn(attr='', examples=["Mayfair Commons", "Pine Central", "Lake Park LLC"]),
#                    ExampleColumn(attr='', examples=["CHICAGO PRODUCE", "FOOD 4 LESS", "WALGREENS"])]
# example_columns = [ExampleColumn(attr='', examples=["Roosevelt High School", "George Washington High School"]),
#                    ExampleColumn(attr='', examples=[319900, 219291]),
#                    ExampleColumn(attr='', examples=[1927, 1956])]
# example_columns = [ExampleColumn(attr='', examples=["Vue53", "City Hyde Park"]),
#                    ExampleColumn(attr='', examples=["Commercial", "Residential"]),
#                    ExampleColumn(attr='', examples=["> 250,000 Sq Ft", "> 250,000 Sq Ft"])]
example_columns = [ExampleColumn(attr='', examples=["Ogden International High School", "University of Chicago - Woodlawn"]),
                   ExampleColumn(attr='', examples=["9-12", "6-12"]),
                   ExampleColumn(attr='', examples=["International Baccalaureate (IB)", "General Education"]),
                    ExampleColumn(attr='', examples=["Level 1", "Level 2+"])]


# long_name, grades_offered, program_group, administrator
# Ogden International High School, 9-12, International Baccalaureate (IB), Devon Herrick
# University of Chicago - Woodlawn, 6-12, General Education, Donald L Gordon


qbe = QueryByExample(aurum_api)
candidate_list = qbe.find_candidate_columns(example_columns, cluster_prune=True)
for i, candidate in enumerate(candidate_list):
    print('column {}: found {} candidate columns'.format(format(i), len(candidate)))
    # for c in candidate:
    #     print(c.to_str())

join_graphs = qbe.find_join_graphs_between_candidate_columns(candidate_list, order_chain_only=True)
print("found {} join graphs".format(len(join_graphs)))
# for i, join_graph in enumerate(join_graphs[:10]):
#     print("----join graph {}----".format(i))
#     join_graph.display()

materializer = Materializer('/home/cc/chicago_open_data/', 200)

result = []
i = 1
j = 1
for join_graph in tqdm(join_graphs):
    # print("materializing join graph", i)
    # i += 1
    df_list = materializer.materialize_join_graph(join_graph)
    for df in df_list:
        # print(df.head(5))
        print("non empty view", j)
        j += 1
        if len(df) != 0:
            result.append(df)

print(len(result))

# join_graph_9 = join_graphs[9]
# df_list = materializer.materialize_join_graph(join_graph_0)
# for i in range(len(result)):
#     df = result[i]
#     new_cols = []
#     i = 1
#     for col in df.columns:
#         new_col = col.split(".")[-1]
#         if new_col in new_cols:
#             new_col += str(i)
#             i += 1
#         new_cols.append(new_col)
#     df.columns = new_cols
#     df.to_csv(f"./test_views/view{i}.csv", index=False)
