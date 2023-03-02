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
example_columns = [ExampleColumn(attr='', examples=["Albany Park", "Austin", "Douglas"]),
                   ExampleColumn(attr='', examples=["Mayfair Commons", "Pine Central", "Lake Park LLC"]),
                   ExampleColumn(attr='', examples=["CHICAGO PRODUCE", "FOOD 4 LESS", "WALGREENS"])]

qbe = QueryByExample(aurum_api)
candidate_list = qbe.find_candidate_columns(example_columns)
for i, candidate in enumerate(candidate_list):
    print('column {}: found {} candidate columns'.format(format(i), len(candidate)))
    # for c in candidate:
    #     print(c.to_str())

join_graphs = qbe.find_join_graphs_between_candidate_columns(candidate_list)
print("found {} join graphs".format(len(join_graphs)))
# for i, join_graph in enumerate(join_graphs[:10]):
#     print("----join graph {}----".format(i))
#     join_graph.display()

materializer = Materializer('/home/cc/chicago_open_data/', 200)

result = []
i = 1
for join_graph in tqdm(join_graphs):
    print("materializing join graph", i)
    i += 1
    df_list = materializer.materialize_join_graph(join_graph)
    for df in df_list:
        # print(df.head(5))
        if len(df) != 0:
            result.append(df)
print(len(result))
# join_graph_9 = join_graphs[9]
# df_list = materializer.materialize_join_graph(join_graph_0)
for i in range(len(result)):
    result[i].to_csv(f"./test_views/view{i}.csv", index=False)