from ver_demo import Demo
from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler
from algebra import API
from qbe_module.column_selection import ColumnSelection
from qbe_module.query_by_example import ExampleColumn, QueryByExample
from qbe_module.materializer import Materializer

graph_path = '/home/cc/chicago_open_data_graph/'
store_client = StoreHandler()
network = fieldnetwork.deserialize_network(graph_path)
aurum_api = API(network=network, store_client=store_client)

# example_columns = [ExampleColumn(attr='', examples=['60615']),
# ExampleColumn(attr='', examples=['Kenwood Academy']),
# ExampleColumn(attr='', examples=['Blackstone'])]

example_columns = [ExampleColumn(attr='', examples=[60615, 60637]),
                   ExampleColumn(attr='', examples=['Kenwood High School', 'Hyde Park High School']),
                #    ExampleColumn(attr='', examples=['Kenwood HS', 'Hyde Park HS']),
                   ExampleColumn(attr='', examples=['Blackstone', 'Coleman'])]

qbe = QueryByExample(aurum_api)
candidate_list = qbe.find_candidate_columns(example_columns)
# for candidate in candidate_list:
#     print('00000')
#     for x in candidate:
#         print(x.tbl_name, x.attr_name)

join_graphs = qbe.find_join_graphs_between_candidate_columns(candidate_list)
for i, join_graph in enumerate(join_graphs[:10]):
    print("----join graph {}----".format(i))
    join_graph.to_str()

materializer = Materializer('/home/cc/chicago_open_data/', 200)
join_graph_9 = join_graphs[0]
df_list = materializer.materialize_join_graph(join_graph_9)
for df in df_list:
    print(df.head())