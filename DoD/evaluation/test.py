from DoD import data_processing_utils as dpu
import server_config as config
from algebra import API
from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler

model_path = config.Chembl.path_model
sep = config.Chembl.separator
base_outpath = config.Chembl.output_path

store_client = StoreHandler()
network = fieldnetwork.deserialize_network(model_path)
aurum_api = API(network=network, store_client=store_client)

# df = dpu.read_column("/home/cc/chemBL/public.compound_records.csv", "compound_name")
# print(df[df["compound_name"]=="SID104219496"].index.values)
#
# res = aurum_api.search_exact_content("SID104219496", max_results=100)
# print (len(res.data))
#
df = dpu.read_column("/home/cc/chemBL/public.compound_records.csv", "compound_name")["compound_name"].values.tolist()
# print(len(df))
# print ("SID104219496" in df)
#
for (i, v) in enumerate(df[1084253:1084353]):
    res = aurum_api.search_exact_content(v, max_results=100)
    if len(res.data) == 0:
        print (i, v)
