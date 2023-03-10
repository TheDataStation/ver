# import json
# from algebra import API
# from DoD import column_infer
# from DoD.utils import FilterType
# from DoD.view_search_pruning import ViewSearchPruning
# from knowledgerepr import fieldnetwork
# from modelstore.elasticstore import StoreHandler
# from DoD import data_processing_utils as dpu
# import pandas as pd

# model_path = '/home/cc/models/nyc_data/'
# sep = ','
# base_outpath = '/home/cc/output'

# store_client = StoreHandler()
# network = fieldnetwork.deserialize_network(model_path)

# aurum_api = API(network=network, store_client=store_client)

# print("data loaded")

def search_attribute(api, name: str, limit: int):
    drs_attr = api.search_attribute(name, max_results=limit)
    return drs_attr

def search_content(api, kw:str, limit: int):
    drs_content = api.search_content(kw, max_results=limit)
    return drs_content
