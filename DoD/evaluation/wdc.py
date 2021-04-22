import json
import server_config as config
from DoD import column_infer
from DoD.utils import FilterType
from DoD.view_search_pruning import ViewSearchPruning, start
from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler
from DoD import data_processing_utils as dpu
from chembl import evaluate_view_search
import pandas as pd
import os
import errno

model_path = config.Wdc.path_model
sep = config.Wdc.separator
base_outpath = config.Wdc.output_path

store_client = StoreHandler()
network = fieldnetwork.deserialize_network(model_path)
columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)
viewSearch = ViewSearchPruning(network=network, store_client=store_client, csv_separator=sep)

def query_1():
    attrs = ["", "", ""]
    values = [["Shoot the Moon", "Faith Dunlap", "National Society of Film Critics Awards"],
              ["Annie Hall", "Annie Hall", "National Society of Film Critics Awards"],
              ["Looking for Mr. Goodbar", "Theresa", "New York Film Critics Circle Award"]]
    gt_cols = [("1438042988061.16_20150728002308-00126-ip-10-236-191-2_880924977_1.json.csv", "Title"), ("1438042988061.16_20150728002308-00126-ip-10-236-191-2_880924977_1.json.csv", "Role"), ("1438042988061.16_20150728002308-00126-ip-10-236-191-2_880924977_3.json.csv", "Award")]
    gt_path = "1438042988061.16_20150728002308-00126-ip-10-236-191-2_880924977_1.json.csv-Title JOIN 1438042988061.16_20150728002308-00126-ip-10-236-191-2_880924977_3.json.csv-Nominated work"
    return attrs, values, gt_cols, gt_path

def query_2():
    attrs = ["", ""]
    values = [["CT", "Connecticut"], ["GA", "Georgia"], ["VA", "Virginia"]]
    gt_cols = [("1438042988061.16_20150728002308-00307-ip-10-236-191-2_27730064_4.json.csv", "State"), ("1438042988061.16_20150728002308-00311-ip-10-236-191-2_31120164_1.json.csv", "State")]
    gt_path = "1438042988061.16_20150728002308-00307-ip-10-236-191-2_27730064_4.json.csv-Newspaper Title JOIN 1438042988061.16_20150728002308-00311-ip-10-236-191-2_31120164_1.json.csv-Title"
    return attrs, values, gt_cols, gt_path

def query_3():
    attrs = ["", ""]
    values = [["Florida Theatre", "Tampa Tribune"], ["Pease Auditorium", "Jackson Citizen"],
              ["The Capitol Theatre", "New-York Gazette"]]
    gt_cols = [("1438042988061.16_20150728002308-00202-ip-10-236-191-2_882777968_4.json.csv", "venue"), ("1438042988061.16_20150728002308-00307-ip-10-236-191-2_27730064_4.json.csv", "Newspaper Title")]
    gt_path = "1438042988061.16_20150728002308-00202-ip-10-236-191-2_882777968_4.json.csv-State JOIN 1438042988061.16_20150728002308-00307-ip-10-236-191-2_27730064_4.json.csv-State"
    return attrs, values, gt_cols, gt_path

def query_4():
    pass
if __name__ == '__main__':
    attrs, values, gt_cols, gt_path = query_2()
    found = dict()
    found["method1"] = 0
    found["method2"] = 0
    found["method3"] = 0
    name = "query_2/"
    candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values)
    # if evaluate_view_search(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 1, gt_cols, gt_path, 10000,
    #                      base_outpath + name + "pipeline1/"):
    #     found["method1"] += 1
    if evaluate_view_search(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 2, gt_cols, gt_path, 10000,
                         base_outpath + name + "pipeline2/"):
        found["method2"] += 1
    # if evaluate_view_search(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 3, gt_cols, gt_path, 10000,
    #                      base_outpath + name + "pipeline3/"):
    #     found["method3"] += 1
    f = open(base_outpath + name + "/hit.txt", "w")
    f.write(str(found))

