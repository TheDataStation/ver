import server_config as config
from archived.DoD import column_infer
from archived.DoD import ViewSearchPruning
from knowledgerepr import fieldnetwork
from archived.modelstore import StoreHandler
from chembl import evaluate_view_search, loadData2
from archived.DoD import data_processing_utils as dpu
import time

model_path = config.Wdc.path_model
sep = config.Wdc.separator
base_outpath = config.Wdc.output_path
base_path = config.Wdc.base_path

store_client = StoreHandler()
network = fieldnetwork.deserialize_network(model_path)
columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)
viewSearch = ViewSearchPruning(network=network, store_client=store_client, base_path=base_path, csv_separator=sep)

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
    gt_path = "1438042988061.16_20150728002308-00202-ip-10-236-191-2_882777968_4.json.csv-state JOIN 1438042988061.16_20150728002308-00307-ip-10-236-191-2_27730064_4.json.csv-State"
    return attrs, values, gt_cols, gt_path

def query_4():
    attrs = ["", ""]
    values = [["Belgaum", "IXG"], ["", "BEP"], ["Ranchi", "IXR"]]
    gt_cols = [("1438042988458.74_20150728002308-00045-ip-10-236-191-2_254829946_0.json.csv", "District"), ("1438042988458.74_20150728002308-00024-ip-10-236-191-2_279531848_0.json.csv", "IATA")]
    gt_path = "1438042988458.74_20150728002308-00045-ip-10-236-191-2_254829946_0.json.csv-District JOIN 1438042988458.74_20150728002308-00024-ip-10-236-191-2_279531848_0.json.csv-Location"
    return attrs, values, gt_cols, gt_path

def test_different_sample_size():
    for num in range(0,2):
        zero_noise_samples, gt_cols, gt_path = loadData2("/home/cc/tests_wdc_diff_sample_size/wdc_gt" + str(num) + ".json")
        name = "wdc_gt" + str(num) + "/"
        for (idx, values) in enumerate(zero_noise_samples):
            print("Processing query with", 1 + 2*idx, "samples")
            attrs = ["", ""]
            es_start = time.time()
            candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values, 300)
            es_time = time.time() - es_start
            evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 3, gt_cols, gt_path, 1000, base_outpath + name + "sample" + str(idx) + "/")

if __name__ == '__main__':
    for num in range(0, 5):
        zero_noise_samples, mid_noise_samples, high_noise_samples, gt_cols, gt_path = loadData("/home/cc/tests_wdc4/wdc_gt" + str(num) + ".json")

        found = dict()
        found["method1"] = 0
        found["method2"] = 0
        found["method3"] = 0
        name = "wdc_gt" + str(num) + "/"
        for (idx, values) in enumerate(zero_noise_samples):
            print("Processing zero noise samples no.", idx)
            attrs = ["", ""]
            es_start = time.time()
            candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values, 200)
            es_time = time.time() - es_start
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 1, gt_cols, gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result1/"):
                found["method1"] += 1
            es_start = time.time()
            candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values, 300)
            es_time = time.time() - es_start
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 2, gt_cols, gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result2/"):
                found["method2"] += 1
            es_start = time.time()
            candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values, 200)
            es_time = time.time() - es_start
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 3, gt_cols, gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result3/"):
                found["method3"] += 1
        f = open(base_outpath + name + "zero_noise" + "/hit.txt", "a+")
        f.write(str(found))

        found["method1"] = 0
        found["method2"] = 0
        found["method3"] = 0
        for (idx, values) in enumerate(mid_noise_samples):
            print("Processing mid noise samples no.", idx)
            attrs = ["", ""]
            es_start = time.time()
            candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values,300)
            es_time = time.time() - es_start
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 1, gt_cols, gt_path, 1000, base_outpath + name + "mid_noise/" + "sample" + str(idx) + "/result1/"):
                found["method1"] += 1
            es_start = time.time()
            candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values, 300)
            es_time = time.time() - es_start
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 2, gt_cols, gt_path, 1000, base_outpath + name + "mid_noise/" + "sample" + str(idx) + "/result2/"):
                found["method2"] += 1
            es_start = time.time()
            candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values, 300)
            es_time = time.time() - es_start
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 3, gt_cols, gt_path, 1000, base_outpath + name + "mid_noise/" + "sample" + str(idx) + "/result3/"):
                found["method3"] += 1
        f = open(base_outpath + name + "mid_noise" + "/hit.txt", "a+")
        f.write(str(found))

        found["method1"] = 0
        found["method2"] = 0
        found["method3"] = 0
        for (idx, values) in enumerate(high_noise_samples):
            print("Processing high noise samples no.", idx)
            attrs = ["", ""]
            es_start = time.time()
            candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values, 200)
            es_time = time.time() - es_start
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 1, gt_cols, gt_path, 1000, base_outpath + name + "high_noise/" + "sample" + str(idx) + "/result1/"):
                found["method1"] += 1
            es_start = time.time()
            candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values, 300)
            es_time = time.time() - es_start
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 2, gt_cols, gt_path, 1000, base_outpath + name + "high_noise/" + "sample" + str(idx) + "/result2/"):
                found["method2"] += 1
            es_start = time.time()
            candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values, 200)
            es_time = time.time() - es_start
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 3, gt_cols, gt_path, 1000, base_outpath + name + "high_noise/" + "sample" + str(idx) + "/result3/"):
                found["method3"] += 1
        f = open(base_outpath + name + "high_noise" + "/hit.txt", "a+")
        f.write(str(found))
    base = "/home/cc/wdc_10000/"
    attrs, values, gt_cols, gt_path = query_3()
    df1 = dpu.read_column(base + gt_cols[0][0], "state")
    df2 = dpu.read_column(base + gt_cols[1][0], "State")

    x1 = df1["state"].dropna().drop_duplicates().values.tolist()
    x2 = df2["State"].dropna().drop_duplicates().values.tolist()

    # attrs, values, gt_cols, gt_path = query_3()
    # df1 = dpu.read_column(base + "1438042988061.16_20150728002308-00001-ip-10-236-191-2_876256027_0.json.csv", "Artist")
    # df2 = dpu.read_column(base + "1438042988061.16_20150728002308-00003-ip-10-236-191-2_131833501_0.json.csv", "Artist")
    #
    # x1 = df1["Artist!"].dropna().drop_duplicates().values.tolist()
    # x2 = df2["Artist!"].dropna().drop_duplicates().values.tolist()
    print(len(set(x1).intersection(set(x2))))
    print(min(len(x1), len(x2)))
    overlap = len(set(x1).intersection(set(x2))) / min(len(x1), len(x2))
    print(overlap)

    found = dict()
    found["method1"] = 0
    found["method2"] = 0
    found["method3"] = 0
    attrs, values, gt_cols, gt_path = query_1()
    name = "query_1/"
    es_start = time.time()
    candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values, 50)
    es_time = time.time() - es_start
    print("finished es")
    if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 1, gt_cols, gt_path, 10000,
                         base_outpath + name + "result1/"):
        found["method1"] += 1
    if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 2, gt_cols, gt_path, 10000,
                         base_outpath + name + "result2/"):
        found["method2"] += 1
    if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 3, gt_cols, gt_path, 10000,
                         base_outpath + name + "result3/"):
        found["method3"] += 1
    f = open(base_outpath + name + "/hit.txt", "w")
    f.write(str(found))

