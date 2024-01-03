import unittest

import server_config as config
from archived.DoD import column_infer
from archived.DoD import ViewSearchPruning, start, evaluate_view_search, found_view_eval
from knowledgerepr import fieldnetwork
from archived.modelstore import StoreHandler
from archived.DoD import data_processing_utils as dpu
from archived.DoD import generate_tests

'''
Test cases for adventureWork Dataset
'''
class ChemblDataTest(unittest.TestCase):
    def setUp(self):
        model_path = config.Chembl.path_model
        sep = config.Chembl.separator
        self.base_outpath = config.Chembl.output_path

        store_client = StoreHandler()
        network = fieldnetwork.deserialize_network(model_path)
        self.columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)
        self.viewSearch = ViewSearchPruning(network=network, store_client=store_client, csv_separator=sep)

    def test_case1(self):
        attrs = ['assay_test_type', 'assay_category', 'journal', 'year', 'volume']
        values = [['', '', '', '', '']]
        types = ['object', 'object', 'object', 'object', 'object']
        start(self.viewSearch, self.columnInfer, attrs, values, types, number_jps=10,
              output_path=config.Chembl.output_path)


    def test_case2(self):
        attrs = ['accession', 'sequence', 'organism', 'start_position', 'end_position', 'assay_category']
        values = [['', '', '', '', '', '']]
        types = ['object', 'object', 'object', 'object', 'object', 'object', 'object']
        start(self.viewSearch, self.columnInfer, attrs, values, types, number_jps=10,
              output_path=config.Chembl.output_path)

    def test_case3(self):
        attrs = ['', '']
        # values = [['Homo sapiens', 'Eimeria'],
        #           ['Mus musculus', 'Plasmodium']]
        # values = [['Homo sapiens', 'XVI'],
        #           ['Mus musculus', 'XIX']]

        # values = [['Glycine max', 'Doxorubicin'],
        #           ['Mus musculus', 'Ciprofloxacin']]
       # values = [['Homo sapiens', 'Doxorubicin'], ['Mus musculus', 'Ciprofloxacin'], ['Rattus norvegicus', 'Chloroquine']]
       #  values = [['Staphylococcus aureus', 'Chloroquine'], ['Canis lupus familiaris', 'Ampicillin'],
       #   ['Escherichia coli', 'Fluconazole']]
        values = [['Homo sapiens', '2-{4-[3-(1,4,5,6-Tetrahydro-pyrimidin-2-ylamino)-benzoylamino]-phenyl}-cyclopropanecarboxylic acid'], ['Epstein-Barr virus (strain B95-8) (HHV-4) (Human herpesvirus 4)', '(+/-)-8'], ['Human immunodeficiency virus type 1 group M subtype B (isolateBRU/LAI) (HIV-1)', 'NSC-364170']]
        types = ['object', 'object']

        start(self.viewSearch, self.columnInfer, attrs, values, types, number_jps=10,
              output_path=config.Chembl.output_path)

    def test_case4(self):
        f = open('view_search_eval_4.txt', 'w')
        attrs = ['', '']

        path1 = "/Users/gongyue/data/groundTruth/assay_organism.csv"
        df1 = dpu.read_relation_on_copy2(path1, ',')
        path2 = "/Users/gongyue/data/groundTruth/compound_name_small.csv"
        df2 = dpu.read_relation_on_copy2(path2, ',')

        data = [[] for _ in range(500)]
        for i, row in df1.iterrows():
            if i >= 500:
                break
            data[i].append((row['v']))

        for i, row in df2.iterrows():
            if i >= 500:
                break
            data[i].append(row['v'])

        idx = 0
        cnt = 0
        while True:
            if cnt == 1:
                break
            values = []
            values.append(data[idx])
            values.append(data[idx+1])
            values.append(data[idx+2])
            print(values)
            evaluate_view_search(self.viewSearch, self.columnInfer, attrs, values, 1, 1000, self.base_outpath + "/result1/")
            evaluate_view_search(self.viewSearch, self.columnInfer, attrs, values, 2, 1000, self.base_outpath + "/result2/")
            evaluate_view_search(self.viewSearch, self.columnInfer, attrs, values, 3, 1000, self.base_outpath + "/result3/")
            evaluate_view_search(self.viewSearch, self.columnInfer, attrs, values, 4, 1000, self.base_outpath + "/result4/")
            cnt += 1
            idx += 10

    def test_found_view_or_not(self):
        zero_noise, med_noise, high_noise = generate_tests()
        gt_cols = [("public.assays.csv", "assay_organism"), ("public.compound_records.csv", "compound_name")]
        attrs = ["", ""]
        # correct1, correct2, correct3 = 0, 0, 0
        # for case in zero_noise:
        #     print(case)
        #     f1, f2, f3 = found_view_eval(self.columnInfer, attrs, case, gt_cols)
        #     if f1:
        #         correct1 += 1
        #     if f2:
        #         correct2 += 1
        #     if f3:
        #         correct3 += 1
        # print(correct1, correct2, correct3)
        # correct1, correct2, correct3 = 0, 0, 0
        # for case in med_noise:
        #     print(case)
        #     f1, f2, f3 = found_view_eval(self.columnInfer, attrs, case,  gt_cols)
        #     if f1:
        #         correct1 += 1
        #     if f2:
        #         correct2 += 1
        #     if f3:
        #         correct3 += 1
        # print(correct1, correct2, correct3)
        correct1, correct2, correct3 = 0, 0, 0
        for case in high_noise:
            print(case)
            f1, f2, f3 = found_view_eval(self.columnInfer, attrs, case, gt_cols)
            if f1:
                correct1 += 1
            if f2:
                correct2 += 1
            if f3:
                correct3 += 1
        print(correct1, correct2, correct3)

    def test_view_spec_1(self):
        # no ambiguity
        # attrs = ['', '', '', '', '', '', '', '', '']

        # activity.units activity.type assays.curated_by organism assay_tissue who_name
        values = [['uM', 'Selective toxicity', 'Autocuration', 'Homo sapiens',  'Frontal cortex', 'hydrogen peroxide', 'Tigatuzumab light chain', 'bioassay type', 'Lung'],
                  ['ms', 'Relative potency', 'Expert', 'Mus musculus', 'Caudal artery', 'tibezonium iodide', 'Enavatuzumab heavy chain', 'assay format', 'Breast Adenocarcinoma']]
        # types = ['object', 'object', 'object', 'object', 'object', 'object', 'object', object]
        attrs = [''] * len(values[0])
        types = ['object'] * len(values[0])
        start(self.viewSearch, self.columnInfer, attrs, values, types, number_jps=10,
              output_path=config.Chembl.output_path)

    def test_view_spec_2(self):
        values = [['Rattus norvegicus'], ['Homo sapiens']]
        attrs = [''] * len(values[0])
        types = ['object'] * len(values[0])
        start(self.viewSearch, self.columnInfer, attrs, values, types, number_jps=10,
              output_path=config.Chembl.output_path)
