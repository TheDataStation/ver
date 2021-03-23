import unittest

import server_config as config
from DoD import column_infer
from DoD.view_search_pruning import ViewSearchPruning, start, evaluate_view_search
from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler
from DoD import data_processing_utils as dpu

'''
Test cases for adventureWork Dataset
'''
class ChemblDataTest(unittest.TestCase):
    def setUp(self):
        model_path = config.Chembl.path_model
        sep = config.Chembl.separator

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
        '''
        GroundTruth SQL:
            SELECT FirstName, LastName, Gender, JobTitle
            FROM HumanResources.Employee JOIN Person.Person
            on Employee.BusinessEntityID = Person.BusinessEntityID;
        '''
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

        values = [['Glycine max', 'Doxorubicin'],
                  ['Mus musculus', 'Ciprofloxacin']]
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
            if cnt == 50:
                break
            values = []
            values.append(data[idx])
            values.append(data[idx+1])
            values.append(data[idx+2])
            num_candidate_group, num_join_paths = evaluate_view_search(self.viewSearch, self.columnInfer, attrs, values, 4)
            f.write(str(num_candidate_group) + " " + str(num_join_paths) + '\n')
            cnt += 1
            idx += 10
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
