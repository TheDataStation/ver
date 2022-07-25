import unittest

import server_config as config
from DoD import column_infer
from DoD.view_search_pruning import ViewSearchPruning, start
from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler
from DoD import data_processing_utils as dpu
import sqlite3

'''
Test cases for adventureWork Dataset
'''
class OpenDataTest(unittest.TestCase):
    def setUp(self):
        model_path = config.OpenData.path_model
        sep = config.OpenData.separator

        store_client = StoreHandler()
        network = fieldnetwork.deserialize_network(model_path)
        self.columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)
        self.viewSearch = ViewSearchPruning(network=network, store_client=store_client, csv_separator=sep)

    # def test_view_spec_1(self):
    #     values = [['America', 'Asia', 'Europe'],
    #               ['Africa', 'America', 'Oceania']]
    #     attrs = [''] * len(values[0])
    #
    #     gt_path = "/Users/gongyue/data/groundTruth/gt_1.csv"
    #
    #     self.columnInfer.evaluation(attrs, values, gt_path)

    '''
    33 test cases 
    Table: t_1934eacab8c57857____c10_0____0
    Column: "Country/region name"
    '''
    def test_view_spec_2(self):
        conn = sqlite3.connect('/Users/gongyue/Downloads/benchmark.sqlite')
        c = conn.cursor()
        data = []
        for row in c.execute("SELECT DISTINCT \"Country/region name\" FROM t_1934eacab8c57857____c10_0____0"):
            data.append(row[0])
        data = data[:3 * (len(data) // 3)]
        print("num", len(data) / 3)

        values = [[] for _ in range(3)]
        i = 0
        for e in data:
            if i == 3:
                i = 0
            values[i].append(e)
            i += 1

        attrs = [''] * len(values[0])

        gt_path = "/Users/gongyue/data/groundTruth/gt_2.csv"
        self.columnInfer.evaluation(attrs, values, gt_path)

    '''
        3 test cases 
        Table: t_1934eacab8c57857____c10_0____0
        Column: "Division name"
    '''
    def test_view_spec_3(self):
        conn = sqlite3.connect('/Users/gongyue/Downloads/benchmark.sqlite')
        c = conn.cursor()
        data = []
        for row in c.execute("SELECT DISTINCT \"Division name\" FROM t_1934eacab8c57857____c10_0____0"):
            if len(row[0].split(" ")) > 1:
                data.append(row[0])
        data = data[:3*(len(data)//3)]
        print("num", len(data)/3)

        values = [[] for _ in range(3)]
        i = 0
        for e in data:
            if i == 3:
                i = 0
            if i == 1:
                values[i].append(e.split()[0])
            else:
                values[i].append(e)
            i += 1

        attrs = [''] * len(values[0])

        gt_path = "/Users/gongyue/data/groundTruth/gt_3.csv"
        self.columnInfer.evaluation(attrs, values, gt_path)

    '''
            40 test cases 
            Table: t_1934eacab8c57857____c10_1____1
            Column: "Fund centre name"
    '''
    def test_view_spec_4(self):
        conn = sqlite3.connect('/Users/gongyue/Downloads/benchmark.sqlite')
        c = conn.cursor()
        data = []
        for row in c.execute("SELECT DISTINCT \"Fund centre name\" FROM t_1934eacab8c57857____c10_1____1"):
            data.append(row[0])
        data = data[:3 * (len(data) // 3)]
        print("num", len(data) / 3)

        values = [[] for _ in range(3)]
        i = 0
        for e in data:
            if i == 3:
                i = 0
            values[i].append(e)
            i += 1

        attrs = [''] * len(values[0])

        gt_path = "/Users/gongyue/data/groundTruth/gt_4.csv"
        self.columnInfer.evaluation(attrs, values, gt_path)

    '''
                4 test cases 
                Table: t_1934eacab8c57857____c10_0____1
                Column: "Division name"
    '''
    def test_view_spec_5(self):
        conn = sqlite3.connect('/Users/gongyue/Downloads/benchmark.sqlite')
        c = conn.cursor()
        data = []
        for row in c.execute("SELECT DISTINCT \"Division name\" FROM t_1934eacab8c57857____c10_0____1"):
            data.append(row[0])
        data = data[:3 * (len(data) // 3)]
        print("num", len(data) / 3)

        values = [[] for _ in range(3)]
        i = 0
        for e in data:
            if i == 3:
                i = 0
            values[i].append(e)
            i += 1

        attrs = [''] * len(values[0])

        gt_path = "/Users/gongyue/data/groundTruth/gt_5.csv"
        self.columnInfer.evaluation(attrs, values, gt_path)

    '''
        36 test cases 
        Table: t_1934eacab8c57857____c10_0____2
        Column: "Country/region name"
    '''
    # def test_view_spec_6(self):
    #     conn = sqlite3.connect('/Users/gongyue/Downloads/benchmark.sqlite')
    #     c = conn.cursor()
    #     data = []
    #     for row in c.execute("SELECT DISTINCT \"Country/region name\" FROM t_1934eacab8c57857____c10_0____2"):
    #         data.append(row[0])
    #     data = data[:3 * (len(data) // 3)]
    #     print("num", len(data) / 3)
    #
    #     values = [[] for _ in range(3)]
    #     i = 0
    #     for e in data:
    #         if i == 3:
    #             i = 0
    #         values[i].append(e)
    #         i += 1
    #
    #     attrs = [''] * len(values[0])
    #
    #     gt_path = "/Users/gongyue/data/groundTruth/gt_6.csv"
    #     self.columnInfer.evaluation(attrs, values, gt_path)

    # def test_view_spec_2_with_error(self):
    #     conn = sqlite3.connect('/Users/gongyue/Downloads/benchmark.sqlite')
    #     c = conn.cursor()
    #     data = []
    #     for row in c.execute("SELECT DISTINCT \"Country/region name\" FROM t_1934eacab8c57857____c10_0____0"):
    #         data.append(row[0])
    #     data = data[:3 * (len(data) // 3)]
    #     print("num", len(data) / 3)
    #
    #     values = [[] for _ in range(3)]
    #     i = 0
    #     for e in data:
    #         if i == 3:
    #             i = 0
    #         if i == 1:
    #             values[i].append(e[0:len(e)//2])
    #         else:
    #             values[i].append(e)
    #         i += 1
    #
    #     attrs = [''] * len(values[0])
    #
    #     gt_path = "/Users/gongyue/data/groundTruth/gt_2.csv"
    #     self.columnInfer.evaluation(attrs, values, gt_path)