import unittest

import server_config as config
from DoD import column_infer
from DoD.view_search_pruning import ViewSearchPruning, start
from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler


class MitDataTest(unittest.TestCase):
    def setUp(self):
        model_path = config.Mit.path_model
        sep = config.Mit.separator

        store_client = StoreHandler()
        network = fieldnetwork.deserialize_network(model_path)
        self.columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)
        self.viewSearch = ViewSearchPruning(network=network, store_client=store_client, csv_separator=sep)

    def test_case1(self):
        attrs = ["Mit Id", "Krb Name", "Hr Org Unit Title"]
        values = [["", "kimball", "Mechanical Engineering"]]
        types = ["int64", "object", "object"]
        start(self.viewSearch, self.columnInfer, attrs, values, types, number_jps=10,
              output_path=config.Mit.output_path)

    def test_case2(self):
        attrs = ["Mit Id", "", ""]
        values = [["", "kimball", "Mechanical"],
                  ["", "pjcorn", "Medical"],
                  ["", "Patil", "Computer Science"]]
        types = ["int64", "object", "object"]
        start(self.viewSearch, self.columnInfer, attrs, values, types, number_jps=10,
              output_path=config.Mit.output_path)

    def test_case3(self):
        # attrs = ["Building Name Long", "Ext Gross Area", "Building Room", "Room Square Footage"]
        # values = [["", "", "", ""]]
        # types = ["object", "float", "object", "float"]
        attrs = ["faculty", "building"] # 2,3,4,5,6,10 | 0,1,2,3
        values = [['madden', '']]
        types = ["object", "object"]
        start(self.viewSearch, self.columnInfer, attrs, values, types, number_jps=10,
              output_path=config.Mit.output_path)

    def test_case4(self):
        attrs = ["Subject", "Title", "Publisher"]
        values = [["", "Man who would be king and other stories", "Oxford university press, incorporated"]]
        types = ["object", "object", "object"]
        start(self.viewSearch, self.columnInfer, attrs, values, types, number_jps=10,
              output_path=config.Mit.output_path)

    def test_case5(self):
        attrs = ["Building Name Long", "Ext Gross Area", "Building Room", "Room Square Footage"]
        values = [["224 Albany Street", "", "", ""]]
        types = ["object", "float", "object", "float"]
        start(self.viewSearch, self.columnInfer, attrs, values, types, number_jps=10,
              output_path=config.Mit.output_path, offset=100)
