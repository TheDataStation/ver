import unittest

import server_config as config
from DoD import column_infer
from DoD.view_search_pruning import ViewSearchPruning, start
from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler

'''
Test cases for TPCH Dataset
'''


class TpchDataTest(unittest.TestCase):
    def setUp(self):
        model_path = config.TPCH.path_model
        sep = config.TPCH.separator

        store_client = StoreHandler()
        network = fieldnetwork.deserialize_network(model_path)
        self.columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)
        self.viewSearch = ViewSearchPruning(network=network, store_client=store_client, csv_separator=sep)

    def test_case1(self):
        '''
        GroundTruth SQL:
            select
                s_acctbal,
                s_name,
                n_name,
                p_partkey,
                p_mfgr,
                s_address,
                s_phone,
                s_comment
            from
                part,
                supplier,
                partsupp,
                nation,
                region
            where
                p_partkey = ps_partkey
                and s_suppkey = ps_suppkey
                and s_nationkey = n_nationkey
                and n_regionkey = r_regionkey
            limit 10
        '''
        attrs = ["s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment"]
        # TODO: setting values will crash
        # values = [["8232.28","Supplier#000003624","UNITED STATES","1123","Manufacturer#2","gbXvyWmGOMR2g,UizoehIDlhoWKN8Mad47X",
        #           "34-926-564-4481", "beans above the blithely regular ideas integrate furiously final instructions. furiously fi"]]
        # TODO: first batch of join paths are all FALSE (second batch have valid join paths)
        values = []
        types = ["float", "object", "object", "int64", "object", "object", "object", "object"]
        start(self.viewSearch, self.columnInfer, attrs, values, types, number_jps=10,
              output_path=config.Mit.output_path)

    def test_case2(self):
        '''
        GroundTruth SQL:
            select
                c_mktsegment,
                l_orderkey,
                o_orderdate,
                o_shippriority
            from
                customer,
                orders,
                lineitem
            where
                c_custkey = o_custkey
                and l_orderkey = o_orderkey
            limit 10
        '''

        attrs = ["c_mktsegment", "l_orderkey", "o_orderdate", "o_shippriority"]
        # TODO: setting values will crash
        # values = [["HOUSEHOLD", "290", "1994 - 01 - 01", "0"]]
        # TODO: finding join path took too long
        values = []
        types = ["object", "int64", "object", "int64"]
        start(self.viewSearch, self.columnInfer, attrs, values, types, number_jps=10,
              output_path=config.Mit.output_path)

    def test_case3(self):
        '''
        GroundTruth SQL:
            select
                c_custkey,
                c_name,
                o_orderdate,
                l_extendedprice,
                l_discount,
                n_name,
                r_name
            from
                customer,
                orders,
                lineitem,
                supplier,
                nation,
                region
            where
                c_custkey = o_custkey
                and l_orderkey = o_orderkey
                and l_suppkey = s_suppkey
                and c_nationkey = s_nationkey
                and s_nationkey = n_nationkey
                and n_regionkey = r_regionkey
            limit 10
        '''

        attrs = ["c_custkey", "c_name", "o_orderdate", "l_extendedprice", "l_discount", "n_name", "r_name"]
        # TODO: setting values will crash
        # values = [["99607", "Customer  # 000099607","1997-08-27","42190.50","0.07","UNITED STATES","AMERICA"]]
        # TODO: can't find valid join path. Tried max_hop = 3 instead, but got stuck?
        values = []
        types = ["int64", "object", "object", "float", "float", "object", "object"]
        start(self.viewSearch, self.columnInfer, attrs, values, types, number_jps=10,
              output_path=config.Mit.output_path)
