import unittest

import server_config as config
from DoD import column_infer
from DoD.view_search_pruning import ViewSearchPruning, start
from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler

'''
Test cases for adventureWork Dataset
'''
class AWDataTest(unittest.TestCase):
    def setUp(self):
        model_path = config.Mit.path_model
        sep = config.Mit.separator

        store_client = StoreHandler()
        network = fieldnetwork.deserialize_network(model_path)
        self.columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)
        self.viewSearch = ViewSearchPruning(network=network, store_client=store_client, csv_separator=sep)

    def test_case1(self):
        '''
        GroundTruth SQL:
            SELECT FirstName, LastName, Gender, JobTitle
            FROM HumanResources.Employee JOIN Person.Person
            on Employee.BusinessEntityID = Person.BusinessEntityID;
        '''
        attrs = ["FirstName", "LastName", "JobTitle"]
        values = [["Syed", "Abbas", "Pacific Sales Manager"],
                  ["Ryan", "Cornelsen", "Production Technician - WC40"],
                  ["Erin", "Hagens", "Buyer"]]
        types = ["object", "object", "object"]
        start(self.viewSearch, self.columnInfer, attrs, values, types, number_jps=10,
              output_path=config.Mit.output_path)

    def test_case2(self):
        '''
        GroundTruth SQL:
            SELECT Person.CountryRegion.Name, Sales.Currency.CurrencyCode, Sales.Currency.Name
            FROM Sales.Currency
            JOIN Sales.CountryRegionCurrency
            on Sales.Currency.CurrencyCode = Sales.CountryRegionCurrency.CurrencyCode
            JOIN Person.CountryRegion
            on Sales.CountryRegionCurrency.CountryRegionCode = Person.CountryRegion.CountryRegionCode;
        '''
        attrs = ["Country", "Currency", "Currency Name"]
        values = [["Thailand", "THB", "Baht"],
                  ["United States", "USD", "US Dollar"],
                  ["China", "CNY", "Yuan Renminbi"]]
        types = ["object", "object", "object"]
        start(self.viewSearch, self.columnInfer, attrs, values, types, number_jps=10,
              output_path=config.Mit.output_path)

    def test_case3(self):
        '''
        GroundTruth SQL:
            SELECT Person.Address.AddressLine1, Person.StateProvince.Name, Sales.SalesTerritory.Name
            FROM Person.Address
            JOIN Person.StateProvince
            ON Person.Address.StateProvinceID = Person.StateProvince .StateProvinceID
            JOIN Sales.SalesTerritory
            ON Sales.SalesTerritory.TerritoryID = Person.StateProvince.TerritoryID;
        '''
        attrs = ["Address", "State", "Region"]
        values = [["1046 San Carlos Avenue", "California", "Southwest"],
                  ["105 Woodruff Ln.", "Washington", "Northwest"],
                  ["1050 Greenhills Circle", "New South Wales", "Australia"]]
        types = ["object", "object", "object"]
        start(self.viewSearch, self.columnInfer, attrs, values, types, number_jps=10,
              output_path=config.Mit.output_path)