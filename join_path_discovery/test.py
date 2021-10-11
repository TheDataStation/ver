from ddapi import API
from api.apiutils import Relation
from modelstore.elasticstore import StoreHandler
from knowledgerepr.fieldnetwork import deserialize_network
import pandas as pd
from join_path import JoinKey, JoinPath
from DoD import data_processing_utils as dpu
import sys

def get_column_content(sn, fn):
    print("reading", sn, fn)
    df = dpu.read_column(sn, fn)
    return df

if __name__ == "__main__":
    cnt = 0
    log = open('log_test.txt', 'w')
    for i in range(2):
        try:
            sn, fn = '/home/cc/datasets/wdc_100000/1438042988061.16_20150728002308-00071-ip-10-236-191-2_806531725_6.json.csv', 'ChannelMarket'
            df = get_column_content(sn, fn)
        except:
            log.write(sn + ' ' + fn)
            cnt += 1
            continue
        print(i)
    log.close()
    print('failed reading columns:', cnt)
    # # create store handler
    # store_client = StoreHandler()
    # # read command line arguments
    # data_path = '/home/cc/datasets/adventureWorks/' # path to csv files
    # path = '../models/adventureWorks/'  # path = '../models/adventureWorks/'
    # table = 'Employee.csv'     # Employee.csv
    # max_hop = 2  # max_hop of join paths

    # network = deserialize_network(path)
    # api = API(network)
    # api.init_store()

    # columns = api.drs_from_table(table)
    # for col in columns:
    #     df = pd.read_csv(data_path + col.source_name, usecols=[col.field_name])
    #     res = network.get_non_empty_values_of(col.nid)
    #     print(col)
    #     print("gt", df[col.field_name].notnull().sum())
    #     if not res:
    #         print('none')
    #     else:
    #         print(res)