from archived.ddapi import API
from aurum_api.apiutils import Relation
from archived.modelstore import StoreHandler
from knowledgerepr.fieldnetwork import deserialize_network
from join_path import JoinKey, JoinPath
import sys
from archived.DoD import data_processing_utils as dpu
from collections import defaultdict


class Join_Path_API:
    def __init__(self, model_path):
        self.network = deserialize_network(model_path)
        self.api = API(self.network)
        self.api.init_store()
    
    def find_join_paths_from(self, start, max_hop, result):
        columns = self.api.drs_from_table(start)
        for col in columns:
            self.find_join_path(col, max_hop, result, [self.col_to_join_key(col)])

    def find_join_path(self, col, max_hop, result, cur_path):
        if max_hop == 0:
            return
        if not self.is_column_nan(col):
            neighbors = self.network.neighbors_id(col, Relation.CONTENT_SIM)
            for nei in neighbors:
                cur_path.append(self.col_to_join_key(nei))
                result.append(JoinPath(cur_path[:]))
                self.find_join_path(nei, max_hop - 1, result, cur_path)
                cur_path.pop()
    
    def is_column_nan(self, col):
        non_empty = self.network.get_non_empty_values_of(col.nid)
        if non_empty == 0:
            return True
        return False

    def get_sizes_from_drs(self, col):
        unique = self.network.get_size_of(col.nid)
        total = int(self.network.get_size_of(col.nid)/self.network.get_cardinality_of(col.nid))
        non_empty = self.network.get_non_empty_values_of(col.nid)
        return unique, total, non_empty


    def col_to_join_key(self, col):
        unique, total, non_empty = self.get_sizes_from_drs(col)
        return JoinKey(col, unique, total, non_empty)


corr_cache = {}
column_cache = {}

def get_correlations(jp_list, data_path, f):
    m = group_by_jp_len(jp_list)
    f.write("jp1, jp2, jp_len, corr\n")
    for _, jps in m.items():
        n = len(jps)
        for i in range(n):
            for j in range(i+1,n):
                corr_list = corelation(jps[i].join_path, jps[j].join_path, data_path)
                print(jps[i].to_str())
                print(jps[j].to_str())
                print(str(corr_list))
                f.write(jps[i].to_str() + "," + jps[j].to_str() + "," + str(corr_list) + "\n")
    f.close()

def group_by_jp_len(jp_list):
    m = defaultdict(list)
    for jp in jp_list:
        m[len(jp.join_path)].append(jp)
    return m

def corelation(jp1, jp2, data_path):
    '''
    correlation measure: cov(i,j)/[stdev(i)*stdev(j)]
    if the number of distinct values is low, stdev will be
    close to 0, which leads the correlation to be nan.
    '''
    if len(jp1) != len(jp2):
        return []
    corr_list = []
    for i in range(len(jp1)):
        jk1 = jp1[i]
        jk2 = jp2[i]
        if (jk1, jk2) in corr_cache:
            corr_list.append(corr_cache[(jk1, jk2)])
            continue
        if jk1.tbl == jk2.tbl and jk1.col == jk2.col:
            corr_list.append(1)
            continue
        if jk1.unique_values < 2 or jk2.unique_values < 2:
            corr_list.append(None)
            continue
        col1 = get_column_content(data_path, jk1)
        col2 = get_column_content(data_path, jk2)
        if col1.dtype == col2.dtype == 'int64':
            co = col1.corr(col2)
        elif col1.dtype == col2.dtype == 'object':
            col1 = col1.astype('category').cat.codes
            col2 = col2.astype('category').cat.codes
            co = col1.corr(col2)
        else:
            co = 0
        corr_cache[(jk1, jk2)] = co
        corr_cache[(jk2, jk1)] = co
        corr_list.append(co)
    return corr_list

def get_column_content(data_path:str, jk: JoinKey):
    query = (jk.tbl, jk.col)
    if query in column_cache:
        return column_cache[query]
    else:
        df = dpu.read_column(data_path+jk.tbl, jk.col)[jk.col]
        column_cache[query] = df
    return df

if __name__ == '__main__':
    # create store handler
    store_client = StoreHandler()
    # read command line arguments
    data_path = sys.argv[1] # path to csv files
    path = sys.argv[2]  # path = '../models/adventureWorks/'
    table = sys.argv[3]     # Employee.csv
    max_hop = int(sys.argv[4])   # max_hop of join paths

    network = deserialize_network(path)
    api = API(network)
    api.init_store()

    # find join paths
    result = []
    find_join_paths_from(table, max_hop, result)
    print("# join paths:", len(result))
    for jp in result:
        jp.print_metadata_str()
        print("------------------------")