import pandas as pd
import csv
import os
import time
import numpy as np

base_path = '/home/cc/opendata_cleaned/'
# query_path = '/home/cc/queries4/'

view_cache = {}

def get_content_by_nid(nid):
    (_, _, sn1, fn1) = network.get_info_for([nid])[0]
    print(sn1, fn1)

def check_col(tbl, col):
    df = pd.read_csv('/home/cc/opendata_cleaned/' + tbl, usecols=[col])
    print(df)

def sample_network(num):
    print("begin read")
    df = pd.read_csv("network_05.csv")
    print("begin sample")
    df_sample = df.sample(num)
    df_sample.to_csv('network_sampled_05_2.csv', index=False)


def _join_two_tables(tbl1, col1, tbl2, col2):
    io_time = 0
    print("reading tables")
    # df1 = pd.read_csv(base_path + tbl1)
    if tbl1 in view_cache:
        print("Hit Cache!")
        df1 = view_cache[tbl1]
    else:
        io_start = time.time()
        df1 = pd.read_csv(base_path + tbl1)
        io_time += time.time() - io_start
        if len(df1.index) > 1000:
            df1 = df1.sample(1000)
        view_cache[tbl1] = df1
    
    if tbl2 in view_cache:
        print("Hit Cache!")
        df2 = view_cache[tbl2]
    else:
        io_start = time.time()
        df2 = pd.read_csv(base_path + tbl2)
        io_time += time.time() - io_start
        if len(df2.index) > 1000:
            df2 = df2.sample(1000)
        view_cache[tbl2] = df2
   
    print("begin join")
    col_names1, col_names2 = df1.columns, df2.columns
    old_col_names1, old_col_names2 = col_names1, col_names2
    col_names1 = [s + '_x' for s in col_names1]
    col_names2 = [s + '_y' for s in col_names2]
    df1.columns = col_names1
    df2.columns = col_names2
    col1 = col1 + '_x'
    col2 = col2 + '_y'
    df1[col1] = df1[col1].astype(str)
    df2[col2] = df2[col2].astype(str)
    print("begin merge")
    print(len(df1.index))
    print(len(df2.index))
    start_merge = time.time()
    print(len(df1.dropna(subset=[col1])))
    print(len(df2.dropna(subset=[col2])))
    print(df1.columns)
    df1_copy = df1.dropna(subset=[col1]).copy()
    df2_copy = df2.dropna(subset=[col2]).copy()
    df1_copy.set_index(col1, inplace=True)
    df2_copy.set_index(col2, inplace=True)
    if col1 == col2:
        merged = df1.dropna(subset=[col1]).merge(df2.dropna(subset=[col1]), how='inner', on=col1)
    else:
        merged = df1_copy.join(df2_copy, how='left')
    print("time to join:", time.time() - start_merge)
    print("finish merge")
    print(len(merged.index))
    df1.columns, df2.columns = old_col_names1, old_col_names2
    return merged, io_time


def join_two_tables(tbl1, col1, tbl2, col2):
    try:
        df1 = pd.read_csv(base_path + tbl1, low_memory=False)
        if len(df1.index) > 1000:
            df1 = df1.sample(1000)
        df2 = pd.read_csv(base_path + tbl2, low_memory=False)
        if len(df2.index) > 1000:
            df2 = df2.sample(1000)
        col_names1, col_names2 = df1.columns, df2.columns
        col_names1 = [s + '_x' for s in col_names1]
        col_names2 = [s + '_y' for s in col_names2]
        df1.columns = col_names1
        df2.columns = col_names2
        col1 = col1 + '_x'
        col2 = col2 + '_y'
        print(df1.columns)
        print(col1)
        print(df2.columns)
        print(col2)
        df1[col1] = df1[col1].astype(str)
        df2[col2] = df2[col2].astype(str)
        print("begin join")
        s = time.time()
        if col1 == col2:
            merged = df1.merge(df2.dropna(subset=[col1]), how='inner', on=col1)
        else:
            merged = df1.dropna(subset=[col1]).drop_duplicates(subset=[col1], keep="last").merge(df2.dropna(subset=[col2]).drop_duplicates(subset=[col2], keep="last"), how='inner', left_on=col1, right_on=col2)
        print("time used:", time.time() - s)
        print(len(merged.index))
        return (merged, col_names1, col_names2)
    except KeyError:
        print("key error happened")


def sample_column(df):
    # df = pd.read_csv(base_path + tbl, low_memory=False)
    df = preprocessing_df(df)
    if df.empty:
        return  None
    if len(df.columns) < 3:
        retry = len(df.columns)
    else:
        retry = 5
    while retry != 0:
        df_sub = df.sample(n=1, axis='columns', replace=False)
        df_sub = df_sub.dropna().drop_duplicates()
        if len(df.index) < 3:
            retry -= 1
        else:
            retry = 0
    if len(df_sub.index) < 3:
        return None
    col_name = df_sub.columns[0]
    return col_name
   # return (col_name, df_sub[col_name].sample(3).tolist())
 
def clean_queries(query_path):
    for filename in os.listdir(query_path):
        full_path = os.path.join(query_path, filename)
        df = pd.read_csv(full_path, low_memory=False)
        columns = []
        bad_query = False
        for (col_name, col_data) in df.iteritems():
            if 'valor' in col_name:
                continue
            col_values = col_data.values.tolist()
            for v in col_values:
                if v == 'gdf' or v == 'rain' or v == 'year' or v == 'rd' or v == 'month':
                    bad_query = True
                    break
                if isinstance(v, int) or isinstance(v, float) or v.isnumeric():
                    bad_query = True
                    break
                if ';' in v:
                    bad_query = True
                    break
                if v == '-' or v == '.':
                    bad_query = True
                    break
            if bad_query:
                break
            columns.append(col_values)
        if len(columns) == 2 and columns[0] != columns[1]:
            df.to_csv('/home/cc/queries_cleaned2_2/{}'.format(filename), index=False)
            print('saved', filename)
          
def preprocessing_df(df):
    valid_cols = []
    for col_name in df.columns:
        if df.dtypes[col_name] != 'object':
            continue
        valid_cols.append(col_name)
    return df[valid_cols]

def generate_query(network_path):
    network = pd.read_csv(network_path)
    output_base = '/home/cc/queries2/'
    for index, row in network.iterrows():
        if index <= 4442:
            continue
        print("index", index)
        filename = "jp_{}.csv".format(index)
        tbl1, col1, tbl2, col2 = row['tbl1'], row['col1'], row['tbl2'], row['col2']
        res = join_two_tables(tbl1, col1, tbl2, col2)
        if res is None:
            continue
        merged, col_names1, col_names2 = res[0], res[1], res[2]
        data = {}
        sampled_col1 = sample_column(merged[col_names1])
        if sampled_col1:
            sampled_col2 = sample_column(merged[col_names2])
            if sampled_col2:
                query_df = merged[[sampled_col1, sampled_col2]].dropna().drop_duplicates()
                if len(query_df.index) >= 3:
                    query_df_sampled = query_df.sample(3)
                    print("saving", filename)
                    query_df_sampled.to_csv(output_base + filename, index=False)

# generate_query('network_sampled_05_2.csv')
clean_queries('/home/cc/queries2/')

# network = pd.read_csv('network_sampled_05_2.csv')
# row = network.iloc[2597]
# tbl1, col1, tbl2, col2 = row['tbl1'], row['col1'], row['tbl2'], row['col2']
# res = join_two_tables(tbl1, col1, tbl2, col2)
# # sample_network(10000)
# generate_query()
# clean_queries()

# join_paths = pd.read_csv('/home/cc/join_path_discovery/network_sampled_05.csv')
# print(join_paths.iloc[983]['tbl1'])

# for filename in os.listdir('/home/cc/queries100/')[0:10]:
#     print("processing", filename)
#     print(int(filename[3:-4]))

# sample_network(10000)
# network = pd.read_csv('network_sampled.csv')
# output_base = '/home/cc/queries2/'
# for index, row in network.iterrows():
#     print(index)
#     filename = "jp_{}.csv".format(index)
#     data = {}
#     sampled1 = sample_column(row['tbl1'])
#     if sampled1:
#         data[sampled1[0]] = sampled1[1]
#         sampled2 = sample_column(row['tbl2'])
#         if sampled2:
#             data[sampled2[0]] = sampled2[1]
#             df = pd.DataFrame(data)
#             df.to_csv(output_base + filename, index=False)            
    # merged = join_two_tables(row['tbl1'], row['col1'], row['tbl2'], row['col2'])


# network_file = open('test.csv', 'w', encoding='UTF8', buffering=1)
# csv_writer = csv.writer(network_file)
# csv_writer.writerow(["x,c", "jj"])
# check_col('data_overheid_nl___101bc760-fbf3-41bb-9c17-ef4ada651398___0_0.csv', 'driver_levensduur')


# for filename in os.listdir(query_path)[:5]:
#     print("processing", filename)

#_join_two_tables('datos_gob_mx___7146e282-9d11-470a-b5af-0afa626ee69b___0_22.csv', 'Sobre los medicamentos que le recetaron ¿la persona que lo atendió…', 'datos_gob_mx___7146e282-9d11-470a-b5af-0afa626ee69b___0_6.csv', 'm308a')