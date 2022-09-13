import random
import os
import shutil
import pandas as pd
import json

join_path_dir = '/home/cc/generality_experiment/views_attr/'
found_gt_path = join_path_dir + 'found_gt.txt'

def rank_queries_by_join_path_num():
    query_list = []
    query_count = []
    with open(found_gt_path) as queries:
        for query in queries:
            query_list.append(query.strip()[0:-5])

    for query in query_list:
        df = pd.read_csv(join_path_dir + query + '/join_paths.csv')
        print(len(df.index))
        query_count.append((query, len(df.index)))
        # log_path = join_path_dir + query + '/log.txt'
        # with open(log_path) as log:
        #     lines = log.readlines()
        #     num_join_paths = int(lines[1].split(":")[1].strip())
        #     query_count.append((query, num_join_paths))
    
    query_count = sorted(query_count, key = lambda x: x[1])
    return query_count

def classify_queries():
    query_count = rank_queries_by_join_path_num()
    f1 = open(join_path_dir + 'under_100k.txt', 'w')
    f2 = open(join_path_dir + 'over_100k.txt', 'w')
    for query, count in query_count:
        print(count)
        if count <= 100000:
            f1.write('{}, {}\n'.format(query, count))
        else:
            f2.write('{}, {}\n'.format(query, count))

def get_queries_with_gt():
    for filename in os.listdir(join_path_dir):
        gt_log_path = join_path_dir + filename

def sample_n_queries(n):
    query_count = rank_queries_by_join_path_num()
    l = []
    for query, count in query_count:
        if count <= 100000:
            l.append((query, count))
    
    sampled = random.sample(l, n)
    sampled.sort(key = lambda x: x[1])
    print(sampled)
    f = open(join_path_dir + 'under_100k_sampled.txt', 'w')
    for query, count in sampled:
        f.write('{}, {}\n'.format(query, count))


def sample_queries(n):
    l = []
    for filename in os.listdir('/home/cc/output_views_small/'):
        if filename[0:2] == 'jp':
            df = pd.read_csv('/home/cc/queries_cleaned1/' + filename + '.csv')
            skip = False
            for colname in df.columns:
                if 'Unnamed' in colname:
                    skip = True
                    break
            if not skip:
                l.append(filename)
    sampled = random.sample(l, n)
    print(sampled)
    with open('/home/cc/generality_experiment/selected_queries.txt', 'w') as f:
        for el in sampled:
            f.write(el + '\n')

def copy_sampled_queres():
    with open('/home/cc/generality_experiment/selected_queries.txt') as f:
        for jp in f:
            jp = jp.strip()
            shutil.copyfile('/home/cc/queries_cleaned1/' + jp + '.csv', '/home/cc/generality_experiment/selected_queries/' + jp + '.csv')

def _create_keyword_queries():
    output_path = '/home/cc/generality_experiment/queries_keyword/'
    base_query_path = '/home/cc/generality_experiment/selected_queries/'
    for filename in os.listdir(base_query_path):
        df = pd.read_csv(base_query_path + filename)
        data = []
        for (_, col_data) in df.iteritems():
            data.extend(col_data.values.tolist())
        keywords = random.sample(data, 3)
        df = pd.DataFrame({"keywords": keywords})
        df.to_csv('{}/{}.csv'.format(output_path, filename[:-4]), index=False)
       
def create_keyword_queries():
    output_path = '/home/cc/generality_experiment/queries_keyword/'
    join_paths = pd.read_csv('/home/cc/join_path_discovery/network_sampled_05_1.csv')
    base_query_path = '/home/cc/generality_experiment/selected_queries/'
    for filename in os.listdir(base_query_path):
        jp_num = int(filename[3:-4])
        row = join_paths.iloc[jp_num]
        print(row)
        df = pd.read_csv(base_query_path + filename)
        gt_index = random.sample([1, 2], 1)[0]
        if gt_index == 1:
            gt_table = row['tbl1']
            samples = df.iloc[:, 0]
        else:
            gt_table = row['tbl2']
            samples = df.iloc[:, 1]
        
        df = pd.DataFrame({"keywords": samples})
        df.to_csv('{}/{}.csv'.format(output_path, filename[:-4]), index=False)
        with open('{}/{}.txt'.format(output_path, filename[:-4]), 'w') as f:
            f.write(gt_table)

def create_table_queries():
    output_path = '/home/cc/generality_experiment/queries_table/'
    join_paths = pd.read_csv('/home/cc/join_path_discovery/network_sampled_05_1.csv')
    base_query_path = '/home/cc/generality_experiment/selected_queries/'
    for filename in os.listdir(base_query_path):
        jp_num = int(filename[3:-4])
        row = join_paths.iloc[jp_num]
        tables = [row['tbl1'], row['tbl2']]
        sampled_table = random.sample(tables, 1)
        with open(output_path + filename[0:-4] + '.txt', 'w') as f:
            f.write(sampled_table[0])

def create_attr_queries():
    output_path = '/home/cc/generality_experiment/queries_attr/'
    base_query_path = '/home/cc/generality_experiment/selected_queries/'
    for filename in os.listdir(base_query_path):
        df = pd.read_csv(base_query_path + filename)
        attrs = df.columns
        attrs = [attr[:-2] for attr in attrs]
        attr = random.sample(attrs, 1)[0]
        with open(output_path + filename[0:-4] + '.txt', 'w') as f:
            f.write(attr)

# create_keyword_queries()
# query_count = rank_queries_by_join_path_num()
# print(query_count)
create_attr_queries()

# copy_sampled_queres()
# sample_queries(10)
# classify_queries()
#  sample_n_queries(18)