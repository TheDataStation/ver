import pandas as pd
import time
import os
import json

base_join_path_dir = '/home/cc/generality_experiment/views_table/'
output_base_dir = '/home/cc/generality_experiment/views_table/'
# base_join_path_dir = '/home/cc/generality_experiment/views_keyword/'
# output_base_dir = '/home/cc/generality_experiment/views_keyword/'
base_path = '/home/cc/opendata_cleaned/'
view_cache = {}

def load_join_paths(path):
    join_paths = []
    df = pd.read_csv(path)
    print(len(df.index))
    for row in df.itertuples(index=False):
        join_paths.append((row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
        # join_paths.append((row['tbl1'], row['col1'], row['tbl2'], row['col2'], row['proj1'], row['proj2'], row['num_relation']))
    return join_paths


def read_table(tbl):
    io_time = 0
    if tbl in view_cache:
        print("Hit Cache!")
        df = view_cache[tbl]
    else:
        io_start = time.time()
        df = pd.read_csv(base_path + tbl)
        io_time = time.time() - io_start
        if len(df.index) > 1000:
            df = df.sample(1000)
        view_cache[tbl] = df
    return df, io_time

def save_csv(proj_view, output_path, i):
    start = time.time()
    view_path = output_path + "/view_" + str(i) + ".csv"
    proj_view.to_csv(view_path, encoding='utf8', index=False)
    return time.time() - start

def materialize_single_table(tbl1, proj1, proj2, i, output_path):
    df, read_time = read_table(tbl1)
    # proj_view = dpu.project(df, [proj1, proj2])
    # proj_view = proj_view.drop_duplicates().head(2000)
    # save_time = save_csv(proj_view, output_path, i)
    save_time = save_csv(df.drop_duplicates().head(2000), output_path, i)
    return save_time + read_time

def join_two_tables_old(tbl1, col1, tbl2, col2, proj1, proj2, i, output_path):
    io_time = 0
    print("reading tables")

    df1, read_time = read_table(tbl1)
    df2, read_time = read_table(tbl2)

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
    if col1 == col2:
        merged = df1.merge(df2.dropna(subset=[col1]), how='inner', on=col1)
    else:
        merged = df1.merge(df2.dropna(subset=[col2]), how='inner', left_on=col1, right_on=col2)
    print("finish join")
    if len(merged.index) == 0:
        print("empty view!")
        df1.columns, df2.columns = old_col_names1, old_col_names2
        return io_time, False
    save_time = save_csv(merged[[col1, col2]], output_path, i)
    df1.columns, df2.columns = old_col_names1, old_col_names2
    
    return io_time, True

def join_two_tables(tbl1, col1, tbl2, col2, proj1, proj2, i, output_path):
    io_time = 0
    print("reading tables")
    df1, read_time = read_table(tbl1)
    io_time += read_time
    
    df_l = df1.dropna(subset=[col1]).drop_duplicates(subset=[col1])
    # df_l = dpu.project(df1, set([proj1, col1])).dropna().drop_duplicates()
    
    df2, read_time = read_table(tbl2)
    io_time += read_time
    df_r = df2.dropna(subset=[col2]).drop_duplicates(subset=[col2])
    # df_r = dpu.project(df2, set([proj2, col2])).dropna().drop_duplicates()
    if len(df_l.index) == 0 or len(df_r.index) == 0:
        print("empty view!")
        return io_time, False
    
    col_names1, col_names2 = df_l.columns, df_r.columns
    df_l.columns = [s + '_x' for s in col_names1]
    df_r.columns = [s + '_y' for s in col_names2]
    col1 = col1 + '_x'
    col2 = col2 + '_y'
    df_l[col1] = df_l[col1].astype(str)
    df_r[col2] = df_r[col2].astype(str)
    print("begin join")
    merged = df_l.merge(df_r, how='inner', left_on=col1, right_on=col2, suffixes=('_x', '_y'))

    if len(merged.index) == 0:
        print("empty view!")
        return io_time, False

    # res = merged[[proj1 + '_x', proj2 + '_y']]
    res = merged
    print("finish join")
    save_time = save_csv(res, output_path, i)
    return io_time + save_time, True


def materialize_join_paths(join_paths, output_path):
    total_io_time = 0
    total, cnt = len(join_paths), 0
    for idx, join_path in enumerate(join_paths):
        print("progress: {}\{}".format(idx, total))
        print(join_path)
        tbl1, col1, tbl2, col2, proj1, proj2, num_relations = join_path
        if pd.isna(tbl2) and pd.isna(col2):
            io_time = materialize_single_table(tbl1, proj1, proj2, idx, output_path)
            total_io_time += io_time
            cnt += 1
        else:
            io_time, not_empty = join_two_tables(tbl1, col1, tbl2, col2, proj1, proj2, idx, output_path)
            total_io_time += io_time
            if not_empty:
                cnt += 1
        print("views generated:", cnt)
    return cnt, total_io_time


def materialize_one_query(jp_name):
    profile = {"materialization": 0, "io": 0, "view_count": 0}
    join_path_file = base_join_path_dir + jp_name + '/join_paths.csv'
    join_paths = load_join_paths(join_path_file)
    output_path = output_base_dir + jp_name + '/views/'
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    start = time.time()
    view_cnt, total_io_time = materialize_join_paths(join_paths, output_path)
    total_time = time.time() - start
    profile["materialization"] = total_time
    profile["io"] = total_io_time
    profile["view_count"] = view_cnt
    with open('{}/run_time_join.json'.format(output_path), 'w') as json_file:
        json.dump(profile, json_file)


def begin_materialization(file_path):
    with open(file_path) as f:
        for line in f:
            jp_name = line.split(',')[0].strip()
            materialize_one_query(jp_name)

def begin_materialization2():
    for filename in os.listdir(base_join_path_dir):
        if filename[0:2] != 'jp':
            continue
        print("processing", filename)
        materialize_one_query(filename)

# materialize_one_query('jp_710')
# begin_materialization(base_join_path_dir + 'under_100k_sampled.txt')
begin_materialization2()