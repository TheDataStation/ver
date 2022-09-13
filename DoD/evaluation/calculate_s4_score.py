from view_rank import get_S4_score
import os 
import pandas as pd
import csv

view_join_path = '/home/cc/output_views_join_paths/'

join_paths = pd.read_csv('/home/cc/join_path_discovery/network_sampled_05_1.csv')

def load_join_paths(path):
    join_paths = []
    df = pd.read_csv(path)
    return join_paths

def read_example_values(path):
    df = pd.read_csv(path)
    columns = []
    for (col_name, col_data) in df.iteritems():
        columns.append(col_data.values.tolist()) 
    values = []
    for i in range(len(columns[0])):
        values.append([column[i] for column in columns])
    return values

def compute_s4_jp(jp_name, values, csv_writer):
    full_path = '/home/cc/output_views_small/' + jp_name + '/'
    join_path_dir = '/home/cc/output_views_join_paths/' + jp_name + '/join_paths.csv'
    
    df = pd.read_csv(join_path_dir)
    for view in os.listdir(full_path):
        print("scoring", view)
        if view[-3:] != 'csv':
            continue
        view_num = int(view[5:-4])
        rel_num = df.iloc[view_num]['num_relation']
        score = get_S4_score(full_path + view, values, rel_num)
        csv_writer.writerow([view, score])

def compute_s4_score():
    for filename in os.listdir('/home/cc/generality_experiment/selected_queries/'):
        print("processing", filename)
        jp_name = filename[:-4]
        values = read_example_values('/home/cc/generality_experiment/selected_queries/' + filename)
        output_path = '/home/cc/generality_experiment/s4_scores/' + jp_name + '_s4_score.csv'
        s4_file = open(output_path, 'w', encoding='UTF8', buffering=1)
        csv_writer = csv.writer(s4_file)
        csv_writer.writerow(['view', 'score'])
        compute_s4_jp(jp_name, values, csv_writer)

def _compute_s4_score(jp_name, view_base_path, query_base_path):
    for filename in os.listdir('/home/cc/generality_experiment/selected_queries/'):
        jp = filename[:-4]
        full_path = view_base_path + filename
        values = read_example_values('/home/cc/generality_experiment/selected_queries/' + filename)

        get_S4_score(full_path, values, )

compute_s4_score()