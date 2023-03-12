from archived.DoD import data_processing_utils as dpu
import numpy as np


def read_view(path):
    df = dpu.read_relation_on_copy_renamed(path, ',', ["col1", "col2"], 2000)
    return df


def row_row_similarity(t, r):
    score = 0
    for i in range(len(t)):
        tokens1 = set(str(t[i]).split())
        tokens2 = set(str(r[i]).split())
        score += len(tokens1.intersection(tokens2)) / len(tokens1.union(tokens2))
    return score


def row_wise_score(es, view):
    score = 0
    for row1 in es:
        max_r_score = 0
        for row2 in view:
            rr_score = row_row_similarity(row1, row2)
            if rr_score > max_r_score:
                max_r_score = rr_score
        score += max_r_score
    return score


def column_wise_score(es, view):
    score = 0
    for j in range(len(es[0])):
        for i in range(len(es)):
            v = es[i][j]
            col = view[j]
            max_cell_score = 0
            for x in col:
                tokens1 = set(str(v).split())
                tokens2 = set(str(x).split())
                cell_score = len(tokens1.intersection(tokens2)) / len(tokens1.union(tokens2))
                if cell_score > max_cell_score:
                    max_cell_score = cell_score
            score += max_cell_score
    return score


def get_S4_score(view_path, es, relations):
    df = read_view(view_path)
    view_rows = []
    view_cols = []
    for i, row in df.iterrows():
        view_rows.append(row.values.tolist())
    for col in df:
        view_cols.append(df[col].values.tolist())
    row_score = row_wise_score(es, view_rows)
    col_score = column_wise_score(es, view_cols)
    score = (0.5 * row_score + 0.5 * col_score) / 1 + np.log(1 + np.log(relations))
    return score

def get_S4_score_direct(df, es, relations):
    view_rows = []
    view_cols = []
    for i, row in df.iterrows():
        view_rows.append(row.values.tolist())
    for col in df:
        view_cols.append(df[col].values.tolist())
    row_score = row_wise_score(es, view_rows)
    col_score = column_wise_score(es, view_cols)
    score = (0.5 * row_score + 0.5 * col_score) / 1 + np.log(1 + np.log(relations))
    return score

if __name__ == '__main__':
    es = [['Homo sapiens', 'Doxorubicin'], ['Mus musculus', 'Ciprofloxacin'], ['Rattus norvegicus', 'Chloroquine']]

    groundTruth = [11]
    view_score = []
    for index in range(13):
        view_rows = []
        view_cols = []
        alpha = 0.5
        df = read_view("/Users/gongyue/aurum-datadiscovery/test/chemblResult/result4/view_" + str(index) + ".csv")
        for i, row in df.iterrows():
            view_rows.append(row.values.tolist())
        for col in df:
            view_cols.append(df[col].values.tolist())
        row_score = row_wise_score(es, view_rows)
        col_score = column_wise_score(es, view_cols)
        score = 0.5 * row_score + 0.5 * col_score
        view_score.append((index, score))
    view_score.sort(key=lambda x: x[1], reverse=True)
    gt_rank = 0
    for rank, x in enumerate(view_score):
        if x[0] in groundTruth:
            gt_rank = rank + 1
            print('view' + str(x[0]) + '[gt]', x[1])
        else:
            print('view' + str(x[0]), x[1])
    print("gt_rank", gt_rank)