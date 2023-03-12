from archived.DoD import data_processing_utils as dpu
from archived.DoD import GroundTruth, GroundTruth_extend, getGroundTruths_Chembl, getGroudTruths_Chembl_Extend
import random
import json

def read_view(path):
    df = dpu.read_relation_on_copy_renamed(path, ',', ["v"], 2000)
    return df

def generate_tests():
    dpu.configure_csv_separator(';')
    # zero noise: sample values from columns that construct the ground truth view
    zeros_noise_samples = []
    sample_num = 10
    samples_per_col = 3
    total_samples = sample_num*samples_per_col
    path1 = "/Users/gongyue/data/chemBL/public.assays.csv"
    df1 = dpu.read_column(path1, "assay_organism")
    path2 = "/Users/gongyue/data/chemBL/public.compound_records.csv"
    df2 = dpu.read_column(path2, "compound_name")

    zeros_noise_data = []
    zeros_noise_data.append(df1['assay_organism'].dropna().drop_duplicates().values.tolist())
    zeros_noise_data.append(df2['compound_name'].dropna().drop_duplicates().values.tolist())

    for i in range(sample_num):
        values = []
        for j in range(i*samples_per_col, (i+1)*samples_per_col):
            values.append([zeros_noise_data[0][j], zeros_noise_data[1][j]])
        zeros_noise_samples.append(values)

    # medium noise: sample 4 values from ground truth and sample 2 values from noise columns:
    med_noise_samples = []
    noise_path1 = "/Users/gongyue/data/groundTruth/noise1.csv"
    noise_path2 = "/Users/gongyue/data/groundTruth/noise2.csv"
    df_noise1 = dpu.read_relation_on_copy2(noise_path1, ',')
    df_noise2 = dpu.read_relation_on_copy2(noise_path2, ',')
    noise_data1 = df_noise1['v'].values.tolist()
    noise_data2 = df_noise2['v'].values.tolist()
    for i in range(sample_num):
        values = []
        cnt = 0
        for j in range(i * samples_per_col, (i + 1) * samples_per_col):
            if cnt < 2:
                values.append([zeros_noise_data[0][j], zeros_noise_data[1][j]])
            else:
                values.append([noise_data1[j], noise_data2[j]])
            cnt += 1
        med_noise_samples.append(values)

    # high noise: sample 2 values from ground truth and sample 4 values from noise columns:
    high_noise_samples = []
    for i in range(sample_num):
        values = []
        cnt = 0
        for j in range(i * samples_per_col, (i + 1) * samples_per_col):
            if cnt < 1:
                values.append([zeros_noise_data[0][j], zeros_noise_data[1][j]])
            else:
                values.append([noise_data1[j], noise_data2[j]])
            cnt += 1
        high_noise_samples.append(values)

    return zeros_noise_samples, med_noise_samples, high_noise_samples

def gen_test_zero_noise_diff_size(gt: GroundTruth):
    zero_noise_samples = []
    zero_noise_data = [gt.zero_noise1, gt.zero_noise2]
    samples = []
    for _ in range(5):
        values = [[] for _ in range(2)]
        for j in range(len(zero_noise_data)):
            data = random.sample(zero_noise_data[j], 2)
            for idx, value in enumerate(values):
                value.append(data[idx])
        samples.extend(values)
        print(len(samples))
        zero_noise_samples.append(samples[:])
    return zero_noise_samples

def run_diff_size():
    gts = getGroundTruths_Chembl()
    for (idx, gt) in enumerate(gts):
        print(idx)
        data = dict()
        x = gen_test_zero_noise_diff_size(gt)
        data["table1"] = gt.table1
        data["attr1"] = gt.attr1
        data["table2"] = gt.table2
        data["attr2"] = gt.attr2
        data["jp"] = gt.join_path
        data["zero"] = x
        json.dump(data, open("/home/cc/tests_chembl_diff_sample_size_3/chembl_gt" + str(idx) + ".json", 'w'))

def gen_test_diff_column_size(gt: GroundTruth_extend):
    zero_noise_data = gt.get_data()
    print("column:", len(zero_noise_data))
    samples_per_col = 3
    values = [[] for _ in range(samples_per_col)]
    for j in range(len(zero_noise_data)):
        data = random.sample(zero_noise_data[j], samples_per_col)
        for idx, value in enumerate(values):
            value.append(data[idx])
    print(values)
    return values

def run_test_diff_column_size():
    gts = getGroudTruths_Chembl_Extend()
    data = dict()
    data["zero"] = []
    for (idx, gt) in enumerate(gts):
        print(idx)
        x = gen_test_diff_column_size(gt)
        data["zero"].append(x)
    json.dump(data, open("/home/cc/tests_chembl_diff_col_size/chembl_gt" + str(0) + ".json", 'w'))

def gen_tests(base_path, gt: GroundTruth):
    dpu.configure_csv_separator(',')
    # zero noise: sample values from columns that construct the ground truth view
    zeros_noise_samples = []
    sample_num = 5
    samples_per_row = 2
    samples_per_col = 3
    total_samples = sample_num * samples_per_col
    # path1 = base_path + gt.table1
    # df1 = dpu.read_column(path1, gt.attr1)
    # path2 = base_path + gt.table2
    # df2 = dpu.read_column(path2, gt.attr2)

    zeros_noise_data = [gt.zero_noise1, gt.zero_noise2]
    # zeros_noise_data.append(df1[gt.attr1].dropna().drop_duplicates().values.tolist())
    # zeros_noise_data.append(df2[gt.attr2].dropna().drop_duplicates().values.tolist())
    # zeros_noise_data.append(gt.zero_noise1)
    # zeros_noise_data.append(gt.zero_noise2)

    for i in range(sample_num):
        values = [[] for _ in range(samples_per_col)]
        for j in range(len(zeros_noise_data)):
            data = random.sample(zeros_noise_data[j], samples_per_col)
            for idx, value in enumerate(values):
                value.append(data[idx])
        zeros_noise_samples.append(values)

    # medium noise: sample 6 values from ground truth and sample 2 values from noise columns:
    med_noise_samples = []
    noise_data = [gt.noise1, gt.noise2]
    sample_cnt = [0, 0]
    noise_cnt  = 2
    if len(noise_data[0]) == 0:
        print("noise 1 is none")
        print("noise 2", len(noise_data[1]))
        sample_cnt[0] = 0
        sample_cnt[1] = 1
    elif len(noise_data[1]) == 0:
        print("noise 1", len(noise_data[0]))
        print("noise 2 is none")
        sample_cnt[0] = 1
        sample_cnt[1] = 0
    else:
        sample_cnt[0] = 1
        sample_cnt[1] = 1
    for i in range(sample_num):
        values = [[] for _ in range(samples_per_col)]
        for j in range(samples_per_row):
            if len(noise_data[j]) == 0:
                data = random.sample(zeros_noise_data[j], samples_per_col)
            else:
                data = random.sample(zeros_noise_data[j], samples_per_col - sample_cnt[j])
                print("noise", j, len(noise_data[j]), sample_cnt[j])
                data.extend(random.sample(noise_data[j], sample_cnt[j]))
            for idx, value in enumerate(values):
                value.append(data[idx])
        med_noise_samples.append(values)


    # high noise: sample 2 values from ground truth and sample 4 values from noise columns:
    sample_cnt = [0, 0]
    noise_cnt = 4
    if len(noise_data[0]) == 0:
        sample_cnt[0] = 0
        sample_cnt[1] = 2
    elif len(noise_data[1]) == 0:
        sample_cnt[0] = 2
        sample_cnt[1] = 0
    else:
        sample_cnt[0] = 2
        sample_cnt[1] = 2
    high_noise_samples = []
    for i in range(sample_num):
        values = [[] for _ in range(samples_per_col)]
        for j in range(samples_per_row):
            if len(noise_data[j]) == 0:
                data = random.sample(zeros_noise_data[j], samples_per_col)
            else:
                data = random.sample(zeros_noise_data[j], samples_per_col - sample_cnt[j])
                print("noise", j, len(noise_data[j]), sample_cnt[j])
                data.extend(random.sample(noise_data[j], sample_cnt[j]))
            for idx, value in enumerate(values):
                value.append(data[idx])
        high_noise_samples.append(values)

    return zeros_noise_samples, med_noise_samples, high_noise_samples

def gen_tests2(base_path, gt: GroundTruth):
    dpu.configure_csv_separator(',')
    # zero noise: sample values from columns that construct the ground truth view
    zeros_noise_samples = []
    sample_num = 5
    samples_per_row = 2
    samples_per_col = 3
    total_samples = sample_num * samples_per_col
    path1 = base_path + gt.table1
    df1 = dpu.read_column(path1, gt.attr1)
    path2 = base_path + gt.table2
    df2 = dpu.read_column(path2, gt.attr2)

    zeros_noise_data = []
    zeros_noise_data.append(df1[gt.attr1].dropna().drop_duplicates().values.tolist())
    zeros_noise_data.append(df2[gt.attr2].dropna().drop_duplicates().values.tolist())

    for i in range(sample_num):
        values = []
        for j in range(i*samples_per_col, (i+1)*samples_per_col):
            values.append([zeros_noise_data[0][j], zeros_noise_data[1][j]])
        zeros_noise_samples.append(values)

    # medium noise: sample 6 values from ground truth and sample 2 values from noise columns:
    med_noise_samples = []
    noise_data = [gt.noise1, gt.noise2]

    for i in range(sample_num):
        values = []
        cnt = 0
        for j in range(i * samples_per_col, (i + 1) * samples_per_col):
            if cnt < samples_per_col-1:
                values.append([zeros_noise_data[0][j], zeros_noise_data[1][j]])
            else:
                values.append([noise_data[0][j], noise_data[1][j]])
            cnt += 1
        med_noise_samples.append(values)

    # high noise: sample 2 values from ground truth and sample 4 values from noise columns:
    high_noise_samples = []
    for i in range(sample_num):
        values = []
        cnt = 0
        for j in range(i * samples_per_col, (i + 1) * samples_per_col):
            if cnt < samples_per_col-2:
                values.append([zeros_noise_data[0][j], zeros_noise_data[1][j]])
            else:
                values.append([noise_data[0][j], noise_data[1][j]])
            cnt += 1
        high_noise_samples.append(values)

    return zeros_noise_samples, med_noise_samples, high_noise_samples


if __name__ == '__main__':
    run_diff_size()
    # data = json.load(open("/home/cc/tests/chembl_gt0.json"))
    # print(data["zero"][3])
    # gts = getGroundTruths_Chembl()
    # # for (idx, gt) in enumerate(gts):
    # #     print(idx)
    # #     data = dict()
    # #     x, y, z = gen_tests("/home/cc/chembl/", gt)
    # #     # print(x)
    # #     # print("=================")
    # #     # print(y)
    # #     # print("=================")
    # #     # print(z)
    # #     # print("=================")
    # #     data["table1"] = gt.table1
    # #     data["attr1"] = gt.attr1
    # #     data["table2"] = gt.table2
    # #     data["attr2"] = gt.attr2
    # #     data["jp"] = gt.join_path
    # #     data["zero"] = x
    # #     data["mid"] = y
    # #     data["high"] = z
    # #     json.dump(data, open("/home/cc/tests_chembl_5_4/chembl_gt"+str(5+idx)+".json", 'w'))
    # for (idx, gt) in enumerate(gts):
    #     print(idx)
    #     data = dict()
    #     x = gen_test_zero_noise_diff_size(gt)
    #     data["table1"] = gt.table1
    #     data["attr1"] = gt.attr1
    #     data["table2"] = gt.table2
    #     data["attr2"] = gt.attr2
    #     data["jp"] = gt.join_path
    #     data["zero"] = x
    #     json.dump(data, open("/home/cc/tests_chembl_diff_sample_size_3/chembl_gt" + str(idx) + ".json", 'w'))