from DoD import data_processing_utils as dpu
from DoD.evaluation.groundTruth import GroundTruth, getGroundTruths
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

def gen_tests(base_path, gt: GroundTruth):
    dpu.configure_csv_separator(';')
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
        values = [[] for _ in range(samples_per_col)]
        for j in range(len(zeros_noise_data)):
            data = random.sample(zeros_noise_data[j], samples_per_col)
            for idx, value in enumerate(values):
                value.append(data[idx])
        zeros_noise_samples.append(values)

    # medium noise: sample 4 values from ground truth and sample 2 values from noise columns:
    med_noise_samples = []
    noise_data = [gt.noise1, gt.noise2]

    for i in range(sample_num):
        values = [[] for _ in range(samples_per_col)]
        for j in range(samples_per_row):
            data = random.sample(zeros_noise_data[j], 2)
            data.extend(random.sample(noise_data[j], 1))
            for idx, value in enumerate(values):
                value.append(data[idx])
        med_noise_samples.append(values)


    # high noise: sample 2 values from ground truth and sample 4 values from noise columns:
    high_noise_samples = []
    for i in range(sample_num):
        values = [[] for _ in range(samples_per_col)]
        for j in range(samples_per_row):
            data = random.sample(zeros_noise_data[j], 1)
            data.extend(random.sample(noise_data[j], 2))
            for idx, value in enumerate(values):
                value.append(data[idx])
        high_noise_samples.append(values)

    return zeros_noise_samples, med_noise_samples, high_noise_samples

if __name__ == '__main__':
    data = json.load(open("/Users/gongyue/aurum-datadiscovery/DoD/evaluation/tests/chembl_gt0.json"))
    print(data["table1"])
    # gts = getGroundTruths()
    # for (idx, gt) in enumerate(gts):
    #     print(idx)
    #     data = dict()
    #     x, y, z = gen_tests("/Users/gongyue/data/chemBL/", gt)
    #     data["table1"] = gt.table1
    #     data["attr1"] = gt.attr1
    #     data["table2"] = gt.table2
    #     data["attr2"] = gt.attr2
    #     data["jp"] = gt.join_path
    #     data["zero"] = x
    #     data["mid"] = y
    #     data["high"] = z
    #     # json.dump(data, open("chembl_gt"+str(idx)+".json", 'w'))