import json
import os
from view_rank import get_S4_score
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def get_agg_plot():
    agg_dict = dict()
    agg_dict["S4-pipeline"] = 0
    agg_dict["Squid-pipeline"] = 0
    agg_dict["DoD-pipeline"] = 0

    for i in range(6):
        gt = "/home/cc/experiments_chembl_5_9/chembl_gt" + str(i) + "/"
        for noise_level, noise in enumerate(["zero_noise", "mid_noise", "high_noise"]):
            noise_path = gt + noise + "/hit.txt"
            log = open(noise_path, 'r')
            line = log.readlines()[0]
            tokens = line.split(",")
            m1_hit = int(tokens[0].split(":")[1])
            m2_hit = int(tokens[1].split(":")[1])
            m3_hit = int(tokens[2].split(":")[1][0:-1])

            agg_dict["S4-pipeline"] += m1_hit
            agg_dict["Squid-pipeline"] += m2_hit
            agg_dict["DoD-pipeline"] += m3_hit
    print(agg_dict)

    fig = plt.figure(figsize=(10, 5))

    methods = list(agg_dict.keys())
    hits = list(agg_dict.values())
    # creating the bar plot
    plt.bar(methods, hits, color='maroon',
            width=0.4)

    plt.xlabel("Courses offered")
    plt.ylabel("No. of students enrolled")
    plt.title("Students enrolled in different courses")
    plt.show()

def get_mrr_s4():
    data = [[0 for _ in range(18)] for _ in range(3)]
    for i in range(5):
        gt = "/home/cc/experiments_chembl_5_9/chembl_gt" + str(i) + "/"
        print(gt)
        for noise_level, noise in enumerate(["zero_noise", "mid_noise", "high_noise"]):
            print(noise)
            noise_path = gt + noise + "/"
            view_dict = dict()
            found_cnt = dict()
            view_dict["m1"] = 0
            view_dict["m2"] = 0
            view_dict["m3"] = 0
            found_cnt["m1"] = 5
            found_cnt["m2"] = 5
            found_cnt["m3"] = 5
            for j in range(5):
                sample_path = noise_path + "sample" + str(j) + "/"
                for k in range(1, 4):
                    result_path = sample_path + "result" + str(k) + "/log.txt"
                    log = open(result_path, 'r')
                    lines = log.readlines()
                    if lines[1].strip() == "not found ground truth":
                        found_cnt["m" + str(k)] -= 1
                        continue
                    view_idx = 0
                    view_score = []
                    for line in lines:
                        if "s4_score" in line:
                            if "dod_score" not in line:
                                score = float(line.split(":")[1].strip())
                            else:
                                score = float(line.split(",")[0].split(":")[1].strip())
                            view_score.append((view_idx, score))
                            view_idx += 1
                    # print(view_score)
                    gt_view = int(lines[-8].split(":")[1].split("view")[1].split(".csv")[0].strip())

                    view_score.sort(key=lambda x: x[1], reverse=True)
                    gt_rank = 0
                    for rank, x in enumerate(view_score):
                        if x[0] == gt_view:
                            gt_rank = rank + 1
                            # print('view' + str(x[0]) + '[gt]', x[1])
                            break
                    view_dict["m" + str(k)] += 1/gt_rank

            for k, v in view_dict.items():
                if found_cnt[k] == 0:
                    res = 0
                    # print(k, 0)
                else:
                    res = round(v / found_cnt[k], 3)
                    # print(k, round(v / found_cnt[k], 3))

                print (k, res)
                # data[int(k[-1]) - 1][6 * noise_level + i] = res

    # f = open("/home/cc/experiments_chembl_small_4/s4_mrr.txt", 'w')
    # for row in data:
    #     f.write(str(row)[1:-1] + "\n")
if __name__ == '__main__':
    get_agg_plot()
    # get_mrr_s4()
    # data = [[0 for _ in range(18)] for _ in range(3)]
    # for i in range(6):
    #     # gt = "/home/cc/experiments_chembl_small_3/chembl_gt" + str(i) + "/"
    #     gt = "/home/cc/experiments_chembl_small_4/chembl_gt" + str(i) + "/"
    #     # test = "/home/cc/test_chembl3/chembl_gt" + str(i) + ".json"
    #     # testcases = json.load(open(test))
    #     print(gt)
    #     for noise_level, noise in enumerate(["zero_noise", "mid_noise", "high_noise"]):
    #         print(noise)
    #         # samples = testcases[noise.split('_')[0]]
    #         noise_path = ""
    #         noise_path = gt + noise + "/"
    #         view_dict = dict()
    #         found_cnt = dict()
    #         view_dict["m1"] = 0
    #         view_dict["m2"] = 0
    #         view_dict["m3"] = 0
    #         found_cnt["m1"] = 5
    #         found_cnt["m2"] = 5
    #         found_cnt["m3"] = 5
    #         for j in range(5):
    #             sample_path = ""
    #             sample_path = noise_path + "sample" + str(j) + "/"
    #             # es = samples[j]
    #             for k in range(1, 4):
    #                 result_path = ""
    #                 result_path = sample_path + "result" + str(k) + "/log.txt"
    #                 log = open(result_path, 'r')
    #                 lines = log.readlines()
    #                 if lines[1].strip() == "not found ground truth":
    #                     found_cnt["m" + str(k)] -= 1
    #                     continue
    #                 n_views = int(lines[-9].split(":")[1].strip())
    #                 # gt_view = int(lines[-3].split(":")[1].split("view")[1].split(".csv")[0].strip())
    #                 print(result_path)
    #                 run_time = float(lines[-5].split(":")[1].strip())
    #                 relations = []
    #                 view_dict["m" + str(k)] += run_time
    #
    #         for k, v in view_dict.items():
    #             if found_cnt[k] == 0:
    #                 res = 0
    #                 print(k, 0)
    #             else:
    #                 res = round(v / found_cnt[k], 3)
    #                 print(k, round(v / found_cnt[k], 3))
    #             data[int(k[-1]) - 1][6 * noise_level + i] = res
    #
    # f = open("/home/cc/experiments_chembl_small_4/run_time.txt", 'w')
    # for row in data:
    #     f.write(str(row)[1:-1] + "\n")