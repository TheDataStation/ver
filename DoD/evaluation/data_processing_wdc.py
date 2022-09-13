import json
import os
from view_rank import get_S4_score

def get_mrr_s4():
    # data = [[0 for _ in range(15)] for _ in range(3)]
    for i in range(1):
        gt = "/home/cc/experiments_wdc_10000_5_6/query_1" + "/"
        print(gt)
        for noise_level, noise in enumerate([""]):
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
            for j in range(1):
                # sample_path = noise_path + "sample" + str(j) + "/"
                for k in range(1, 4):
                    result_path = noise_path + "result" + str(k) + "/log.txt"
                    print(result_path)
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
                    print(view_score)
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
                    print(k, 0)
                else:
                    res = round(v / found_cnt[k], 3)
                    print(k, round(v / found_cnt[k], 3))
                print(k, v)
                # data[int(k[-1]) - 1][5 * noise_level + i] = res

    # f = open("/home/cc/experiments_wdc_10000_2/s4_mrr.txt", 'w')
    # for row in data:
    #     f.write(str(row)[1:-1] + "\n")

if __name__ == '__main__':
    get_mrr_s4()
    # data = [[0 for _ in range(15)] for _ in range(3)]
    # for i in range(5):
    #     # gt = "/home/cc/experiments_chembl_small_3/chembl_gt" + str(i) + "/"
    #     gt = "/home/cc/experiments_wdc_10000_2/wdc_gt" + str(i) + "/"
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
    #                 view_dict["m" + str(k)] += n_views
    #
    #         for k, v in view_dict.items():
    #             if found_cnt[k] == 0:
    #                 res = 0
    #                 print(k, 0)
    #             else:
    #                 res = round(v / found_cnt[k], 3)
    #                 print(k, round(v / found_cnt[k], 3))
    #             data[int(k[-1]) - 1][5 * noise_level + i] = res
    #
    # f = open("/home/cc/experiments_wdc_10000_2/run_time.txt", 'w')
    # for row in data:
    #     f.write(str(row)[1:-1] + "\n")