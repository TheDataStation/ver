import json
import os
from view_rank import get_S4_score
if __name__ == '__main__':
    data = [[0 for _ in range(18)] for _ in range(3)]
    for i in range(6):
        gt = "/home/cc/experiments_chembl/chembl_gt" + str(i) + "/"
        test = "/home/cc/tests_chembl/chembl_gt" + str(i) + ".json"
        testcases = json.load(open(test))
        print(gt)
        for noise_level, noise in enumerate(["zero_noise", "mid_noise", "high_noise"]):
            print(noise)
            samples = testcases[noise.split('_')[0]]
            noise_path = ""
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
                sample_path = ""
                sample_path = noise_path + "sample" + str(j) + "/"
                es = samples[j]
                for k in range(1, 4):
                    result_path = ""
                    result_path = sample_path + "result" + str(k) + "/log.txt"
                    log = open(result_path, 'r')
                    lines = log.readlines()
                    if lines[1].strip() == "not found ground truth":
                        found_cnt["m" + str(k)] -= 1
                        continue
                    n_views = int(lines[-4].split(":")[1].strip())
                    gt_view = int(lines[-3].split(":")[1].split("view")[1].split(".csv")[0].strip())
                    relations = []
                    view_dict["m" + str(k)] += 1/(gt_view + 1)

            for k, v in view_dict.items():
                if found_cnt[k] == 0:
                    res = 0
                    print(k, 0)
                else:
                    res = round(v / found_cnt[k], 3)
                    print(k, round(v / found_cnt[k], 3))
                data[int(k[-1]) - 1][6 * noise_level + i] = res

    f = open("/home/cc/experiments_chembl/dod_rank.txt", 'w')
    for row in data:
        f.write(str(row)[1:-1] + "\n")