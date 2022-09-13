import json
import os
from view_rank import get_S4_score

def correct_gt_view():
    for i in range(1):
        gt = "/home/cc/experiments_chembl_5_4/chembl_gt" + str(i) + "/"
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
                    for

if __name__ == '__main__':
    get_mrr_s4()