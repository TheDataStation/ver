import json
import os
from view_rank import get_S4_score
if __name__ == '__main__':
    for i in range(6):
        gt = "/home/cc/experiments/chembl_gt" + str(i) + "/"
        gt_2 = "/home/cc/s4_score/chembl_gt" + str(i) + "/"
        test = "/home/cc/tests3/chembl_gt" + str(i) + ".json"
        testcases = json.load(open(test))
        print(gt)
        for noise in ["zero_noise", "mid_noise", "high_noise"]:
            print(noise)
            samples = testcases[noise.split('_')[0]]
            noise_path = ""
            noise_path = gt + noise + "/"
            noise_path_2 = gt_2 + noise + "/"
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
                sample_path_2 = noise_path_2 + "sample" + str(j) + "/"
                es = samples[j]
                for k in range(1, 4):
                    result_path = ""
                    result_path = sample_path + "result" + str(k) + "/log.txt"
                    result_path_2 = sample_path_2
                    log = open(result_path, 'r')
                    lines = log.readlines()
                    if lines[1].strip() == "found ground truth":
                        found_cnt["m" + str(k)] -= 1
                        continue
                    if not os.path.exists(os.path.dirname(result_path_2)):
                        try:
                            os.makedirs(os.path.dirname(result_path_2))
                        except OSError as exc:  # Guard against race condition
                            if exc.errno != errno.EEXIST:
                                raise
                    s4_score = open(result_path_2 + "/s4_score_" + "pipeline" + str(k) + ".txt", "w")

                    # if lines[1].strip() == "not found ground truth":
                    #     found_cnt["m" + str(k)] -= 1
                    #     continue
                    n_views = int(lines[-4].split(":")[1].strip())
                    gt_view = int(lines[-3].split(":")[1].split("view")[1].split(".csv")[0].strip())
                    relations = []
                    for line in lines:
                        if "#relations" in line:
                            relations.append(int(line.split(":")[1].strip()))
                    view_score = []
                    for m in range(n_views):
                        view_name = "view_" + str(m) + ".csv"
                        view_path = sample_path + "result" + str(k) + "/" + view_name
                        score = get_S4_score(view_path, es, relations[m])
                        s4_score.write(view_name + " " + str(round(score, 3)) + "\n")
                        view_score.append((m, score))
                    # view_score.sort(key=lambda x: x[1], reverse=True)
                    # gt_rank = 0
                    # for rank, x in enumerate(view_score):
                    #     if x[0] == gt_view:
                    #         gt_rank = rank + 1
                    #         # print('view' + str(x[0]) + '[gt]', x[1])
                    #         break
                    # view_dict["m" + str(k)] += 1/gt_rank

            for k, v in view_dict.items():
                if found_cnt[k] == 0:
                    print(k, 0)
                else:
                    print(k, round(v / found_cnt[k], 3))