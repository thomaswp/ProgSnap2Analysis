import pandas as pd
import sys
import os
# from scipy import stats


if __name__ == "__main__":
    read_path = "./data/CloudCoder"
    out_dir = "./out"

    if len(sys.argv) > 1:
        read_path = sys.argv[1]
    if len(sys.argv) > 2:
        out_dir = sys.argv[2]

    grades_table = pd.read_csv(os.path.join(read_path, "LinkTables/Subject.csv"))
    count_dict = {}
    count_dict["grades"] = grades_table.shape[0]

    for metric in ["EQ", "Watwin", "RED"]:
        path = os.path.join(out_dir, metric + ".csv")
        if not os.path.isfile(path):
            continue
        print("Found: " + path)
        metric_table = pd.read_csv(path)
        count_dict[metric] = metric_table.shape[0]
        grades_table = grades_table.merge(metric_table, on=["SubjectID"])
    count_dict["Merged"] = grades_table.shape[0]

    pearson = grades_table.drop("SubjectID", axis=1).corr(method="pearson")
    print(pearson)
    pearson.to_csv(os.path.join(out_dir, "corr_pearson.csv"))

    spearman = grades_table.drop("SubjectID", axis=1).corr(method="spearman")
    print(spearman)
    spearman.to_csv(os.path.join(out_dir, "corr_spearman.csv"))

    print(count_dict)
    with open(os.path.join(out_dir, "counts.txt"), 'w') as file:
        file.write(str(count_dict))

    # data_table = grades_table.drop("SubjectID", axis=1)
    # x = data_table.iloc[:, 0]
    # y = data_table.iloc[:, 1]
    # r_squared = stats.linregress(x, y)[2] ** 2
    # print(r_squared)
    # r_squared.to_csv(os.path.join(out_dir, "r_squared.csv"))
