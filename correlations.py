
import pandas as pd
import sys
import os


if __name__ == "__main__":
    read_path = "./data"
    out_dir = "./out"

    if len(sys.argv) > 1:
        read_path = sys.argv[1]
    if len(sys.argv) > 2:
        out_dir = sys.argv[2]

    grades_table = pd.read_csv(os.path.join(read_path, "LinkTables/Subject.csv"))

    for metric in ["EQ", "Watwin", "RED"]:
        path = os.path.join(out_dir, metric + ".csv")
        if not os.path.isfile(path):
            continue
        print("Found: " + path)
        metric_table = pd.read_csv(path)
        grades_table = grades_table.merge(metric_table, on=["SubjectID"])

    pearson = grades_table.drop("SubjectID", axis=1).corr(method="pearson")
    print(pearson)
    pearson.to_csv(os.path.join(out_dir, "corr_pearson.csv"))

    pearson = grades_table.drop("SubjectID", axis=1).corr(method="spearman")
    print(pearson)
    pearson.to_csv(os.path.join(out_dir, "corr_spearman.csv"))
