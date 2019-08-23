import pandas as pd
import sys
import os
from scipy.stats import pearsonr
from scipy.stats import spearmanr


def calculate_correlation_pvalues(df, fn=pearsonr):
    df = df.dropna()._get_numeric_data()
    df_cols = pd.DataFrame(columns=df.columns)
    p_values = df_cols.transpose().join(df_cols, how='outer')
    for r in df.columns:
        for c in df.columns:
            p_values[r][c] = fn(df[r], df[c])[1]
    return p_values


if __name__ == "__main__":
    read_path = "./data"
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

    metrics = grades_table.drop("SubjectID", axis=1)

    pearson = metrics.corr(method="pearson")
    print(pearson)
    pearson.to_csv(os.path.join(out_dir, "corr_pearson.csv"))
    calculate_correlation_pvalues(metrics).to_csv(os.path.join(out_dir, "corr_pearson_p.csv"))

    spearman = grades_table.drop("SubjectID", axis=1).corr(method="spearman")
    print(spearman)
    spearman.to_csv(os.path.join(out_dir, "corr_spearman.csv"))
    calculate_correlation_pvalues(metrics, spearmanr).to_csv(os.path.join(out_dir, "corr_spearman_p.csv"))

    print(count_dict)
    with open(os.path.join(out_dir, "counts.txt"), 'w') as file:
        file.write(str(count_dict))

    # data_table = grades_table.drop("SubjectID", axis=1)
    # x = data_table.iloc[:, 0]
    # y = data_table.iloc[:, 1]
    # r_squared = stats.linregress(x, y)[2] ** 2
    # print(r_squared)
    # r_squared.to_csv(os.path.join(out_dir, "r_squared.csv"))

