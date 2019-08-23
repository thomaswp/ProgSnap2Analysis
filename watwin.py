import pandas as pd
import numpy as np
import sys
import os
import datetime
from datetime import timedelta
import utils


def check_attr(main_table_df):
    # Check whether the dataset has required attributes, if not, pop-up warnings:
    counter = 0
    for required_attr in ["SubjectID", "Order", "EventType", "EventID", "CodeStateID", "ParentEventID",
                          "CompileMessageData", "CompileMessageType", "SourceLocation", "ServerTimestamp"]:
        if required_attr not in main_table_df:
            print("The dataset misses the attribute required: ", required_attr + " !")
            counter = 1
    if counter == 0:
        return True
    else:
        return False


def time_perp(main_table_df):
    # Watson(2013) doesn't state how they get mean and sd, we assume both mean and sd calculated from all compilation pairs
    # Initialization:
    time_arr = {}
    mean_dict = {}
    std_dict = {}

    for subj in set(main_table_df["SubjectID"]):
        current_df = main_table_df.loc[main_table_df["SubjectID"] == subj]
        current_df.sort_values(by=['Order'])
        compiles = current_df[current_df["EventType"] == "Compile"]
        compile_errors = current_df[current_df["EventType"] == "Compile.Error"]

        sum_time = 0
        count_time = 0

        if len(compiles) > 1:
            time_arr[subj] = {}
            for i in range(len(compiles) - 1):
                # Watson(2013) requires pair pruning, in which Remove identical pairs
                if compiles["CodeStateID"].iloc[i + 1] != compiles["CodeStateID"].iloc[i]:
                    e1_errors = compile_errors[compile_errors["ParentEventID"] == compiles["EventID"].iloc[i]]
                    e2_errors = compile_errors[compile_errors["ParentEventID"] == compiles["EventID"].iloc[i + 1]]
                    # If e1 compile resulted in error
                    if len(e1_errors) > 0:
                        # Watson(2013) requires time estimate preparation before calculating score, we assume no invocation reported in dataset, which means using time difference of compilcation pairs directly
                        datetimeFormat = '%Y-%m-%dT%H:%M:%S'
                        date1 = datetime.datetime.strptime(compiles["ServerTimestamp"].iloc[i + 1], datetimeFormat)
                        date2 = datetime.datetime.strptime(compiles["ServerTimestamp"].iloc[i], datetimeFormat)
                        time_diff = ((((date1.month - date2.month) * 30 + (date1.day - date2.day)) * 24 + (
                                    date1.hour - date2.hour)) * 60 + (date1.minute - date2.minute)) * 60 + (
                                                date1.second - date2.second)
                        sum_time += time_diff
                        count_time = count_time + 1
                        time_arr[subj][compiles["CodeStateID"].iloc[i]] = time_diff

        if count_time != 0:
            mean_time = sum_time / count_time
            mean_dict[subj] = mean_time
            std_time = np.std(np.asarray(list(time_arr[subj].values())))
            std_dict[subj] = std_time
        else:
            mean_time = 0
            mean_dict[subj] = mean_time
            std_time = 0
            std_dict[subj] = std_time

    return time_arr, mean_dict, std_dict


def calculate_watwin(main_table, subject_id):
    # Watson(2013) requires 1) deletion fixes 2) commented fixes during data preparation 3) error message generalization, we assume the dataset has fulfilled this requirement
    subject_events = main_table.loc[main_table["SubjectID"] == subject_id]
    subject_events.sort_values(by=['Order'])
    compiles = subject_events[subject_events["EventType"] == "Compile"]
    compile_errors = subject_events[subject_events["EventType"] == "Compile.Error"]

    if len(compiles) <= 1:
        return None

    # Begin calculate WatWin scores:
    score = 0
    pair_count = 0

    for i in range(len(compiles) - 1):
        # Only look at consecutive compiles within a single assignment/problem/session
        changed_segments = False
        for segment_id in ["SessionID", "ProblemID", "AssignmentID"]:
            if segment_id not in compiles:
                continue
            if compiles[segment_id].iloc[i] != compiles[segment_id].iloc[i + 1]:
                changed_segments = True
                break
        if changed_segments:
            continue

        pair_count += 1

        # Watson(2013) requires pair pruning, in which Remove identical pairs
        if compiles["CodeStateID"].iloc[i] != compiles["CodeStateID"].iloc[i + 1]:

            # Get all compile errors associated with compile events e1 and e2
            e1_errors = compile_errors[compile_errors["ParentEventID"] == compiles["EventID"].iloc[i]]
            e2_errors = compile_errors[compile_errors["ParentEventID"] == compiles["EventID"].iloc[i + 1]]

            # if former event has error
            if len(e1_errors) > 0:

                # if later event has error
                if len(e2_errors) > 0:

                    # Get the set of errors shared by both compiles
                    shared_errors = set(e1_errors["CompileMessageType"]).intersection(
                        set(e2_errors["CompileMessageType"]))

                    # if same full message
                    # We assume the attribute containing full message is CompileMessageData
                    e1_error_message = e1_errors["CompileMessageData"].iloc[0]
                    e2_error_message = e2_errors["CompileMessageData"].iloc[0]
                    if e1_error_message == e2_error_message:
                        score += 4
                        # if same error type
                    if len(shared_errors) > 0:
                        score += 4
                    # TODO: Watson (2013) requires for error line number of compiled code
                    # if same line
                    if e1_errors["SourceLocation"].iloc[0].split(':')[1] == \
                            e2_errors["SourceLocation"].iloc[0].split(':')[1]:
                        score += 2
                    # if time < M - 1SD
                    if compiles["TimeEst"].iloc[i] < (
                            compiles["TimeMean"].iloc[i] - compiles["TimeStd"].iloc[i]):
                        score += 1
                    else:
                        # if time > M - 1SD
                        if compiles["TimeEst"].iloc[i] > (
                                compiles["TimeMean"].iloc[i] - compiles["TimeStd"].iloc[i]):
                            score += 25
                        else:
                            score += 15
            # if later event does not have error
            else:
                # if time < M - 1SD
                if compiles["TimeEst"].iloc[i] < (
                        compiles["TimeMean"].iloc[i] - compiles["TimeStd"].iloc[i]):
                    score += 1
                else:
                    # if time > M - 1SD
                    if compiles["TimeEst"].iloc[i] > (
                            compiles["TimeMean"].iloc[i] - compiles["TimeStd"].iloc[i]):
                        score += 25
                    else:
                        score += 15

    if pair_count == 0:
        return None

    watwin = (score / 35.) / (len(compiles) - 1.)
    return watwin

if __name__ == "__main__":
    read_path = "./data"
    write_path = "./out/WatWin.csv"

    if len(sys.argv) > 1:
        read_path = sys.argv[1]
    if len(sys.argv) > 2:
        write_path = sys.argv[2]

    main_table_df = pd.read_csv(os.path.join(read_path, "./MainTable.csv"))
    checker = check_attr(main_table_df)
    if checker:
        time_arr = time_perp(main_table_df)[0]
        mean_dict = time_perp(main_table_df)[1]
        std_dict = time_perp(main_table_df)[2]
        main_table_df["TimeEst"] = [
            time_arr[main_table_df["SubjectID"][i]][main_table_df["CodeStateID"][i]] if main_table_df["SubjectID"][
                                                                                            i] in time_arr.keys() and
                                                                                        main_table_df["CodeStateID"][i] in
                                                                                        time_arr[main_table_df["SubjectID"][
                                                                                            i]].keys() else -1 for i in
            range(len(main_table_df))]
        main_table_df["TimeMean"] = [mean_dict[i] if i in mean_dict.keys() else 0 for i in main_table_df["SubjectID"]]
        main_table_df["TimeStd"] = [std_dict[i] if i in std_dict.keys() else 0 for i in main_table_df["SubjectID"]]
        watwin_map = utils.calculate_metric_map(main_table_df, calculate_watwin)
        print(watwin_map)
        utils.write_metric_map("WatWin", watwin_map, write_path)

