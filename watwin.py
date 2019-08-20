import pandas as pd
import numpy as np
import sys
import os
import datetime
from datetime import timedelta
import utils


def calculate_watwin(main_table, subject_id):
    subject_events = main_table.loc[main_table["SubjectID"] == subject_id]

    subject_events.sort_values(by=['Order'])
    compiles = subject_events[subject_events["EventType"] == "Compile"]
    compile_errors = subject_events[subject_events["EventType"] == "Compile.Error"]

    if len(compiles) <= 1:
        return None

    # time estimation: time_diff of (e_i,e_i+1) is written on e_i
    # calculate time estimation, mean and std for each subject:
    time_arr = {}
    mean_dict = {}
    std_dict = {}

    for subj in range(len(compiles) - 1):
        # Only look at consecutive compiles within a single assignment/problem/session
        # Before starting the algorithm:
        # Watson (2013) requires deletion fixes and commented fixes, we assume dataset have done this
        changed_segments = False
        for segment_id in ["SessionID", "ProblemID", "AssignmentID"]:
            if segment_id not in compiles:
                continue
            if compiles[segment_id].iloc[subj] != compiles[segment_id].iloc[subj + 1]:
                changed_segments = True
                break
        if changed_segments:
            continue

        sum_time = 0
        count_time = 0

        if len(compiles) > 1:
            time_arr[subj] = {}
            for i in range(len(compiles) - 1):
                # remove the identical pairs of events by their CodeStateID
                if compiles["CodeStateID"].iloc[i + 1] != compiles["CodeStateID"].iloc[i]:
                    # Get all compile errors associated with compile events e1 and e2
                    e1_errors = compile_errors[compile_errors["ParentEventID"] == compiles["EventID"].iloc[i]]
                    e2_errors = compile_errors[compile_errors["ParentEventID"] == compiles["EventID"].iloc[i + 1]]
                    # If e1 compile resulted in error
                    if len(e1_errors) > 0:
                        datetimeformat = '%Y-%m-%dT%H:%M:%S'
                        date1 = datetime.datetime.strptime(compiles["ServerTimestamp"].iloc[i + 1], datetimeformat)
                        date2 = datetime.datetime.strptime(compiles["ServerTimestamp"].iloc[i], datetimeformat)
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

    # add TimeEst, TimeMean, and TimeStd to compiles dataframe
    compiles["TimeEst"] = [time_arr[compiles["SubjectID"][i]][compiles["CodeStateID"][i]] if compiles["SubjectID"][i] in time_arr.keys() and compiles["CodeStateID"][i] in time_arr[compiles["SubjectID"][i]].keys() else -1 for i in range(len(compiles))]
    compiles["TimeMean"] = [mean_dict[i] if i in mean_dict.keys() else 0 for i in compiles["SubjectID"]]
    compiles["TimeStd"] = [std_dict[i] if i in std_dict.keys() else 0 for i in compiles["SubjectID"]]

    # begin calculate WatWin scores:
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

        # remove identical pairs
        if compiles["CodeStateID"].iloc[i] != compiles["CodeStateID"].iloc[i + 1]:
            # Get all compile errors associated with compile events e1 and e2
            e1_errors = compile_errors[compile_errors["ParentEventID"] == compiles["EventID"].iloc[i]]
            e2_errors = compile_errors[compile_errors["ParentEventID"] == compiles["EventID"].iloc[i + 1]]
            # if former event has error
            if len(e1_errors) > 0:
                # if later event has error
                if len(e2_errors) > 0:
                    # Get the set of errors shared by both compiles
                    shared_errors = set(e1_errors["CompileMessageType"]).intersection(set(e2_errors["CompileMessageType"]))
                    # if same full message
                    if e1_errors["ProgramErrorOutput"] == e2_errors["ProgramErrorOutput"]:
                        score += 4
                    # if same error type
                    if len(shared_errors) > 0:
                        score += 4

                    # TODO: Watson (2013) requires for error line number of compiled code
                    # if same line
                    if err_df["SourceLocation"].iloc[i].split(':')[1] == err_df["SourceLocation"].iloc[i + 1].split(':')[1]:
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
    #read_path = "./data"
    read_path = "../PythonAST/data/PCRS"
    write_path = "./out/WatWin.csv"

    if len(sys.argv) > 1:
        read_path = sys.argv[1]
    if len(sys.argv) > 2:
        write_path = sys.argv[2]

    main_table_df = pd.read_csv(os.path.join(read_path, "MainTable.csv"))
    watwin_map = utils.calculate_metric_map(main_table_df, calculate_watwin)
    print(watwin_map)
    utils.write_metric_map("WatWinScore", watwin_map, write_path)
