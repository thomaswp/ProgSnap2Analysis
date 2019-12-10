import pandas as pd
import numpy as np
import sys
import os
import datetime
import data_filter
import utils
import logging

out = logging.getLogger()

def time_perp(main_table_df):
    out.info("Performing Watwin pre-processing...")
    # Watson(2013) doesn't state how they get mean and sd, we assume both mean and sd calculated from all compilation
    # pairs
    # Initialization:
    time_arr = {}
    mean_dict = {}
    std_dict = {}

    subjects = set(main_table_df["SubjectID"])
    timer_index = 1
    for subj in subjects:
        utils.print_progress_bar(timer_index, len(subjects))
        timer_index += 1

        current_df = main_table_df.loc[main_table_df["SubjectID"] == subj]
        current_df = current_df.sort_values(by=['Order'])
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
                        # Watson(2013) requires time estimate preparation before calculating score, we assume no
                        # invocation reported in dataset, which means using time difference of compilcation pairs
                        # directly
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

    out.info("Finished Watwin pre-processing...")
    return time_arr, mean_dict, std_dict


def calculate_watwin(session_table):
    # Watson(2013) requires 1) deletion fixes 2) commented fixes during data preparation 3) error message
    # generalization, we assume the dataset has fulfilled this requirement
    session_table = session_table.sort_values(by=['Order'])
    compiles = session_table[session_table["EventType"] == "Compile"]
    compile_errors = session_table[session_table["EventType"] == "Compile.Error"]

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

                    # TODO: Don't just use the first compile message - use all
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
                    try:
                        if e1_errors["SourceLocation"].iloc[0].split(':')[1] == \
                                e2_errors["SourceLocation"].iloc[0].split(':')[1]:
                            score += 2
                    except:
                        out.info("Improperly formatted source location in: [%s, %s]" % (
                            e1_errors["SourceLocation"].iloc[0], e2_errors["SourceLocation"].iloc[0]))

                    # if time < M - 1SD
                    if compiles["TimeEst"].iloc[i] < (
                            compiles["TimeMean"].iloc[i] - compiles["TimeStd"].iloc[i]):
                        score += 1
                    # if time >= M - 1SD
                    else:
                        # if time > M + 1SD
                        if compiles["TimeEst"].iloc[i] > (
                                compiles["TimeMean"].iloc[i] + compiles["TimeStd"].iloc[i]):
                            score += 25
                        # if time <= M + 1SD
                        else:
                            score += 15
                # if later event does not have error
                else:
                    # if time < M - 1SD
                    if compiles["TimeEst"].iloc[i] < (
                            compiles["TimeMean"].iloc[i] - compiles["TimeStd"].iloc[i]):
                        score += 1
                    # if time >= M - 1SD
                    else:
                        # if time > M + 1SD
                        if compiles["TimeEst"].iloc[i] > (
                                compiles["TimeMean"].iloc[i] + compiles["TimeStd"].iloc[i]):
                            score += 25
                        # if time <= M + 1SD
                        else:
                            score += 15

    if pair_count == 0:
        return None

    watwin = (score / 35.) / (len(compiles) - 1.)
    return watwin


if __name__ == "__main__":
    read_path = "./data"
    # read_path = "./data/DataChallenge"
    write_path = "./out/Watwin.csv"

    if len(sys.argv) > 1:
        read_path = sys.argv[1]
    if len(sys.argv) > 2:
        write_path = sys.argv[2]

    main_table_df = data_filter.load_main_table(read_path)
    checker = utils.check_attributes(main_table_df, ["SubjectID", "Order", "EventType", "EventID", "CodeStateID",
                                                     "ParentEventID", "CompileMessageData", "CompileMessageType",
                                                     "SourceLocation", ["ServerTimestamp", "ClientTimestamp"]])
    if checker:
        perp = time_perp(main_table_df)
        time_arr = perp[0]
        mean_dict = perp[1]
        std_dict = perp[2]
        main_table_df["TimeEst"] = [
            time_arr[main_table_df["SubjectID"].iloc[i]][main_table_df["CodeStateID"].iloc[i]]
            if main_table_df["SubjectID"].iloc[i] in time_arr.keys() and main_table_df["CodeStateID"].iloc[i] in
               time_arr[main_table_df["SubjectID"].iloc[i]].keys() else -1 for i in range(len(main_table_df))]
        main_table_df["TimeMean"] = [mean_dict[i] if i in mean_dict.keys() else 0 for i in main_table_df["SubjectID"]]
        main_table_df["TimeStd"] = [std_dict[i] if i in std_dict.keys() else 0 for i in main_table_df["SubjectID"]]
        watwin_map = utils.calculate_metric_map(main_table_df, calculate_watwin)
        out.info(watwin_map)
        utils.write_metric_map("WatWin", watwin_map, write_path)

