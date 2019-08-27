# Table 1: Basic data set statistics System
# For each dataset, output: 1) System, 2) Course, 3) Language, 4) Students number, 5) Exercises(in # Sets), 6)
# Compilation events(% with error)
# What will be generated in this script: 1) System, 3) Language, 4) Students number, 5) Exercises, 6) Compilation events
# Table 2: Per-session data set statistics Dataset
# For each dataset, output: 1) Dataset, 2) Gap Time - Min Sessions, 3) Students number,
# 4) Compilation Events(% of total), 5) Sessions
# What will be generated in this script: 1) Dataset, 2) Gap Time- Min Sessions, 3) Students number,
# 4) Compilation Events(% of total), 5) Sessions
# Required format of input dataset: ProgSnap2

import pandas as pd
import datetime
import sys
import os
import csv
import pathlib

gap_time = 1200
min_sessions = 2
min_compiles = 4
datetimeFormat = '%Y-%m-%dT%H:%M:%S'
pd.options.mode.chained_assignment = None  # default='warn'

def check_attr(main_table_df):
    # Check whether the dataset has required attributes, if not, pop-up warnings:
    counter = 0
    for required_attr in ["SubjectID", "ProblemID", "EventType", "CodeStateID", "ServerTimestamp"]:
        if required_attr not in main_table_df:
            print("The dataset misses the attribute required: ", required_attr + " !")
            counter = 1
    if counter == 0:
        return True
    else:
        return False


def assign_session_ids(main_table_df, gap_time):
    if "SessionID" in main_table_df:
        return main_table_df

    for tsf in ["ServerTimestamp", "ClientTimestamp"]:
        if tsf in main_table_df:
            timestamp_field = tsf
    if timestamp_field is None:
        raise Exception("No Timestamp!")

    main_table_df.sort_values(by=['Order'])
    # Initialize SessionID column in main_table_df
    main_table_df["SessionID"] = 0
    new_main_table_df = pd.DataFrame()
    for subject_id in set(main_table_df["SubjectID"]):
        subject_events = main_table_df[main_table_df["SubjectID"] == subject_id]

        session_id = 0
        last_timestamp = datetime.datetime.strptime(subject_events[timestamp_field].iloc[0], datetimeFormat)
        for i in range(len(subject_events)):
            subject_events["SessionID"].iloc[i] = str(subject_id) + "_" + str(session_id)

            timestamp = datetime.datetime.strptime(subject_events[timestamp_field].iloc[i], datetimeFormat)
            if (timestamp - last_timestamp).total_seconds() / 60.0 > gap_time:
                session_id += 1
            last_timestamp = timestamp
        new_main_table_df = new_main_table_df.append(subject_events)

    return new_main_table_df


def filter_dataset(main_table_df, gap_time, min_compiles, min_sessions):
    main_table_df = assign_session_ids(main_table_df, gap_time)

    session_to_keep = [session_id for session_id in set(main_table_df["SessionID"])
                       if len(main_table_df[(main_table_df['SessionID'] == session_id) &
                                            (main_table_df['EventType'] == "Compile")]) >= min_compiles]
    main_table_df = main_table_df[main_table_df["SessionID"].isin(session_to_keep)]

    students_to_keep = [subject_id for subject_id in set(main_table_df["SubjectID"])
                        if len(set(main_table_df[main_table_df["SubjectID"] == subject_id]
                                   ["SessionID"])) >= min_sessions]

    main_table_df = main_table_df[main_table_df["SubjectID"].isin(students_to_keep)]

    return main_table_df


def get_table_1(main_table_df):
    # Calculate Table 1
    # We assume the dataset contains "Compile" and "Compile.Error" in EventType attribute
    # For the dataset which uses Python as programming languge, we assume that any error reported by Python results in a
    # "Compilation Failure"
    # We assume one set contains one question
    # Get 1) and 3)
    if 'ToolInstances' in main_table_df.columns:
        tool_instances_arr = main_table_df["ToolInstances"].iloc[0].split(';')
        system = tool_instances_arr[0]
        if len(tool_instances_arr) > 1:
            language = tool_instances_arr[1]
        else:
            language = 'N/A'
    else:
        system = 'N/A'
        language = 'N/A'
    # Get 4) and 5)
    students_num = len(main_table_df['SubjectID'].unique().tolist())
    exercises_num = len(main_table_df['ProblemID'].unique().tolist())
    sets_num = exercises_num
    # Get 6)
    compiles = main_table_df[main_table_df["EventType"] == "Compile"]
    compilation_event = len(compiles)

    compile_errors = main_table_df[main_table_df["EventType"] == "Compile.Error"]
    perc_w_error = '{:.1%}'.format(len(compile_errors)/compilation_event)

    return system, language, students_num, exercises_num, sets_num, compilation_event, perc_w_error


def get_table_2(main_table_df):
    # Calculate Table 2
    # We assume the dataset contains "Compile" and "Compile.Error" in EventType attribute
    # For the dataset which uses Python as programming languge, we assume that any error reported by Python results in
    # a "Compilation Failure"
    # We assume each session contains one exercise

    # Get 1)
    if 'ToolInstances' in main_table_df.columns:
        tool_instances_arr = main_table_df["ToolInstances"].iloc[0].split(';')
        system = tool_instances_arr[0]
        if len(tool_instances_arr) > 1:
            language = tool_instances_arr[1]
        else:
            language = 'N/A'
    else:
        system = 'N/A'
        language = 'N/A'
    dataset_name = system + '(' + language + ')'

    # Get 3), 4), 5)
    compiles = main_table_df[main_table_df["EventType"] == "Compile"]
    total_compilation_event = len(compiles)
    main_table_df = filter_dataset(main_table_df, gap_time, min_compiles, min_sessions)
    compilation_event = len(main_table_df[main_table_df["EventType"] == "Compile"])
    perc_of_total = '{:.1%}'.format(compilation_event/total_compilation_event)

    sessions = len(main_table_df['SessionID'].unique().tolist())
    students = len(main_table_df['SubjectID'].unique().tolist())

    return dataset_name, gap_time, min_sessions, students, compilation_event, perc_of_total, sessions


if __name__ == "__main__":
    #read_path = "./data/DataChallenge"
    read_path = "./data/"
    write_dir = "./out"

    if len(sys.argv) > 1:
        read_path = sys.argv[1]
    if len(sys.argv) > 2:
        write_path = sys.argv[2]

    main_table = pd.read_csv(os.path.join(read_path, "MainTable_WatWin.csv"))
    checker = check_attr(main_table)
    if checker:
        table_1 = get_table_1(main_table)
        print(table_1)
        table_2 = get_table_2(main_table)
        print(table_2)

        pathlib.Path(write_dir).parent.mkdir(parents=True, exist_ok=True)
        csvfile1 = open(os.path.join(write_dir, 'table_1_Peterson2015.csv'), 'w', newline='')
        obj = csv.writer(csvfile1)
        obj.writerow(
            ['System', 'Language', 'Students', 'Exercises', 'in # Sets', 'Compilation Events', '% with Error'])
        obj.writerow(table_1)
        csvfile1.close()

        csvfile2 = open(os.path.join(write_dir, 'table_2_Peterson2015.csv'), 'w', newline='')
        obj = csv.writer(csvfile2)
        obj.writerow(
            ['Dataset', 'Gap Time', 'Min Sessions', 'Students', 'Compilation Events', '% of Total', 'Sessions'])
        obj.writerow(table_2)
        csvfile2.close()


