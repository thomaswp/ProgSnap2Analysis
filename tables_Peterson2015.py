# Table 1: Basic data set statistics System
# For each dataset, output: 1) System, 2) Course, 3) Language, 4) Students number, 5) Exercises(in # Sets), 6) Compilation events(% with error)
# What will be generated in this script: 1) System, 3) Language, 4) Students number, 5) Exercises, 6) Compilation events
# Table 2: Per-session data set statistics Dataset
# For each dataset, output: 1) Dataset, 2) Gap Time - Min Sessions, 3) Students number, 4) Compilation Events(% of total), 5) Sessions
# What will be generated in this script: 1) Dataset, 2) Gap Time- Min Sessions, 3) Students number, 4) Compilation Events(% of total), 5) Sessions
# Required format of input dataset: ProgSnap2

import pandas as pd
import datetime
from datetime import timedelta
import sys
import os
import utils
import csv


def data_prep(main_table_df, subj, gap_time, min_sessions):
    # Data Preperation (Peterson 2015):
    # 1) Remove submissions where no code changed between submissions
    # 2) Combine sequences of submissions into work sessions
    # 3) Drop sessions with few submissions and omitted students that do not have enough sessions to constitute a representative sample of behaviour
    current_df = main_table_df.loc[main_table_df["SubjectID"] == subj]
    submit_df = current_df[current_df["EventType"] == "Submit"]
    compile_df = current_df[current_df["EventType"] == "Compile"]

    for i in reversed(range(len(submit_df) - 1)):
        if submit_df["CodeStateID"].iloc[i + 1] == submit_df["CodeStateID"].iloc[i]:
            # Start to do 1):
            submit_df = submit_df.drop(submit_df.index[i + 1])

    # Initialize session(subj_session) to be ProblemID numbers for each subject, compile events(subj_compile) to be all compile events for each subject. If a subject has no usable data, usability turns False
    subj_session = len(submit_df['ProblemID'].unique().tolist())
    subj_compile = len(compile_df)
    usability = True
    # Begin calculating new subj_session and subj_compile with thresholds
    # We assume each session only contains one exercise
    for prob in set(submit_df["ProblemID"]):
        prob_df = submit_df[submit_df["ProblemID"] == prob]
        prob_compile = len(compile_df[compile_df["ProblemID"] == prob])

        for j in reversed(range(len(prob_df) - 1)):
            datetimeFormat = '%Y-%m-%dT%H:%M:%S'
            date1 = datetime.datetime.strptime(current_df["ServerTimestamp"].iloc[j + 1], datetimeFormat)
            date2 = datetime.datetime.strptime(current_df["ServerTimestamp"].iloc[j], datetimeFormat)
            time_diff = ((((date1.month - date2.month) * 30 + (date1.day - date2.day)) * 24 + (
                        date1.hour - date2.hour)) * 60 + (date1.minute - date2.minute)) * 60 + (
                                    date1.second - date2.second)
            # Threshold 1: if the duration between two consequtive submissions exceeds gap_time
            if time_diff > gap_time:
                subj_session = subj_session - 1
                subj_compile = subj_compile - prob_compile
                break
            else:
                # Threshold 2: min_sessions (Jadud's selection of min 7 distinct submission per session)
                if len(prob_df) < min_sessions:
                    subj_session = subj_session - 1
                    subj_compile = subj_compile - prob_compile
                    break

    if subj_session == 0 or subj_compile == 0:
        usability = False

    return subj_session, subj_compile, usability


def get_table_1(main_table_df):
    # Calculate Table 1
    # We assume the dataset contains "Compile" and "Compile.Error" in EventType attribute
    # For the dataset which uses Python as programming languge, we assume that any error reported by Python results in a "Compilation Failure"
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
    # For the dataset which uses Python as programming languge, we assume that any error reported by Python results in a "Compilation Failure"
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
    # Get 2)
    # Set thresholds, which can be changed according to different needs
    gap_time = 1200
    min_sessions = 7
    # Get 3), 4), 5)
    # Initialization:
    students = len(main_table_df['SubjectID'].unique().tolist())
    compilation_event = 0
    sessions = 0
    compiles = main_table_df[main_table_df["EventType"] == "Compile"]
    total_compilation_event = len(compiles)
    # Begin calulate for each subject:
    for subj in set(main_table_df["SubjectID"].loc[main_table_df["EventType"]=="Submit"]):
        #subj_data is an array contains (subj_session, subj_compile, usability)
        subj_data = data_prep(main_table_df, subj, gap_time, min_sessions)
        if subj_data[2] == False:
            students = students - 1
        sessions = sessions + subj_data[0]
        compilation_event = compilation_event + subj_data[1]
    perc_of_total = '{:.1%}'.format(compilation_event/total_compilation_event)

    return dataset_name, gap_time, min_sessions, students, compilation_event, perc_of_total, sessions

if __name__ == "__main__":
    read_path = "./data"
    #write_path = "./out/tables_Peterson2015.csv"

    if len(sys.argv) > 1:
        read_path = sys.argv[1]
    if len(sys.argv) > 2:
        write_path = sys.argv[2]

    main_table_df = pd.read_csv(os.path.join(read_path, "MainTable_CloudCoder.csv"))
    table_1 = get_table_1(main_table_df)
    print(table_1)
    table_2 = get_table_2(main_table_df)
    print(table_2)

    csvfile1 = open('./out/table_1_Peterson2015.csv', 'w', newline='')
    obj = csv.writer(csvfile1)
    obj.writerow(
        ['System', 'Language', 'Students', 'Exercises', 'in # Sets', 'Compilation Events', '% with Error'])
    obj.writerow(table_1)
    csvfile1.close()

    csvfile2 = open('./out/table_2_Peterson2015.csv', 'w', newline='')
    obj = csv.writer(csvfile2)
    obj.writerow(
        ['Dataset', 'Gap Time', 'Min Sessions', 'Students', 'Compilation Events', '% of Total', 'Sessions'])
    obj.writerow(table_2)
    csvfile2.close()


