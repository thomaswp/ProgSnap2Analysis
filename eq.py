import pandas as pd
import sys
import os
import utils


def check_attr(main_table_df):
    # Check whether the dataset has required attributes, if not, pop-up warnings:
    counter = 0
    for required_attr in ["SubjectID", "Order", "EventType", "EventID", "ParentEventID",
                          "CompileMessageType"]:
        if required_attr not in main_table_df:
            print("The dataset misses the attribute required: ", required_attr + " !")
            counter = 1
    if counter == 0:
        return True
    else:
        return False


def calculate_eq(main_table, subject_id):
    subject_events = main_table.loc[main_table["SubjectID"] == subject_id]

    subject_events.sort_values(by=['Order'])
    compiles = subject_events[subject_events["EventType"] == "Compile"]
    compile_errors = subject_events[subject_events["EventType"] == "Compile.Error"]

    if len(compiles) <= 1:
        return None

    score = 0
    pair_count = 0
    # Iterate through pairs of compile events, e1 and e2
    for i in range(len(compiles) - 1):
        # Only look at consecutive compiles within a single assignment/problem/session
        # TODO: Jadud (2006) doesn't specify whether a session can cross problems, but we assume not
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

        # Get all compile errors associated with compile events e1 and e2
        e1_errors = compile_errors[compile_errors["ParentEventID"] == compiles["EventID"].iloc[i]]
        e2_errors = compile_errors[compile_errors["ParentEventID"] == compiles["EventID"].iloc[i + 1]]

        score_delta = 0
        if len(e1_errors) > 0 and len(e2_errors) > 0:
            # If both compiles resulted in errors, add 8 to the score
            score_delta += 8

            # Get the set of errors shared by both compiles
            # TODO: Check how Jadud handled multiple compile errors (don't think he did)
            shared_errors = set(e1_errors["CompileMessageType"]).intersection(set(e2_errors["CompileMessageType"]))
            if len(shared_errors) > 0:
                score_delta += 3
        score += score_delta

    if pair_count == 0:
        return None

    # Normalize the score by the maximum value and take the average
    return (score / 11.) / pair_count


if __name__ == "__main__":
    read_path = "./data"
    write_path = "./out/EQ.csv"

    if len(sys.argv) > 1:
        read_path = sys.argv[1]
    if len(sys.argv) > 2:
        write_path = sys.argv[2]

    main_table_df = pd.read_csv(os.path.join(read_path, "MainTable.csv"))
    checker = check_attr(main_table_df)
    if checker:
        eq_map = utils.calculate_metric_map(main_table_df, calculate_eq)
        print(eq_map)
        utils.write_metric_map("ErrorQuotient", eq_map, write_path)

