import pandas as pd
import sys
import os
import utils
import data_filter


def check_attr(main_table_df):
    # Check whether the dataset has required attributes, if not, pop-up warnings:
    counter = 0
    for required_attr in ["SubjectID", "Order", "EventType", "EventID", "CodeStateID", "ParentEventID",
                          "CompileMessageType"]:
        if required_attr not in main_table_df:
            print("The dataset misses the attribute required: ", required_attr + " !")
            counter = 1
    if counter == 0:
        return True
    else:
        return False


def findconsqerr(df, df_errors, score, start_pos, end_pos):
    idx = start_pos + 1
    if start_pos + 1 <= end_pos:
        for i in range(start_pos, end_pos):
            # Only look at consecutive compiles within a single assignment/problem/session
            # TODO: Jadud (2006) doesn't specify whether a session can cross problems, but we assume not
            changed_segments = False
            for segment_id in ["SessionID", "ProblemID", "AssignmentID"]:
                if segment_id not in df:
                    continue
                if df[segment_id].iloc[i] != df[segment_id].iloc[i + 1]:
                    changed_segments = True
                    break
            if changed_segments:
                continue

            idx = i + 1
            count = 0

            # Get all compile errors associated with compile events e1 and e2
            e1_errors = df_errors[df_errors["ParentEventID"] == df["EventID"].iloc[i]]

            if len(e1_errors) > 0:
                # If e1 contains an error, then search from e1 to the end of event sequence.
                for j in range(i + 1, len(df)):
                    # Becker(2016) doesn't specify whether a sequence can cross problems, we assume yes
                    e2_errors = df_errors[df_errors["ParentEventID"] == df["EventID"].iloc[j]]

                    # Get the set of errors shared by both compiles
                    shared_errors = set(e1_errors["CompileMessageType"]).intersection(
                        set(e2_errors["CompileMessageType"]))

                    # If e1 and e2 contain the same error, seek from e2 to the end of event sequence by changing idx
                    if len(e2_errors) > 0 and len(shared_errors) > 0:
                        idx = idx + 1
                        count = count + 1
                    else:
                        break
                # calculate red for repeated error strings
                score += (count ** 2) / (count + 1)
                break
        # keep calculate red from current e2 to the end of event
        score = findconsqerr(df, df_errors, score, idx, end_pos)

        return score
    else:
        return score


def calculate_red(session_table):
    session_table = session_table.sort_values(by=['Order'])
    compiles = session_table[session_table["EventType"] == "Compile"]
    compile_errors = session_table[session_table["EventType"] == "Compile.Error"]

    if len(compiles) <= 1:
        return None

    start = 0
    end = len(compiles) - 1
    score = 0
    red = findconsqerr(compiles, compile_errors, score, start, end)

    # TODO: No official way to normalize RED, so we divide by the number of compiles
    red = red / (len(compiles) - 1)

    return red


def calculate_red(session_table):
    session_table = session_table.sort_values(by=['Order'])
    compiles = session_table[session_table["EventType"] == "Compile"]
    compile_errors = session_table[session_table["EventType"] == "Compile.Error"]

    compile_pairs = utils.extract_compile_pair_indexes(compiles)
    if len(compile_pairs) == 0:
        return None

    score = 0
    for pair in compile_pairs:
        # Get all compile errors associated with compile events e1 and e2
        e1 = pair[0]
        e2 = pair[1]

        e1_errors = compile_errors[compile_errors["ParentEventID"] == compiles["EventID"].iloc[e1]]
        e2_errors = compile_errors[compile_errors["ParentEventID"] == compiles["EventID"].iloc[e2]]

        score_delta = 0
        if len(e1_errors) > 0 and len(e2_errors) > 0:
            # If both compiles resulted in errors, add 8 to the score
            score_delta += 8

            # Get the set of errors shared by both compiles
            # TODO: Check how Jadud handled multiple compile errors (don't think he did)
            shared_errors = set(e1_errors["CompileMessageType"]).intersection(set(e2_errors["CompileMessageType"]))
            if len(shared_errors) > 0:
                score_delta += 3
        score += score_delta / 11

    return score / len(compile_pairs)


if __name__ == "__main__":
    read_path = "./data"
    # read_path = "./data/DataChallenge"
    write_path = "./out/RED.csv"

    if len(sys.argv) > 1:
        read_path = sys.argv[1]
    if len(sys.argv) > 2:
        write_path = sys.argv[2]

    main_table_df = pd.read_csv(os.path.join(read_path, "MainTable.csv"))
    main_table_df = data_filter.filter_dataset(main_table_df)
    checker = utils.check_attributes(main_table_df, ["SubjectID", "Order", "EventType", "EventID", "ParentEventID",
                                                     "CompileMessageType"])
    if checker:
        red_map = utils.calculate_metric_map(main_table_df, calculate_red)
        print(red_map)
        utils.write_metric_map("RED", red_map, write_path)

