import pandas as pd
import sys
import os
import utils
import data_filter
import logging

out = logging.getLogger()

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
                    if len(shared_errors) > 0:
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


if __name__ == "__main__":
    read_path = "./data"
    # read_path = "./data/DataChallenge"
    write_path = "./out/RED.csv"

    if len(sys.argv) > 1:
        read_path = sys.argv[1]
    if len(sys.argv) > 2:
        write_path = sys.argv[2]

    main_table_df = data_filter.load_main_table(read_path)
    checker = utils.check_attributes(main_table_df, ["SubjectID", "Order", "EventType", "EventID", "ParentEventID",
                                                     "CompileMessageType"])
    if checker:
        red_map = utils.calculate_metric_map(main_table_df, calculate_red)
        out.info(red_map)
        utils.write_metric_map("RED", red_map, write_path)

