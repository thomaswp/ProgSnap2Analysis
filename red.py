import pandas as pd
import sys
import os
import utils


def findconsqerr(df, df_errors, score, start_pos, end_pos):
    idx = start_pos + 1
    if start_pos + 1 <= end_pos:
        for i in range(start_pos, end_pos):
            idx = i + 1
            count = 0

            # Get all compile errors associated with compile events e1 and e2
            e1_errors = df_errors[df_errors["ParentEventID"] == df["EventID"].iloc[i]]

            if len(e1_errors) > 0:
                # If e1 contains an error, then search from e1 to the end of event sequence.
                for j in range(i + 1, len(df)):
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


def calculate_red(main_table, subject_id):
    subject_events = main_table.loc[main_table["SubjectID"] == subject_id]

    subject_events.sort_values(by=['Order'])
    compiles = subject_events[subject_events["EventType"] == "Compile"]
    compile_errors = subject_events[subject_events["EventType"] == "Compile.Error"]

    if len(compiles) <= 1:
        return None

    start = 0
    end = len(compiles) - 1
    score = 0
    red = findconsqerr(compiles, compile_errors, score, start, end)

    return red


if __name__ == "__main__":
    read_path = "./data"
    write_path = "./out/RED.csv"

    if len(sys.argv) > 1:
        read_path = sys.argv[1]
    if len(sys.argv) > 2:
        write_path = sys.argv[2]

    main_table_df = pd.read_csv(os.path.join(read_path, "MainTable.csv"))
    red_map = utils.calculate_metric_map(main_table_df, calculate_red)
    print(red_map)
    utils.write_metric_map("RED", red_map, write_path)

