import pandas as pd
import sys
import os
import utils
import data_filter
import logging

out = logging.getLogger()


def calculate_eq(session_table):
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
        out.log(0, score_delta)
        score += score_delta / 11

    return score / len(compile_pairs)


if __name__ == "__main__":
    read_path = "./data"
    # read_path = "./data/PCRS"
    write_path = "./out/EQ.csv"

    if len(sys.argv) > 1:
        read_path = sys.argv[1]
    if len(sys.argv) > 2:
        write_path = sys.argv[2]

    main_table_df = pd.read_csv(os.path.join(read_path, "MainTable.csv"))
    main_table_df = data_filter.filter_dataset(main_table_df)
    # main_table_df = data_filter.assign_session_ids(main_table_df)
    checker = utils.check_attributes(main_table_df, ["SubjectID", "Order", "EventType", "EventID", "ParentEventID",
                                                     "CompileMessageType"])
    if checker:
        eq_map = utils.calculate_metric_map(main_table_df, calculate_eq)
        out.info(eq_map)
        utils.write_metric_map("ErrorQuotient", eq_map, write_path)

