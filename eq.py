import pandas as pd


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
        # TODO: This is a hack to fix the problem with the current dataset using Order instead of ParentEvent
        if compiles.dtypes["ParentEventID"] == float:
            e1_errors = compile_errors[compile_errors["ParentEventID"] == compiles["Order"].iloc[i]]
            e2_errors = compile_errors[compile_errors["ParentEventID"] == compiles["Order"].iloc[i + 1]]
        else:
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


def calculate_eq_map(main_table):
    return {subject_id: calculate_eq(main_table, subject_id)
            for subject_id in set(main_table_df["SubjectID"])}


if __name__ == "__main__":
    # main_table_df = pd.read_csv("../PythonAST/data/pcrs/MainTable.csv")
    main_table_df = pd.read_csv("./MainTable.csv")
    print(calculate_eq_map(main_table_df))
