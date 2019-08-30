import sys
import utils
import data_filter
import logging

out = logging.getLogger()


def calculate_red(session_table):
    session_table = session_table.sort_values(by=['Order'])
    compiles = session_table[session_table["EventType"] == "Compile"]
    compile_errors = session_table[session_table["EventType"] == "Compile.Error"]

    red = 0
    divisor = 0

    # We operate over individual segments (working on the same problem), since that's how RED was designed to operate
    # i.e. we don't consider an error repeated if it occurs across problems
    segments = utils.get_segments_indexes(compiles)
    for segment in segments:
        repeated = 0
        for i in range(1, len(segment)):
            divisor += 1
            e1_errors = compile_errors[compile_errors["ParentEventID"] == compiles["EventID"].iloc[segment[i - 1]]]
            e2_errors = compile_errors[compile_errors["ParentEventID"] == compiles["EventID"].iloc[segment[i]]]
            shared_errors = set(e1_errors["CompileMessageType"]).intersection(set(e2_errors["CompileMessageType"]))
            if len(shared_errors) > 0:
                # If there is a shared error, we increment the r count
                repeated = repeated + 1
            else:
                # Otherwise, there was a new error or no errors, so we add to RED and reset the repeated count
                if repeated > 0:
                    red += (repeated ** 2) / (repeated + 1)
                repeated = 0

        if repeated > 0:
            red += (repeated ** 2) / (repeated + 1)

    if divisor == 0:
        return None

    # TODO: No official way to normalize RED, so we divide by the number of compiles
    red = red / divisor

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

