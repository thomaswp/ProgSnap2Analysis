import pathlib
import csv


# Print iterations progress
def print_progress_bar(iteration, total, prefix ='', suffix ='', decimals = 1, length = 100, fill ='█'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='')
    # Print New Line on Complete
    if iteration == total:
        print()


def write_metric_map(name, metric_map, path):
    pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf8') as file:
        writer = csv.DictWriter(file, fieldnames=["SubjectID", name], lineterminator='\n')
        writer.writeheader()
        for subject_id, value in metric_map.items():
            writer.writerow({"SubjectID": subject_id, name: value})


# TODO: Change to calculate over sessions
def calculate_metric_map(main_table, metric_fn):
    subject_ids = set(main_table["SubjectID"])
    metric_map = {}
    for i, subject_id in enumerate(subject_ids):
        metric_map[subject_id] = metric_fn(main_table, subject_id)
        print_progress_bar(i + 1, len(subject_ids))
    return metric_map


# TODO: check if code is the same
def extract_compile_pair_indexes(compiles):
    pairs = []
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
