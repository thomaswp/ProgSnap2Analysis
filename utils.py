import pathlib
import csv
import numpy as np
import logging
import sys
import os

out = logging.getLogger()
VERSION = '2019.09.28.A'


def setup_logging(out_dir):
    if out.hasHandlers():
        return
    print("Setting up logger...")
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
    file_handler = logging.FileHandler(os.path.join(out_dir, "log.txt"))
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    out.addHandler(file_handler)
    out.addHandler(stream_handler)
    out.setLevel(logging.DEBUG)
    out.info("Logger initialized for version: " + VERSION)


setup_logging("out")


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


def check_attributes(main_table, attributes):
    # Check whether the dataset has required attributes, if not, pop-up warnings:
    for required_attr in attributes:
        if not isinstance(required_attr, list):
            required_attr = [required_attr]
        has = False
        for attr in required_attr:
            if attr in main_table:
                has = True
        if not has:
            out.info("One of the following attributes is required: " + required_attr + " !")
            return False
    return True


def write_metric_map(name, metric_map, path):
    pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf8') as file:
        writer = csv.DictWriter(file, fieldnames=["SubjectID", name], lineterminator='\n')
        writer.writeheader()
        for subject_id, value in metric_map.items():
            writer.writerow({"SubjectID": subject_id, name: value})


def calculate_metric_map(main_table, metric_fn):
    out.info("Calculating error metric...")
    subject_ids = set(main_table["SubjectID"])
    metric_map = {}
    dropped = 0
    for i, subject_id in enumerate(subject_ids):
        print_progress_bar(i + 1, len(subject_ids))
        subject_events = main_table[main_table["SubjectID"] == subject_id]
        metrics = []
        for session_id in set(subject_events["SessionID"]):
            metric = metric_fn(subject_events[subject_events["SessionID"] == session_id])
            if metric is not None:
                metrics.append(metric)
        out.debug("Metrics %d: %s" % (i, metrics))
        if len(metrics) == 0:
            dropped += 1
            continue
        metric_map[subject_id] = np.mean(metrics)

    out.info("Dropped %d subjects with no pairs of compile events" % dropped)
    return metric_map


# TODO: check if code is the same
def extract_compile_pair_indexes(compiles):
    pairs = []
    start = 0
    for i in range(1, len(compiles) - 1):
        # Only look at consecutive compiles within a single assignment/problem/session
        changed_segments = False
        for segment_id in ["SessionID", "ProblemID", "AssignmentID"]:
            if segment_id not in compiles:
                continue
            if compiles[segment_id].iloc[i] != compiles[segment_id].iloc[start]:
                changed_segments = True
                break
        if changed_segments:
            # If we're in a new Session or Problem, start a new pair
            start = i
            continue

        if compiles["CodeStateID"].iloc[start] == compiles["CodeStateID"].iloc[i]:
            # If the code hasn't changed, skip this end pair
            continue

        # If this is good, add the pair and set this code to the start of the next pair
        pairs.append([start, i])
        start = i
    return pairs
