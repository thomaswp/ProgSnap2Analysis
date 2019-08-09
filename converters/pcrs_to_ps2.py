import ast
import csv
import os
import re
from datetime import datetime


if __name__ == "__main__":
    rows = []

    path = '../SnapHints/R/PCRS/data/code-states-corrected.csv'
    PCRS_INSTANCES = 'Python; PCRS'
    FILE_NAME = 'main.py'

    with open(path, 'r', encoding='utf8') as code_states_file:
        i = 0

        code_states = {}

        spreadsheet = csv.DictReader(code_states_file)
        for row in spreadsheet:
            submission_id = row['submission_id']
            user_id = row['user_id']
            problem_id = row['problem_id']
            timestamp = row['timestamp']
            code = row['code']
            status = row['status']
            result = row['result']

            code = re.sub(r'^\s*[a-f0-9]{40}\n|[a-f0-9]{40}\s*$', '', code)
            if re.search(r'[a-f0-9]{40}', code) is not None:
                print('Hmm: ' + code)

            if code in code_states:
                code_state_id = code_states[code]
            else:
                code_state_id = len(code_states)
                code_states[code] = code_state_id

            timestamp = datetime.strptime(timestamp + '00', '%Y-%m-%d %H:%M:%S.%f%z')
            order = round(timestamp.timestamp() * 1000) * 10

            score = 1 if status == 'Pass' else 0

            # TODO: The problem here is that the student code is sometimes run with some additional template code
            # so it needs to be loaded first
            compile_result = 'Success'
            compile_error = None
            if status != 'Pass':
                try:
                    compile(code, FILE_NAME, 'exec')
                except SyntaxError as e:
                    compile_error = e
                    compile_result = 'Error'
                    #print(problem_id)
                    #print('Exception: ' + str(e.lineno) + ':' + str(e.offset) + str(e))
                    #print('Result: ' + result)
                    #print(code)
                except Exception as e:
                    #print('Non-syntax error!')
                    #print(e)
                    #print(code)
                    pass


            row = {}
            row['SubjectID'] = user_id
            # TODO: make an actual order
            row['ProblemID'] = problem_id
            row['CodeStateID'] = code_state_id
            row['ServerTimestamp'] = timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')
            row['ServerTimezone'] = timestamp.strftime('%z')

            run_event = row.copy()
            run_event['EventType'] = 'Run.Program'
            run_event['EventID'] = submission_id
            run_event['Order'] = order
            run_event['Score'] = score
            order += 1
            rows.append(run_event)

            compile_event = row.copy()
            compile_event['EventType'] = 'Compile'
            compile_event['EventID'] = submission_id + '_c'
            compile_event['ParentEventID'] = run_event['EventID']
            compile_event['Order'] = order
            compile_event['CompileResult'] = compile_result
            order += 1
            rows.append(compile_event)

            if compile_error is not None:
                error_event = row.copy()
                error_event['EventType'] = 'Compile.Error'
                error_event['EventID'] = submission_id + '_ce'
                error_event['ParentEventID'] = compile_event['EventID']
                error_event['Order'] = order
                error_event['CompileMessageType'] = type(compile_error).__name__
                error_event['CompileMessageData'] = result
                error_event['SourceLocation'] = 'Text:' + str(compile_error.lineno) + ':' + str(compile_error.offset)
                error_event['CodeStateSection'] = compile_error.filename
                order += 1
                rows.append(error_event)

            i += 1
            #if i > 100:
            #    break

    header = ['EventID', 'EventType', 'SubjectID', 'Order', 'ProblemID', 'ParentEventID', 'CodeStateID',
              'ServerTimestamp', 'ServerTimezone',
              # Run.Program
              'Score',
              # Compile
              'CompileResult',
              # Compile.Error
              'CompileMessageType', 'CompileMessageData', 'SourceLocation',
              # Note that 'CodeStateSection' is not needed, since we're using a CSV to represent CodeStates
              ]

    out_dir = 'data/pcrs/CodeStates/'
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    with open('data/pcrs/MainTable.csv', 'w', encoding='utf8') as file:
        writer = csv.DictWriter(file, fieldnames=header, lineterminator='\n')
        writer.writeheader()
        for row in rows:
            row = {k: v for k, v in row.items() if k in header}
            writer.writerow(row)

    with open('data/pcrs/CodeStates/CodeStates.csv', 'w', encoding='utf8') as file:
        writer = csv.DictWriter(file, fieldnames=['CodeStateID', 'Code'], lineterminator='\n')
        writer.writeheader()
        for code, code_state_id in code_states.items():
            writer.writerow({'CodeStateID': code_state_id, 'Code': code})
