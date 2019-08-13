# ProgSnap2 Data Analysis

This reposity contains scripts for data analysis on a [ProgSnap2](bit.ly/ProgSnap2) dataset.

Current analyses include:

## Compile Error Metrics

These analyze students' compilation behavior to create a single numerical metric that can be used to predict course success.

These currently include:
* **The Error Quotient** ([eq.py](eq.py)). Jadud, M. C. (2006). Methods and tools for exploring novice compilation behaviour. In Proceedings of the Third International Workshop on Computing Education Research (pp. 73–84). https://doi.org/10.1145/1151588.1151600

Each of these files takes 2 command line arguments:
1) The path to ProgSnap2 repository (e.g. if you pass in "data" it will look for "data/MainTable.csv")
2) The path for the output csv file (including the name of the file)

If these are not supplied, it will default to "./data" and "./out/EQ.csv".

## Adding Student Grades

To see how the error metrics correlate with student grades (and each other), you can run the 
[correlations.py](correlations.py) script. To do this, you will need to make sure your ProgSnap2
dataset contains a Link Table under LinkTables/Subject.csv, containing the columns "SubjectID" and
"X-Grade", which give the grades for the students in the course. Grade here can be any metric of
course performance. 

This script takes 2 command line arguments:
1) The path to ProgSnap2 repository (e.g. if you pass in "data" it will look for "data/MainTable.csv")
2) The path to the output directory where the prior metrics were stored

If these are not supplied, it will default to "./data" and "./out/".

## Dependencies

* Pandas
