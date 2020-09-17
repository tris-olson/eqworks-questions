# eqworks_questions
Solutions to technical interview questions for Data Engineer position at EQ Works

Where to find output for each question:
1) cleaned-data
2) assigned-data
3) poi-stats, map.html
4a) final-stats
4b) pipeline-answer.txt

Implementation notes:
.csv files were slightly edited for ease of data processing, as follows
-header of DataSample.csv manually edited to remove an unexpected space
-header of POIList.csv manually edited to remove an unexpected space and to provide header names different from DataSample.csv

During data cleaning, duplicate entries were removed from DataSample.csv, as well as data points that could not be in Canada.
Duplicate POIs were also removed, under the assumption that these were errors.

All of these notes are included in comments in submission.py
