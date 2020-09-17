WHERE TO FIND ANSWERS:
1) implementation: data-processing.py
   output: cleaned-data
2) implementation: data-processing.py
   output: assigned-data
3) implementation: data-processing.py, map-gen.py
   output: poi-stats, map.html
4a) implementation: poi-modelling.py
    output: final-stats
4b) implementation: pipeline-dependency.py
    output: pipeline-answer.txt

NOTES ON IMPLEMENTATION:
csv files were slightly edited for ease of data processing, as follows
	-header of DataSample.csv manually edited to remove an unexpected space
	- header of POIList.csv manually edited to remove an unexpected space and to provide header names different from DataSample.csv

During data cleaning, duplicate entries were removed from DataSample.csv, as well as data points that could not be in Canada.
Duplicate POIs were also removed, under the assumption that these were errors.