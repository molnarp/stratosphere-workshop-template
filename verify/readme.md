#Validation

For validation of your implementation just use the following script:

Usage:

	python run_validator.py <benchMarkFileName> <yourTestResultFileName>

Your result file format must be:

	# node id, name (optional), list of intersecting areas
	N125813,"Kirchhuchting",Ar3133460,Aw26714865,Ar1086093,Ar1137601,Ar1137596,Ar1136099,Ar2088648,Ar62559,Ar62718,Ar2833343
	
You should take the dataset from http://dev.komoot.de/workshop:

Run your geometry matcher with the json input files `*-raw.csv.gz` and compare your results with the benchmark csv files `*-with-areas.csv`.

