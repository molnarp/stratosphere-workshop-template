localTesting = False

if (localTesting):
	benchmarkFile = "nodes-with-areas.csv"
	#benchmarkFile = "nodes-benchmark.csv"

	testFile = "nodes-with-areas 2.csv"
	#testFile = "nodes-test.csv"

	##for testing
	invalidLines = ["nooooo","N1,,A1,A2,A1"]

# expsected input format for nodes is
# nodeid,name,areaId1,areaId2,areaId3...


def retrieveGeobjMapFromFile(fileName):
	
	print "reading data from file: " + str(fileName)

	if fileName is not None:
		with open(fileName) as f:
			content = f.readlines()
	else:
		content  = ["N125799,,Ar62559,Ar62718,Ar1086070,Ar1136099,Ar1136702,Ar1136703,Ar2088648,Ar2833343,Ar3133460","N125800,,Ar62559,Ar62718,Ar1086070,Ar1136099,Ar1136702,Ar1136705,Ar2088648,Ar2833343,Ar3133460"]

	return retrieveGeobjMapFromContent(content)

def retrieveGeobjMapFromContent(content):
	errorCount = 0
	successCount = 0

	resultList = {}
	for line in content:
		geoObj = parseString(line)
		if geoObj is None:
			errorCount += 1
		else:
			successCount += 1
		resultList.update(geoObj)

	print "valid lines " + str(successCount)
	
	if successCount != len(resultList.keys()):
		errorCount += 1
		print "missmatch between input and output size: duplicate nodes in list?"

	if errorCount < 1:
		print "== yeah good job. no errors in file ==\n"
	else: 
		print "errors " + str(errorCount)

	return resultList

def parseString(lineText):
	lineText = cleanUpStrings(lineText)
	lineData = lineText.split(",")
	if validateLine(lineData):
		return geoObjFromLine(lineData)
	else:
		print "error in line \"" + lineText + "\""
		print "skipped this\n"
		return None

def cleanUpStrings(lineText):
	# because life isnt always clean and easy
	# dirty quickfix
	return lineText.replace("\",\"","") 

def geoObjFromLine(lineData):
	return {lineData[0] : set(lineData[2:])}

def validateLine(lineData):
	if len(lineData) < 2:
		print "at least nodeid and name are mandatory"
		return False
	if len(lineData) != len(set(lineData)):
		print "duplicate areas detected"
		return False

	return True

def compareResultSets(benchmarkFile, testFile):

	benchmarkSet = retrieveGeobjMapFromFile(benchmarkFile)
	testSet = retrieveGeobjMapFromFile(testFile)

	print "\n comparing results..."

	# step 1: simple key test

	not_in_both =  set(benchmarkSet.keys()).difference(set(testSet.keys()))

	if len(not_in_both) > 0:
		print "Failed: benchmark and test data not matching - check for element:"
		print not_in_both 
		return False

	# step 2: check areas

	for benchmarkNode in benchmarkSet.keys():
		if len(testSet[benchmarkNode].difference(benchmarkSet[benchmarkNode])) > 0 :
			print "Failed: Mismatch at Nodeid \"" + benchmarkNode +"\" Areas : " + str(testSet[benchmarkNode].difference(benchmarkSet[benchmarkNode]))
			print 
			return False
	return True

def run(benchmarkFile, testFile):
	matched = compareResultSets(benchmarkFile,testFile)
	print "------------------------------"
	print "== Inspector Clouseau says: =="
	print "------------------------------"
	if matched:
		print "Awesome!!! you made it!"
		print "your data set matched the benchmark!"
		print "champagne for everybody!"
	else:
		print "hmmm...sorry..."
		print "your data set was not the same as our benchmark"
		print "umm..."
		print "but hey no problem... just give another implementation a spin!"
	print "------------------------------"

if localTesting:
	run(benchmarkFile, testFile)

