#!/usr/bin/python

import sys
import validator

if len(sys.argv) != 3:
	print len(sys.argv)
	print sys.argv
	print "please provide two files as arguments"
	print "e.g."
	print "python run_validator.py benchmark.csv test.csv"
else:
	validator.run(sys.argv[1], sys.argv[2])
