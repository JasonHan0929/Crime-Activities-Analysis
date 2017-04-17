from __future__ import print_function

import sys
from pyspark import SparkContext
from csv import reader
import datetime

def gettime(x):
        KYCD = x[6].strip()
        hp_date = datetime.datetime.strptime(x[1], "%m/%d/%Y")
        re_date = datetime.datetime.strptime(x[5], "%m/%d/%Y")
	if hp_date == re_date:
        	x = ((KYCD, 0), 1)
	elif re_date - hp_date <= datetime.timedelta(7):
		x = ((KYCD, 1), 1)
	elif re_date - hp_date <= datetime.timedelta(30):
		x = ((KYCD, 2), 1)
	elif re_date - hp_date <= datetime.timedelta(365):
		x = ((KYCD, 3), 1)
	else:
		x = ((KYCD, 4), 1)
        return x

def formattime(x):
	KYCD = x[0][0]
	if x[0][1] == 0:
		return ((KYCD, "=0"), x[1])
	elif x[0][1] == 1:
		return ((KYCD, "<=7"), x[1])
	elif x[0][1] == 2:
		return ((KYCD, "<=30"), x[1])
	elif x[0][1] == 3:
		return ((KYCD, "<=365"), x[1])
	elif x[0][1] == 4:
		return ((KYCD, ">365"), x[1]) 

if __name__ == "__main__":
        sc = SparkContext()
        tuples = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
        tuples = tuples.filter(lambda x : len(x) > 6 and len(x[1].split('/')) > 2 and len(x[5].split('/')) > 2).filter(lambda x : x[6] != '' and x[1] != '' and x[5] != '')
        pair = tuples.map(gettime)
        result = pair.reduceByKey(lambda x, y : x + y).sortByKey()
        result = result.map(formattime)
        result = result.map(lambda x: '%s\t%s\t%s' % (x[0][0], x[0][1],  x[1]))
        result.saveAsTextFile('KYCD_report_amount.out')
        sc.stop()

