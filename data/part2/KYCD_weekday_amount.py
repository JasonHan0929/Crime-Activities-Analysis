from __future__ import print_function

import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

def weekday(x):
        KYCD = x[6].strip()
        date = datetime.strptime(x[1], "%m/%d/%Y")
        weekday = date.weekday()
        x = ((KYCD, weekday), 1)
        return x
def output(x):
	dic = {}
	for t in x[1]:
		dic[t[0]] = t[1]
	for t in range(0, 7):
		if not dic.has_key(t):
			dic[t] = 0
	s = ''
	for t in range(0, 7):
		s += str(dic[t]) + ','
	return (x[0], s[:-1])

if __name__ == "__main__":
        sc = SparkContext()
        tuples = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
        tuples = tuples.filter(lambda x : len(x) > 6 and len(x[1].split('/')) > 2).filter(lambda x : x[6] != '' and x[1] != '')
        pair = tuples.map(weekday)
        result = pair.reduceByKey(lambda x, y : x + y).map(lambda x : (x[0][0], (x[0][1], x[1]))).groupByKey().map(output)
        result = result.map(lambda x: '%s\t%s' % (x[0], x[1]))
        result.saveAsTextFile('KYCD_weekday_amount.out')
        sc.stop()

