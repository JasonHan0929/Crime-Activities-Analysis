from __future__ import print_function

import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

def uniteyear(x):
        area = x[13].strip().upper()
        date = datetime.strptime(x[1].strip(), "%m/%d/%Y")
        if date.year < 2006:
                x = ((area, "<2006", date.month), 1)
        else:
                x = ((area, date.year, date.month), 1)
        return x
def output(x):
	dic = {}
	for t in x[1]:
		dic[t[0]] = t[1]
	for t in range(1, 13):
		if not dic.has_key(t):
			dic[t] = 0
	s = ''
	for t in range(1, 13):
		s += str(dic[t]) + ','
	return (x[0], s[:-1])

if __name__ == "__main__":
        sc = SparkContext()
        tuples = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
        tuples = tuples.filter(lambda x : len(x) > 13 and len(x[1].split('/')) > 2).filter(lambda x : x[13] != '' and x[1] != '')
        pair = tuples.map(uniteyear)

        bronx = pair.reduceByKey(lambda x, y : x + y).filter(lambda x : x[0][0] == 'BRONX').map(lambda x : (x[0][1], (x[0][2], x[1]))).groupByKey().map(output)
        bronx = bronx.map(lambda x: '%s\t%s' % (x[0], x[1]))
        bronx.saveAsTextFile('bronx_month_amount.out')

        brooklyn = pair.reduceByKey(lambda x, y : x + y).filter(lambda x : x[0][0] == 'BROOKLYN').map(lambda x : (x[0][1], (x[0][2], x[1]))).groupByKey().map(output)
        brooklyn = brooklyn.map(lambda x: '%s\t%s' % (x[0], x[1]))
        brooklyn.saveAsTextFile('brooklyn_month_amount.out')

        manhattan = pair.reduceByKey(lambda x, y : x + y).filter(lambda x : x[0][0] == 'MANHATTAN').map(lambda x : (x[0][1], (x[0][2], x[1]))).groupByKey().map(output)
        manhattan = manhattan.map(lambda x: '%s\t%s' % (x[0], x[1]))
        manhattan.saveAsTextFile('manhattan_month_amount.out')

        queens = pair.reduceByKey(lambda x, y : x + y).filter(lambda x : x[0][0] == 'QUEENS').map(lambda x : (x[0][1], (x[0][2], x[1]))).groupByKey().map(output)
        queens = queens.map(lambda x: '%s\t%s' % (x[0], x[1]))
        queens.saveAsTextFile('queens_month_amount.out')

	SI  = pair.reduceByKey(lambda x, y : x + y).filter(lambda x : x[0][0] == 'STATEN ISLAND').map(lambda x : (x[0][1], (x[0][2], x[1]))).groupByKey().map(output)
        SI = SI.map(lambda x: '%s\t%s' % (x[0], x[1]))
        SI.saveAsTextFile('statenisland_month_amount.out')

        sc.stop()

