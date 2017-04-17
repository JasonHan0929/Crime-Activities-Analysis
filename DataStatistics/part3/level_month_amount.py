from __future__ import print_function

import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

def uniteyear(x):
        level = x[11].strip()
	date = datetime.strptime(x[1], '%m/%d/%Y')
        if date.year < 2006:
                x = ((level, "<2006", date.month), 1)
        else:
                x = ((level, date.year, date.month), 1)
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
        tuples = tuples.filter(lambda x : len(x) > 11 and len(x[1].split('/')) > 2).filter(lambda x : x[11] != '' and x[1] != '')
        pair = tuples.map(uniteyear)

        felony = pair.reduceByKey(lambda x, y : x + y).filter(lambda x : x[0][0] == "FELONY").map(lambda x: (x[0][1], (x[0][2], x[1]))).groupByKey().map(output)
        felony = felony.map(lambda x: '%s\t%s' % (x[0], x[1]))
        felony.saveAsTextFile('felony_month_amount.out')

        misdemeanour = pair.reduceByKey(lambda x, y : x + y).filter(lambda x : x[0][0] == "MISDEMEANOR").map(lambda x: (x[0][1], (x[0][2], x[1]))).groupByKey().map(output)
        misdemeanour = misdemeanour.map(lambda x: '%s\t%s' % (x[0], x[1]))
        misdemeanour.saveAsTextFile('misdemeanour_month_amount.out')

        violation = pair.reduceByKey(lambda x, y : x + y).filter(lambda x : x[0][0] == "VIOLATION").map(lambda x: (x[0][1], (x[0][2], x[1]))).groupByKey().map(output)
        violation = violation.map(lambda x: '%s\t%s' % (x[0], x[1]))
        violation.saveAsTextFile('violation_month_amount.out')

        sc.stop()


