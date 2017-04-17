from __future__ import print_function

import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

def uniteyear(x):
        KYCD = x[6].strip()
	date = datetime.strptime(x[1], "%m/%d/%Y")
        year = date.year
        month = date.month
        if int(year) < 2006:
                x = ((KYCD, "<2006", month), 1)
        else:
                x = ((KYCD, year, month), 1)
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
        tuples = tuples.filter(lambda x : len(x) > 6 and len(x[1].split('/')) > 2).filter(lambda x : x[6] != '' and x[1] != '')
        pair = tuples.map(uniteyear)
	
        result_341 = pair.reduceByKey(lambda x, y : x + y).filter(lambda x : x[0][0] == '341' and x[0][1] != '<2006').map(lambda x : (x[0][1], (x[0][2], x[1]))).groupByKey().map(output)
        result_341 = result_341.map(lambda x: '%s\t%s' % (x[0],  x[1]))
        result_341.saveAsTextFile('341_month_amount.out')

        result_578 = pair.reduceByKey(lambda x, y : x + y).filter(lambda x : x[0][0] == '578' and x[0][1] != '<2006').map(lambda x : (x[0][1], (x[0][2], x[1]))).groupByKey().map(output)
        result_578 = result_578.map(lambda x: '%s\t%s' % (x[0],  x[1]))
        result_578.saveAsTextFile('578_month_amount.out')

        result_344 = pair.reduceByKey(lambda x, y : x + y).filter(lambda x : x[0][0] == '344' and x[0][1] != '<2006').map(lambda x : (x[0][1], (x[0][2], x[1]))).groupByKey().map(output)
        result_344 = result_344.map(lambda x: '%s\t%s' % (x[0], x[1]))
        result_344.saveAsTextFile('344_month_amount.out')

        sc.stop()

