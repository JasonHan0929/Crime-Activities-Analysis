from __future__ import print_function

import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

def uniteyear(x):
        area = x[11].strip().upper()
        date = datetime.strptime(x[1], "%m/%d/%Y")
        if date.year < 2006:
                x = ((area, "<2006"), 1)
        else:
                x = ((area, date.year), 1)
        return x
def output(x):
        dic = {}
        for t in x[1]:
                dic[t[0]] = t[1]
        for t in range(2006, 2016):
                if not dic.has_key(t):
                        dic[t] = 0
        if not dic.has_key('<2006'):
                        dic['<2006'] = 0
        s = str(dic['<2006']) + ','
        for t in range(2006, 2016):
                s += str(dic[t]) + ','
        return (x[0], s[:-1])

if __name__ == "__main__":
        sc = SparkContext()
        tuples = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
        tuples = tuples.filter(lambda x : len(x) > 11 and len(x[1].split('/')) > 2).filter(lambda x : x[11] != '' and x[1] != '')
        pair = tuples.map(uniteyear)
        result = pair.reduceByKey(lambda x, y : x + y).map(lambda x : (x[0][0], (x[0][1], x[1]))).groupByKey().map(output)
        result = result.map(lambda x: '%s\t%s' % (x[0], x[1]))
        result.saveAsTextFile('level_year_amount.out')
        sc.stop()

