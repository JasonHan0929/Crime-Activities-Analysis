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

if __name__ == "__main__":
        sc = SparkContext()
        tuples = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
        tuples = tuples.filter(lambda x : len(x) > 13 and len(x[1].split('/')) > 2).filter(lambda x : x[13] != '' and x[1] != '')
        pair = tuples.map(uniteyear)
        result = pair.reduceByKey(lambda x, y : x + y).sortByKey()
        result = result.map(lambda x: '%s\t%s\t%s\t%s' % (x[0][0], x[0][1], x[0][2], x[1]))
        result.saveAsTextFile('area_month_amount.out')
        sc.stop()

