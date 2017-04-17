from __future__ import print_function

import sys
from pyspark import SparkContext
from csv import reader

def uniteyear(x):
        KYCD = x[6].strip()
        year = x[1].split('/')[2]
        if int(year) < 2006:
                x = ((KYCD, "<2006"), 1)
        else:
                x = ((KYCD, year), 1)
        return x

if __name__ == "__main__":
        sc = SparkContext()
        tuples = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
        tuples = tuples.filter(lambda x : len(x) > 6  and len(x[1].split('/')) > 2).filter(lambda x : x[6] != '' and x[1] != '')
        pair = tuples.map(uniteyear)
        result = pair.reduceByKey(lambda x, y : x + y).sortByKey()
        result = result.map(lambda x: '%s\t%s\t%s' % (x[0][0], x[0][1], x[1]))
        result.saveAsTextFile('KYCD_year_amount.out')
        sc.stop()
