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
def numtoweekday(x):
	KYCD = x[0][0]
	weekday = x[0][1]
	total = x[1]
	if weekday == 0:
                x = ((KYCD, "Monday"), x[1])
        elif weekday == 1:
                x = ((KYCD, "Tuseday"), x[1])
        elif weekday == 2:
                x = ((KYCD, "Wednesday"), x[1])
        elif weekday == 3:
                x = ((KYCD, "Thursday"), x[1])
        elif weekday == 4:
                x = ((KYCD, "Friday"), x[1])
        elif weekday == 5:
                x = ((KYCD, "Saturday"), x[1])
        else:
                x = ((KYCD, "Sunday"), x[1])
        return x
 

if __name__ == "__main__":
        sc = SparkContext()
        tuples = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
        tuples = tuples.filter(lambda x : len(x) > 6 and len(x[1].split('/')) > 2).filter(lambda x : x[6] != '' and x[1] != '')
        pair = tuples.map(weekday)
        result = pair.reduceByKey(lambda x, y : x + y).sortByKey()
	result = result.map(numtoweekday)
        result = result.map(lambda x: '%s\t%s\t%s' % (x[0][0], x[0][1],  x[1]))
        result.saveAsTextFile('KYCD_weekday_amount.out')
        sc.stop()
