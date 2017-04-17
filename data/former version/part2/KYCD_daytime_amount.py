from __future__ import print_function

import sys
import time
from pyspark import SparkContext
from csv import reader

def devidetime(x):
        KYCD = x[6].strip()
	temp = x[2].split(":")
	if temp[0] == "24":
		x[2] = ("%s:%s:%s") % (0, temp[1], temp[2])
        time_stamp = time.strptime(x[2].strip(), "%H:%M:%S")
        bound1 = time.strptime("6:00:00", "%H:%M:%S")
	bound2 = time.strptime("17:00:00", "%H:%M:%S")
	bound3 = time.strptime("23:00:00", "%H:%M:%S")
	if (time_stamp > bound1 and time_stamp <= bound2):
        	x = ((KYCD, 0), 1)
	elif (time_stamp > bound2 and time_stamp <= bound3):
		x = ((KYCD, 1), 1)
	else:
		x = ((KYCD, 2), 1)
        return x
def numtodaytime(x):
        KYCD = x[0][0]
        daytime = x[0][1]
        total = x[1]
        if daytime == 0:
                x = ((KYCD, "Daytime"), x[1])
        elif daytime == 1:
                x = ((KYCD, "Night"), x[1])
        else:
                x = ((KYCD, "Midnight"), x[1])
        return x


if __name__ == "__main__":
        sc = SparkContext()
        tuples = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
        tuples = tuples.filter(lambda x : len(x) > 6).filter(lambda x : x[6] != ''  and x[2] != '')
        pair = tuples.map(devidetime)
        result = pair.reduceByKey(lambda x, y : x + y).sortByKey()
        result = result.map(numtodaytime)
        result = result.map(lambda x: '%s\t%s\t%s' % (x[0][0], x[0][1],  x[1]))
        result.saveAsTextFile('KYCD_daytime_amount.out')
        sc.stop()

