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
def output(x):
	dic = {}
	for t in x[1]:
		dic[t[0]] = t[1]
	for t in range(0, 3):
		if not dic.has_key(t):
			dic[t] = 0
	s = ''
	for t in range(0, 3):
		s += str(dic[t]) + ','
	return (x[0], s[:-1])

if __name__ == "__main__":
        sc = SparkContext()
        tuples = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
        tuples = tuples.filter(lambda x : len(x) > 6).filter(lambda x : x[6] != ''  and x[2] != '')
        pair = tuples.map(devidetime)
        result = pair.reduceByKey(lambda x, y : x + y).map(lambda x : (x[0][0], (x[0][1], x[1]))).groupByKey().map(output)
        result = result.map(lambda x: '%s\t%s' % (x[0], x[1]))
        result.saveAsTextFile('KYCD_daytime_amount.out')
        sc.stop()

