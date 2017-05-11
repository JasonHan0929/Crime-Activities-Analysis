from __future__ import print_function

import sys
import string
import re
from operator import add
from pyspark import SparkContext
from csv import reader

# remember to remove the first line(fields) of csv.
def monthly_temp(line):
	date_ymd = line[2].strip()
	date_ym = date_ymd[:6]
	day_temp = float(line[3].strip())
	return (date_ym, day_temp)


def count_monthly_avg(line):
	count = len(line[1])
	total = sum(line[1])
	avg = float(total/count)
	return (line[0],avg)


if __name__ == "__main__":
	if len(sys.argv) != 2:
        print("Usage: bigram <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    line = sc.textFile(sys.argv[1], 1)
    line = line.mapPartitions(lambda x: reader(x))
    line = line.map(monthly_temp).groupByKey().mapValues(list).map(count_monthly_avg).sortByKey()
    line.saveAsTextFile("2006-2015_monthly_temp.out")

    sc.stop()