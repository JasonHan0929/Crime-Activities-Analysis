from __future__ import print_function

import sys
import string
import re
from operator import add
from pyspark import SparkContext
from csv import reader

# remember to remove the first line(fields) of csv.

def borough_total(line):
	year_list = ["2002","2003","2004","2005","2006"]
	demographic = line[0].strip()
	year = line[2].strip()
	borough = line[1].strip()
	graduate_percent = line[5].strip()
	if demographic == "Borough Total" and year in year_list: 
		return (borough+"-"+str(int(year)+4),graduate_percent)
	else:
		return ("Not relevant",1)


if __name__ == "__main__":
	if len(sys.argv) != 2:
        print("Usage: bigram <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    line = sc.textFile(sys.argv[1], 1)
    line = line.mapPartitions(lambda x: reader(x))
    line = line.map(borough_total).filter(lambda x:x[0]!= "Not relevant").sortByKey()
    line.saveAsTextFile("2006-2010_borough_total_graduation.out")

    sc.stop()