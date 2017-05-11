from __future__ import print_function

import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
        sc = SparkContext()
        tuples = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
        pairs = tuples.filter(lambda x: len(x) > 1).map(lambda x : (x[0], x[1])).filter(lambda x : len(x) < 7)
        result = pairs.map(lambda x: '%s\t%s' % (x[0], x[1]))
        result.saveAsTextFile("vehicle_collision_amout.out")
        sc.stop()


