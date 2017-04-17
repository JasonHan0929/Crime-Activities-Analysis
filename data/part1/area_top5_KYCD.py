from __future__ import print_function

import sys
import heapq
from pyspark import SparkContext
from csv import reader

def topfive(x):
        heap = []
        for t in x[1]:
                if len(heap) >= 5:
                        if t[1] > heap[0]:
                                heapq.heappop(heap)
                                heapq.heappush(heap, t[1])
                else:
                        heapq.heappush(heap, t[1])
	l = []
	for t in x[1]:
		if t[1] >= heap[0]:
			l.append(t)
        return (x[0], l)
def output(x):
        s = ''
	for t in x[1]:
		s += str(t[0]) + '\t' + str(t[1]) + '\n'
	return s[:-1]
        

if __name__ == "__main__":
        sc = SparkContext()
        tuples = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
        tuples = tuples.filter(lambda x : len(x) > 13).filter(lambda x : x[6] != '' and x[13] != '')
        pair = tuples.map(lambda x : ((x[13].strip().upper(), x[6].strip()), 1))
        pair  = pair.reduceByKey(lambda x, y : x + y)
        pair = pair.map(lambda x : (x[0][0], (x[0][1], x[1]))).groupByKey().map(topfive)

        bronx = pair.filter(lambda x : x[0] == 'BRONX').map(output)
        bronx.saveAsTextFile('bronx_top5_KYCD.out')

        brooklyn = pair.filter(lambda x : x[0] == 'BROOKLYN').map(output)
        brooklyn.saveAsTextFile('brooklyn_top5_KYCD.out')

        manhattan = pair.filter(lambda x : x[0] == 'MANHATTAN').map(output)
        manhattan.saveAsTextFile('manhattan_top5_KYCD.out')

        queens = pair.filter(lambda x : x[0] == 'QUEENS').map(output)
        queens.saveAsTextFile('queens_top5_KYCD.out')

        SI = pair.filter(lambda x : x[0] == 'STATEN ISLAND').map(output)
        SI.saveAsTextFile('statenisland_top5_KYCD.out')

        sc.stop()

