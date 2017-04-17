from __future__ import print_function

import sys
import heapq
from pyspark import SparkContext
from csv import reader

def topfive(x):
	heap = []
	for t in x[1]:
		if len(heap) >= 5:
			if t[0] > heap[0]:
				heapq.heappop(heap)
				heapq.heappush(heap, t[0])
		else:
			heapq.heappush(heap, t[0])	 
        return (x[0], (x[1], heap[0]))
def selectfive(x):
	for t in x[1][0]:
		if t[0]  < x[1][1]:
			x[0][1].remove(t)
	return x
def formatkey(x):
	s = ''
	for t in x[1][0]:
		s += t[1] + '\t' + t[0] + '\n'
	s = s.strip()
	return (x[0], s)

if __name__ == "__main__":
        sc = SparkContext()
        tuples = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
        tuples = tuples.filter(lambda x : len(x) > 13).filter(lambda x : x[6] != '' and x[13] != '')
        pair = tuples.map(lambda x : ((x[13].strip().upper(), x[6].strip()), 1))
        pair  = pair.reduceByKey(lambda x, y : x + y)
	result = pair.map(lambda x : (x[0][0], (x[1], x[0][1]))).groupByKey().map(topfive)
	result = result.map(selectfive).map(formatkey).sortByKey()
        result = result.map(lambda x: '%s:\n%s' % (x[0], x[1]))
        result.saveAsTextFile('area_top5_KYCD.out')
        sc.stop()
