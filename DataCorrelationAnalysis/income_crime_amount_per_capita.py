#!/usr/bin/env python

from scipy.stats.stats import pearsonr
import csv

crime = []
income = []
area = []

with open("crime_per_capita.csv") as crime_per_capita:
	tmp = csv.reader(crime_per_capita, delimiter=',')
	for row in tmp:
		row_float = [float(i) for i in row[1:]]
		crime += row_float


with open("income_per_capita.csv") as income_per_capita:
	tmp = csv.reader(income_per_capita, delimiter=',')
	for row in tmp:
		three_tmp = [float(i) for i in row[1:]]
		area.append(row[0])
		income += three_tmp


print(len(crime))
print(len(income))

for i in range(0, len(area)):
	co = pearsonr(crime[0 + 7 * i : 7 + 7 * i], income[0 + 7 * i : 7 + 7 * i])
	print('%s: %s' % (area[i], co))
