#!/usr/bin/env python

from scipy.stats.stats import pearsonr
import csv

crime = []
vehicle = []
year = []

with open("manhattan_month_amount.csv") as crime_per_capita:
	tmp = csv.reader(crime_per_capita, delimiter=',')
	for row in tmp:
		year.append(row[0])
		row_float = [float(i) for i in row[1:]]
		crime += row_float


with open("vehicle_collision_amout.csv") as vehicles_per_capita:
	tmp = csv.reader(vehicles_per_capita, delimiter=',')
	for row in tmp:
		three_tmp = [float(i) for i in row[1:]]
		vehicle += three_tmp


print(len(crime))
print(len(vehicle))

for i in range(0, len(year)):
	if year[i] == '2012':
	     co = pearsonr(crime[6 : 12], vehicle[6 : 12])
	else:
		co = pearsonr(crime[0 + 12 * i : 12 + 12 * i], vehicle[0 + 12 * i : 12 + 12 * i])
	print('%s: %s' % (year[i], co))
