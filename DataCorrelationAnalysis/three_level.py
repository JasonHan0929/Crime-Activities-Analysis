#!/usr/bin/env python

from scipy.stats.stats import pearsonr
import csv

crime_data_06_15 = []
three_level_06_15 = []

with open("crime_year_total_06_15.csv") as crime:
	tmp = csv.reader(crime, delimiter=',')
	for row in tmp:
		row_float = [float(i) for i in row[1:]]
		crime_data_06_15 += row_float


with open("three_level_06_15.csv") as three_level:
	tmp = csv.reader(three_level, delimiter=',')
	for row in tmp:
		three_tmp = [float(i) for i in row[1:]]
		three_level_06_15 += three_tmp


print(len(crime_data_06_15))
print(len(three_level_06_15))
# print(crime_data_06_15)
# print(three_level_06_15)

co = pearsonr(crime_data_06_15[50:60], three_level_06_15[0:10])
print(co)
co = pearsonr(crime_data_06_15[50:60], three_level_06_15[10:20])
print(co)
co = pearsonr(crime_data_06_15[50:60], three_level_06_15[20:30])
print(co)
# print(crime_data_06_15[50:60])
# print(three_level_06_15[0:10])



# with open("crime_year_06_15.csv") as crime:
# 	tmp = csv.reader(crime, delimiter=',')
# 	for row in tmp:
# 		crime_data.append(float(row[1]))

# with open("census_year_06_15.csv") as census:
# 	tmp = csv.reader(census, delimiter=',')
# 	for row in tmp:
# 		census_data.append(float(row[1]))
