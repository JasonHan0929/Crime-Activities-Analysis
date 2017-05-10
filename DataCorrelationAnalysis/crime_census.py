#!/usr/bin/env python

from scipy.stats.stats import pearsonr
import csv

crime_data_06_15 = []
census_data_06_15 = []

with open("crime_year_06_15.csv") as crime:
	tmp = csv.reader(crime, delimiter=',')
	for row in tmp:
		row_float = [float(i) for i in row[1:]]
		crime_data_06_15 += row_float


with open("census_year_06_15.csv") as census:
	tmp = csv.reader(census, delimiter=',')
	for row in tmp:
		row_float = [float(i) for i in row[1:]]
		census_data_06_15 += row_float


print(len(crime_data_06_15))
print(len(census_data_06_15))


print(crime_data_06_15)
print(census_data_06_15)
co = pearsonr(crime_data_06_15,census_data_06_15)
print(co)




# with open("crime_year_06_15.csv") as crime:
# 	tmp = csv.reader(crime, delimiter=',')
# 	for row in tmp:
# 		crime_data.append(float(row[1]))

# with open("census_year_06_15.csv") as census:
# 	tmp = csv.reader(census, delimiter=',')
# 	for row in tmp:
# 		census_data.append(float(row[1]))
