#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from pydoc import help
from scipy.stats.stats import pearsonr
import csv


month_temperature_2006_2015 = []
manhattan_month_crime_2006_2015 = []
with open("2006-2015_monthly_temp.csv") as csvfile_temp:
	readCSV_temp = csv.reader(csvfile_temp, delimiter=',')
	for row in readCSV_temp:
		month_temperature_2006_2015.append(float(row[1]))

with open("manhattan_month_amount.csv") as csvfile_crime:
	readCSV_crime = csv.reader(csvfile_crime, delimiter=',')
	for row in readCSV_crime:
		row_float = [float(i) for i in row[1:]]
		manhattan_month_crime_2006_2015+=row_float


co = pearsonr(month_temperature_2006_2015,manhattan_month_crime_2006_2015)
print(co)
