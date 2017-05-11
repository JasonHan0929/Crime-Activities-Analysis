#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from pydoc import help
from scipy.stats.stats import pearsonr
import csv

crime_per_capita_2006_2010 = []
female_graduation_2006_2010 = []

with open("2006-2010_crime_per_capita.csv") as csvfile_cpc:
	readCSV_cpc = csv.reader(csvfile_cpc, delimiter=',')
	for row in readCSV_cpc:
		row_float = [float(i) for i in row[1:]]
		crime_per_capita_2006_2010+= row_float

with open("2006-2010_female_graduation.csv") as csvfile_gra:
	readCSV_gra = csv.reader(csvfile_gra, delimiter=',')
	for row in readCSV_gra:
		female_graduation_2006_2010.append(float(row[1]))


co = pearsonr(crime_per_capita_2006_2010,female_graduation_2006_2010)
print(co)
