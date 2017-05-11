#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from sklearn import linear_model
import csv

x_income_census = []
y_crime_per_capita = []

with open("2009-2015_manhattan_income_crime.csv") as csvfile:
	read_line = csv.reader(csvfile,delimiter=',')
	for row in read_line:
		y_crime_per_capita.append(float(row[0]))
		row[1] = float(row[1])
		row[2] = float(row[2])
		x_income_census.append([row[1],row[2]])

reg = linear_model.LinearRegression()

reg.fit(x_income_census, y_crime_per_capita)
#LinearRegression(copy_X=True, fit_intercept=True, n_jobs=1, normalize=False)
print(reg.coef_)
r_square = reg.score(x_income_census, y_crime_per_capita)
print(r_square)