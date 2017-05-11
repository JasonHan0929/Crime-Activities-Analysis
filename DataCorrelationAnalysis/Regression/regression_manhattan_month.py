#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from sklearn import linear_model
import csv

x_monthly_temp_census = []
y_month_crime = []

with open("2006-2010_manhattan_month_crime.csv") as csvfile:
	read_line = csv.reader(csvfile,delimiter=',')
	for row in read_line:
		y_month_crime.append(int(row[0]))
		row[1] = float(row[1])
		row[2] = float(row[2])
		x_monthly_temp_census.append([row[1],row[2]])

reg = linear_model.LinearRegression()

reg.fit(x_monthly_temp_census, y_month_crime)
#LinearRegression(copy_X=True, fit_intercept=True, n_jobs=1, normalize=False)
print(reg.coef_)
r_square = reg.score(x_monthly_temp_census, y_month_crime)
print(r_square)