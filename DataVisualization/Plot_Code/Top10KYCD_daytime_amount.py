#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from __future__ import division
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import cnames
#colors = dict(mcolors.BASE_COLORS, **mcolors.CSS4_COLORS)

fig, ax = plt.subplots(1, 1, figsize=(12, 14))
ax.xaxis.set_major_formatter(plt.FuncFormatter('{:.0f}'.format))
ax.yaxis.set_major_formatter(plt.FuncFormatter('{:.0f}'.format))

N = 10
daytime = (408201,328413,211136,231752,177743,97340,70888,72876,65478,112077)
night = (249049,194425,169765,115405,154156,132215,60128,65356,58657,47099)
midnight = (85754,79034,139581,66177,100065,56035,63232,60129,59617,31678)

ind = np.arange(N)
width = 0.5
p1 = plt.bar(ind,daytime,width,color ='#CCCC99',edgecolor = "none",align="center")
p2 = plt.bar(ind,night,width, bottom =daytime,color ='#CC9900',edgecolor = "none",align="center")
p3 = plt.bar(ind,midnight,width,bottom = np.array(daytime)+np.array(night),color = '#660000',edgecolor = "none",align="center")

plt.ylabel("Amount")
plt.title("Time Slots of Happened Time (Top 10 Crime Type)")
plt.xticks(ind, ('PETIT \n LARCENY','HARRASSMENT\n 2','ASSAULT3 & \n RELATED\n OFFENSES','GRAND\n LARCENY','CRIMINAL\n MISCHIEF & \n RELATED OF','DANGEROUS\n DRUGS','OFF. AGNST\n PUB\n ORD SENSBLTY','ROBBERY','FELONY\n ASSAULT','BURGLARY'))
plt.yticks(np.arange(0,750000,80000))
plt.legend((p1[0],p2[0],p3[0]),("daytime","night","midnight"))

plt.show()