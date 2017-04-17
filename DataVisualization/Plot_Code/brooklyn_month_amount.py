#!/usr/bin/env python

import matplotlib.pyplot as plt
import csv


color_sequence = ['#1f77b4', '#aec7e8', '#ff7f0e', '#ffbb78', '#2ca02c',
                  '#98df8a', '#d62728', '#ff9896', '#9467bd', '#c5b0d5',
                  '#8c564b', '#c49c94', '#e377c2', '#f7b6d2', '#7f7f7f',
                  '#c7c7c7', '#bcbd22', '#dbdb8d', '#17becf', '#9edae5']
fig, ax = plt.subplots(1, 1, figsize=(13, 15))

ax.spines['top'].set_visible(False)
ax.spines['bottom'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)

ax.get_xaxis().tick_bottom()
ax.get_yaxis().tick_left()

fig.subplots_adjust(left=.10, right=.75, bottom=.10, top=.94)

ax.set_xlim(1, 13)
ax.set_ylim(8221, 13453)

plt.xticks(range(1, 13, 1), fontsize=14)
plt.yticks(range(8221,13453,500), fontsize=14)
ax.xaxis.set_major_formatter(plt.FuncFormatter('{:.0f}'.format))
ax.yaxis.set_major_formatter(plt.FuncFormatter('{:.0f}'.format))

plt.grid(True, 'major', 'y', ls='--', lw=.5, c='k', alpha=.3)

# Remove the tick marks; they are unnecessary with the tick lines we just
# plotted.
plt.tick_params(axis='both', which='both', bottom='off', top='off',
                labelbottom='on', left='off', right='off', labelleft='on')

x = list(range(1,13))

with open('brooklyn_month_amount.csv','r') as csvfile:
	plots = csv.reader(csvfile, delimiter=',')
	row_number = 0
	offset = 0.5
	for row in plots:
		y = []
		y = [int(i)for i in row[1:13]]
		line = plt.plot(x,y,lw=2.5,
                    color=color_sequence[row_number],label = row[0])
		#y_pos = y[-1] - offset
		#plt.text(12.99, y_pos, row[0], fontsize=14, color=color_sequence[row_number])
		row_number+=1
		#offset +=0.2
plt.xlabel('Month')
plt.ylabel('Amount')
plt.legend(loc='upper right')
fig.suptitle('Monthly Amount of Crime in Brooklyn (2006-2015)\n', fontsize=18, ha='center')
plt.show()



