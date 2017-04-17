#!/usr/bin/env python

import matplotlib.pyplot as plt
import csv


color_sequence = ['#1f77b4', '#ffbb78','#2ca02c','#ff9896','#7f7f7f']

fig, ax = plt.subplots(1, 1, figsize=(12, 14))

ax.spines['top'].set_visible(False)
ax.spines['bottom'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)

ax.get_xaxis().tick_bottom()
ax.get_yaxis().tick_left()

fig.subplots_adjust(left=.10, right=.75, bottom=.10, top=.94)

ax.set_xlim(2006, 2016)
ax.set_ylim(73, 156000)

plt.xticks(range(2006, 2016, 1), fontsize=14)
plt.yticks(range(73,156000,5000), fontsize=14)
ax.xaxis.set_major_formatter(plt.FuncFormatter('{:.0f}'.format))
ax.yaxis.set_major_formatter(plt.FuncFormatter('{:.0f}'.format))

plt.grid(True, 'major', 'y', ls='--', lw=.5, c='k', alpha=.3)

# Remove the tick marks; they are unnecessary with the tick lines we just
# plotted.
plt.tick_params(axis='both', which='both', bottom='off', top='off',
                labelbottom='on', left='off', right='off', labelleft='on')

x = list(range(2006,2016))

with open('area_year_amount.csv','r') as csvfile:
	plots = csv.reader(csvfile, delimiter=',')
	row_number = 0
	for row in plots:
		y = []
		y = [int(i)for i in row[1:11]]
		line = plt.plot(x,y,lw=2.5,
                    color=color_sequence[row_number])
		y_pos = y[-1] - 0.5
		plt.text(2015.99, y_pos, row[0], fontsize=14, color=color_sequence[row_number])
		row_number+=1
plt.xlabel('Year')
plt.ylabel('Amount')
fig.suptitle('Yearly Amount of Reported Crime in 5 Boroughs of NYC (2006-2015)\n', fontsize=18, ha='center')
plt.legend()
plt.show()



