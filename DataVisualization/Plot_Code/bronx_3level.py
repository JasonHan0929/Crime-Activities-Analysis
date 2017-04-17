#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import matplotlib.pyplot as plt

# The slices will be ordered and plotted counter-clockwise.

fig = plt.figure(figsize=[10, 10])
ax = fig.add_subplot(111)

labels = 'FELONY', 'MISDEMEANOR','VIOLATION'
sizes = [283623,593424,131152]
colors = ['#003366', '#990033', '#FFCC33']
explode = (0, 0, 0, 0)  # explode a slice if required

#plt.pie(sizes, explode=explode, labels=labels, colors=colors,
        #autopct='%1.1f%%', shadow=True)
#draw a circle at the center of pie to make it look like a donut
centre_circle = plt.Circle((0,0),0.5,color='white', fc='white',linewidth=1.00)
plt.text(0,0,"Bronx",fontsize=22,color ='#999999',ha='center')
fig = plt.gcf()
fig.gca().add_artist(centre_circle)       
pie_wedge_collection = ax.pie(sizes, colors=colors, labels=labels, autopct='%1.0f%%', labeldistance=1.2);

for pie_wedge in pie_wedge_collection[0]:
    pie_wedge.set_edgecolor('#999999')
        
# Set aspect ratio to be equal so that pie is drawn as a circle.
plt.axis('equal')
plt.show()