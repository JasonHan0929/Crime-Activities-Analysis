# Big-Data-Project

## Team Members:
Chong Han  ch2905  
Mingzhong Dai  md3797  
Yingbing Wang yw2848	      

## Instructions for Running Code
For code in DataStatistics, you could run the execute.sh to get the date you want. 
At first, ensure that you are already in this file.
Suppose the input file(.csv) in your hadoop is called InputFile

1.to get all the data in DataStatistics:
	./execute.sh all InputFile
2.to get the data in particular part:
	./execute.sh part1/part2/part3 InputFIle
3.to get the data of particular script:
	./execute.sh Num InputFile

The relationship between number and script are displayed below:

1			part1/area_total_amount.py
2			part1/area_year_amount.py
3			part1/area_month_amount.py
4			part1/area_top5_KYCD.py
5			part1/area_level_amount.py
6			part2/KYCD_month_amount.py
7			part2/KYCD_total_amount.py
8			part2/KYCD_status_amount.py 
9			part2/KYCD_year_amount.py
10			part2/KYCD_weekday_amount.py
11			part2/KYCD_daytime_amount.py
12			part3/level_total_amount.py
13			part3/level_status_amount.py
14			part3/level_status_amount.py
15			part3/level_month_amount.py
16			part3/level_year_amount.py

After you run the shell script, its output will store in the file called results in corresponding part.


