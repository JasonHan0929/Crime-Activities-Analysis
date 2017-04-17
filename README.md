# Big-Data-Project

## Team members:
Chong Han  ch2905@nyu.edu, Mingzhong Dai  md3797@nyu.edu, Yingbing Wang yw2848@nyu.edu	      


## Project description
This project discusses about crime information in New York. Mainly, it contains 3 parts: data cleaning, data description and data visualization. Each of these will be described below.   
More information can be found on official webpage:   
https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i

## Instructions for running code
### Part 1
This part corresponds to scripts in DataCleaning, which aims to generate type information of data and create clean data.   
First of all, you need to download crime data from official website. After downloading, you will have rows.csv. Later we will ignore the first line for later task.   
The running sequences are  
1. run dumbo_count.py with five_million_lines_data.csv. The type information will locate locally at result.txt
2. run dumbo_clean_data.py with rows.csv. The clean data will be clean_rows.csv
3. run dumbo_cell_information with rows.csv. The cell information will be in cell_information.csv

```bash
wget https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv?accessType=DOWNLOAD
tail -n +1 rows.csv > five_million_lines_data.csv
spark-sumbmit dumbo_count.py
spark-sumbmit dumbo_clean_data.py
spark-sumbmit dumbo_cell_information.py
```


### Part 2
This part corresponds to scripts in DataStatistics. All you need is to run  execute.sh.  
To begin with, ensure that you have already in this file. And suppose the input file(.csv) in your hadoop is called InputFile.  
```
./execute.sh all InputFile                  //get all the data in DataStatistics   
./execute.sh part1/part2/part3 InputFIle    //get the data in particular part
./execute.sh Num InputFile                  //get the data of particular script
```

The relationship between the number and script are displayed below:  
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

### Part 3
This part corresponds to scripts in DataVisualization.   
