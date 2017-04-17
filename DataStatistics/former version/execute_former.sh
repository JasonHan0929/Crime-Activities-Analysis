#!/bin/bash
num1="1"
num2="2"
num3="3" 
num4="4"
num5="5"
num6="6"
num7="7"
num8="8"
num9="9"
num10="10"
num11="11"
num12="12"
num13="13"
num14="14"
num15="15"
part1="part1"
part2="part2"
part3="part3"
all="all"

if [ $1 = $num1 -o $1 = $all -o $1 = $part1 ]
then 
	hadoop fs -rm -r area_total_amount.out
	spark-submit part1/area_total_amount.py data.csv
	hadoop fs -getmerge area_total_amount.out part1/results/area_total_amount.out
fi

if [ $1 = $num2 -o $1 = $all -o $1 = $part1 ]
then
        hadoop fs -rm -r area_year_amount.out
        spark-submit part1/area_year_amount.py data.csv
        hadoop fs -getmerge area_year_amount.out part1/results/area_year_amount.out
fi

if [ $1 = $num3 -o $1 = $all -o $1 = $part1 ]
then
        hadoop fs -rm -r area_month_amount.out
        spark-submit part1/area_month_amount.py data.csv
        hadoop fs -getmerge area_month_amount.out part1/results/area_month_amount.out
fi

if [ $1 = $num4 -o $1 = $all -o $1 = $part1 ]
then
        hadoop fs -rm -r area_top5_KYCD.out
        spark-submit part1/area_top5_KYCD.py data.csv
        hadoop fs -getmerge area_top5_KYCD.out part1/results/area_top5_KYCD.out
fi

if [ $1 = $num5 -o $1 = $all -o $1 = $part1 ]
then
        hadoop fs -rm -r area_level_amount.out
        spark-submit part1/area_level_amount.py data.csv
        hadoop fs -getmerge area_level_amount.out part1/results/area_level_amount.out
fi

if [ $1 = $num6 -o $1 = $all -o $1 = $part2 ]
then
        hadoop fs -rm -r KYCD_month_amount.out
        spark-submit part2/KYCD_month_amount.py data.csv
        hadoop fs -getmerge KYCD_month_amount.out part2/results/KYCD_month_amount.out
fi

if [ $1 = $num7 -o $1 = $all -o $1 = $part2 ]
then
        hadoop fs -rm -r KYCD_total_amount.out
        spark-submit part2/KYCD_total_amount.py data.csv
        hadoop fs -getmerge KYCD_total_amount.out part2/results/KYCD_total_amount.out
fi

if [ $1 = $num8 -o $1 = $all -o $1 = $part2 ]
then
        hadoop fs -rm -r KYCD_status_amount.out
        spark-submit part2/KYCD_status_amount.py data.csv
        hadoop fs -getmerge KYCD_status_amount.out part2/results/KYCD_status_amount.out
fi

if [ $1 = $num9 -o $1 = $all -o $1 = $part2 ]
then
        hadoop fs -rm -r KYCD_year_amount.out
        spark-submit part2/KYCD_year_amount.py data.csv
        hadoop fs -getmerge KYCD_year_amount.out part2/results/KYCD_year_amount.out
fi

if [ $1 = $num10 -o $1 = $all -o $1 = $part2 ]
then
        hadoop fs -rm -r KYCD_weekday_amount.out
        spark-submit part2/KYCD_weekday_amount.py data.csv
        hadoop fs -getmerge KYCD_weekday_amount.out part2/results/KYCD_weekday_amount.out
fi

if [ $1 = $num11 -o $1 = $all -o $1 = $part2 ]
then
        hadoop fs -rm -r KYCD_daytime_amount.out
        spark-submit part2/KYCD_daytime_amount.py data.csv
        hadoop fs -getmerge KYCD_daytime_amount.out part2/results/KYCD_daytime_amount.out
fi

if [ $1 = $num12 -o $1 = $all -o $1 = $part2 ]
then
        hadoop fs -rm -r KYCD_report_amount.out
        spark-submit part2/KYCD_report_amount.py data.csv
        hadoop fs -getmerge KYCD_report_amount.out part2/results/KYCD_report_amount.out
fi

if [ $1 = $num13 -o $1 = $all -o $1 = $part3 ]
then
        hadoop fs -rm -r level_total_amount.out
        spark-submit part3/level_total_amount.py data.csv
        hadoop fs -getmerge level_total_amount.out part3/results/level_total_amount.out
fi

if [ $1 = $num14 -o $1 = $all -o $1 = $part3 ]
then
        hadoop fs -rm -r level_status_amount.out
        spark-submit part3/level_status_amount.py data.csv
        hadoop fs -getmerge level_status_amount.out part3/results/level_status_amount.out
fi

if [ $1 = $num15 -o $1 = $all -o $1 = $part3 ]
then
        hadoop fs -rm -r level_month_amount.out
        spark-submit part3/level_month_amount.py data.csv
        hadoop fs -getmerge level_month_amount.out part3/results/level_month_amount.out
fi
