from __future__ import print_function
from datetime import datetime
from pyspark import SparkContext
from csv import reader


def name_of_type(x):
    if not x:
        return "null"
    try:
        int(x)
        return "int"
    except ValueError:
        pass
    try:
        float(x)
        return "decimal"
    except ValueError:
        pass
    try:
        datetime.strptime(x, '%m/%d/%Y')
        return "date"
    except ValueError:
        pass
    try:
        datetime.strptime(x, '%H:%M:%S')
        return "time"
    except ValueError:
        pass
    return "string"


def check_ky_cd(line):
    field = line[6].strip()
    if field:
        return field
    else:
        return "NULL"


def check_law_cat_cd(x):
    if x:
        if x == "FELONY" or x == "MISDEMEANOR" or x == "VIOLATION":
            return x
        else:
            return ("INVALID",x)
    else:
        return "NULL"


def check_boro_nm(x):
    if x:
        if x == "MANHATTAN" or x == "BROOKLYN" or x == "QUEENS" or x == "BRONX" or x == "STATEN ISLAND":
            return x
        else:
            return ("INVALID",x)
    else:
        return "NULL"


def save_result(cells, i):
    with open("result.txt", "a") as f:
        f.write("------------------\nColumn %s contains: \n" % i)
        for (cell, count) in cells:
            f.write("%s: %i\n" % (cell, count))


# column 11
def law_cat_cd(col):
    sc = SparkContext()
    lines = sc.textFile(file_name, 1)
    cells = lines.mapPartitions(lambda x: reader(x)).map(lambda x: (check_law_cat_cd(x[col]), 1)).reduceByKey(lambda x, y: x + y).collect()
    save_result(cells, col)
    sc.stop()


# column 13
def boro_nm(col):
    sc = SparkContext()
    lines = sc.textFile(file_name, 1)
    cells = lines.mapPartitions(lambda x: reader(x)). \
        map(lambda x: (check_boro_nm(x[col]), 1)).reduceByKey(lambda x, y: x + y).collect()
    save_result(cells, col)
    sc.stop()


if __name__ == "__main__":
    file_name = "five_million_lines_data.csv"
    with open("result.txt", "w") as f:
        f.write("Type Information: \n")
    sc = SparkContext()
    for i in range(24):
        lines = sc.textFile(file_name, 1)
        cells = lines.mapPartitions(lambda x: reader(x)). \
            map(lambda x: (name_of_type(x[i]), 1)).reduceByKey(lambda x, y: x + y).collect()
        with open("result.txt", "a") as f:
            f.write("------------------\nColumn %s contains: \n" % i)
            for (cell, count) in cells:
                f.write("%s, %i\n" % (cell, count))
    sc.stop()
    with open("result.txt", "a") as f:
        f.write("\n\nValue Information: \n")

    law_cat_cd(11)
    boro_nm(13)
