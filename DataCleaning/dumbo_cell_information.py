from pyspark import SparkContext
from csv import reader
from csv import writer
from datetime import datetime


try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


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


def write_csv(x):
    output = StringIO("")
    writer(output).writerow(x)
    return output.getvalue().strip()


def check_date(x):
    if not x:
        return "null"
    try:
        datetime.strptime(x, '%H:%M:%S')
        return "valid"
    except:
        return "invalid"


def check_time(x):
    if not x:
        return "null"
    try:
        datetime.strptime(x, '%m/%d/%Y')
        return "valid"
    except:
        return "invalid"


def check_null(x):
    if not x:
        return "null"
    return "valid"


def meta_information(x, semantic):
    for i in range(24):
        x[i] = x[i] + "_" + name_of_type(x[i]) + "_" + semantic[i] + "_" + check_null(x[i])
    x[3] = x[3] + "_" + name_of_type(x[3]) + "_" + semantic[3] + "_" + check_date(x[3])
    x[4] = x[4] + "_" + name_of_type(x[4]) + "_" + semantic[4] + "_" + check_time(x[4])
    return [element for element in x]


if __name__ == "__main__":
    file_name = "clean_rows.csv"
    result_file = "cell_information.csv"
    semantic = ["ID", "date", "time", "date", "time", "date", "code", "code", "code", "descprition",
                "descprition","level", "department", "borough", "code", "location", "place", "park name", "nycha name",
                "X-coordinate", "Y-coordinate", "Latitude", "Longitude", "Latitude & Longitude"]
    sc = SparkContext()
    lines = sc.textFile(file_name, 1)
    cells = lines.mapPartitions(lambda x: reader(x)).map(lambda x: meta_information(x, semantic)).\
            map(lambda x: write_csv(x)).saveAsTextFile(result_file)
    sc.stop()
