# import csv
# import sys
#
# with open('output_gsmarena_clean.csv') as csvfile:
#     readCSV = csv.reader(csvfile, delimiter='|')
#     dates = []
#     colors = []
#     i = 0
#     for row in readCSV:
#         if (i == 0):
#             print(row)
#         else:
#             sys.exit(1)
#         i = i + 1;

        # color = row[3]
        # date = row[0]
        #
        # dates.append(date)
        # colors.append(color)

    # print(dates)
    # print(colors)

# !/usr/bin/env python

import csv
import re
import sys
import pprint


# Function to convert a csv file to a list of dictionaries.  Takes in one variable called "variables_file"

def csv_dict_list(variables_file):
    # Open variable-based csv, iterate over the rows and map values to a list of dictionaries containing key/value pairs

    reader = csv.DictReader(open(variables_file, 'rb'), delimiter='|')
    dict_list = []
    for line in reader:
        dict_list.append(line)
    return dict_list


# Calls the csv_dict_list function, passing the named csv

device_values = csv_dict_list('output_gsmarena_clean.csv')

# Prints the results nice and pretty like
total_rows = 0
for row in device_values:
    if (total_rows < 10):
        #print(row['os'])
        #print(re.search('v[0-9]',row['os']).group(0))
        print(re.sub(',[ ()a-zA-Z0-9.]*','',row['os']))
        #print(re.sub(' [a-zA-Z0-9]*','',row['DeviceName'])) # replace spasi+(huruf/angka yg ada isinya atau lebih)

    total_rows = total_rows + 1

