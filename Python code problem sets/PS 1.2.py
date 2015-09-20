# -*- coding: utf-8 -*-
# Find the time and value of max load for each of the regions
# COAST, EAST, FAR_WEST, NORTH, NORTH_C, SOUTHERN, SOUTH_C, WEST
# and write the result out in a csv file, using pipe character | as the delimiter.
# An example output can be seen in the "example.csv" file.

import xlrd
import os
import csv
from zipfile import ZipFile
import numpy as np
import pandas as pd

datafile = "2013_ERCOT_Hourly_Load_Data.xls"
outfile = "2013_Max_Loads.csv"


def open_zip(datafile):
    with ZipFile('{0}.zip'.format(datafile), 'r') as myzip:
        myzip.extractall()


def parse_file(datafile):
    workbook = xlrd.open_workbook(datafile)
    sheet = workbook.sheet_by_index(0)
    dictHelp = {}
    data = {'Station': [], 'Year': [], 'Month': [], 'Day': [], 'Hour': [], 'Max Load': []}
    # 0. Initialize dictionary of import data
    for i in range(sheet.ncols):
        dictHelp[sheet.cell_value(0,i)] = []
    # 1. Save data as dictionary
    for i in range(sheet.ncols):
        for j in range(1,sheet.nrows):
            dictHelp[sheet.cell_value(0,i)].append(sheet.cell_value(j,i))
        
    # 2. Populate output dictionary
    for key in dictHelp.iterkeys():
        if not(key == 'ERCOT' or key == 'Hour_End'):
            data['Station'].append(key)
            data['Max Load'].append(np.max(dictHelp[key]))
            Year, Month, Day, Hour, Minute, Second = xlrd.xldate_as_tuple(
                    dictHelp['Hour_End'][dictHelp[key].index(np.max(dictHelp[key]))],0)
            data['Year'].append(Year)
            data['Month'].append(Month)
            data['Day'].append(Day)
            data['Hour'].append(Hour)
    # 3. Save as dataframe
    data = pd.DataFrame(data)
    cols = data.columns.tolist()
    cols = cols[4:5] + cols[-1:] + cols[3:4] + cols[0:1] + cols[1:2] + cols[2:3]
    data = data[cols]
    return data


def save_file(data, filename):
    data.to_csv(outfile, sep = '|', index = False)

    
def test():
    open_zip(datafile)
    data = parse_file(datafile)
    save_file(data, outfile)

    number_of_rows = 0
    stations = []

    ans = {'FAR_WEST': {'Max Load': '2281.2722140000024',
                        'Year': '2013',
                        'Month': '6',
                        'Day': '26',
                        'Hour': '17'}}
    correct_stations = ['COAST', 'EAST', 'FAR_WEST', 'NORTH',
                        'NORTH_C', 'SOUTHERN', 'SOUTH_C', 'WEST']
    fields = ['Year', 'Month', 'Day', 'Hour', 'Max Load']

    with open(outfile) as of:
        csvfile = csv.DictReader(of, delimiter="|")
        for line in csvfile:
            station = line['Station']
            if station == 'FAR_WEST':
                for field in fields:
                    # Check if 'Max Load' is within .1 of answer
                    if field == 'Max Load':
                        max_answer = round(float(ans[station][field]), 1)
                        max_line = round(float(line[field]), 1)
                        assert max_answer == max_line

                    # Otherwise check for equality
                    else:
                        assert ans[station][field] == line[field]

            number_of_rows += 1
            stations.append(station)

        # Output should be 8 lines not including header
        assert number_of_rows == 8

        # Check Station Names
        assert set(stations) == set(correct_stations)

        
if __name__ == "__main__":
    test()
