#!/usr/bin/env python

import os
import datetime
import pandas as pd
import csv

RANGE = range(-10, 11)
RANGE.insert(0, -1000)
RANGE.append(1000)
PERIOD = 20
GAIN = 10
PATH = 'data/'
OUTPATH = 'out/'


def init_dict():
    ret = {}
    for i in range(len(RANGE)-1):
        for j in range(len(RANGE)-1):
            key = "[%d,%d][%d,%d]" % (RANGE[i], RANGE[i+1], RANGE[j], RANGE[j+1])
            ret[key] = 0
    return ret


def find_range(ma10, ma100):
    for i in range(len(RANGE)-1):
        for j in range(len(RANGE)-1):
            if RANGE[i] < ma10 < RANGE[i+1] and RANGE[j] < ma100 < RANGE[j+1]:
                return "[%d,%d][%d,%d]" % (RANGE[i], RANGE[i+1], RANGE[j], RANGE[j+1])
    return "error"


def zhang_10pct(df, date):
    pp = df.loc[date:date + PERIOD*datetime.timedelta(1), 'Close']
    chg = (pp.max() - pp[0]) / pp[0]
    if chg > 0.1:
        return True
    return False


def update_result(df, date, result):
    ma10 = df.loc[date, '10_MAC']
    ma100 = df.loc[date, '100_MAC']
    key = find_range(ma10, ma100)
    if key == "error":
        return
    result[key] += 1


def calculation(symbol):
    in_file = os.path.join('data', symbol + '.csv')
    df = pd.read_csv(in_file, index_col='Date', parse_dates=True, usecols=['Date', 'Close', '10_MAC', '100_MAC'])

    result = init_dict()

    for date in df.index:
        if zhang_10pct(df, date):
            update_result(df, date, result)

    writer = csv.writer(open(OUTPATH + symbol + '.csv', 'wb'))
    for key, value in result.items():
        writer.writerow([key, value])
    return

if __name__ == '__main__':
    if not os.path.exists('out'):
        os.makedirs('out')

    for files in os.listdir(PATH):
        symbol, ext = os.path.splitext(files)
        if ext == '.csv':
            print "working on " + symbol
            calculation(symbol)

