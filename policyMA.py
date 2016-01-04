#!/usr/bin/env python

import sp500data
import os
import datetime
import pandas as pd

RANGE = range(-1, 2)
RANGE.insert(0, -1000)
RANGE.append(1000)
PERIOD = 20
GAIN = 10


def init_dict():
    dict = {}
    for i in range(len(RANGE)-1):
        for j in range(len(RANGE)-1):
            key = "[%d,%d][%d,%d]" % (RANGE[i], RANGE[i+1], RANGE[j], RANGE[j+1])
            dict[key] = 0
    return dict


def find_range(ma10, ma100):
    for i in range(len(RANGE)-1):
        for j in range(len(RANGE)-1):
            if RANGE[i] < ma10 and ma10 > RANGE[i+1] and RANGE[j] < ma100 and ma100 > RANGE[j+1]:
                return "[%d,%d][%d,%d]" % (RANGE[i], RANGE[i+1], RANGE[j], RANGE[j+1])
    return "error"


def zhang_10pct():
    return True


def calculation(symbol):
    in_file = os.path.join('data', symbol + '.csv')
    df = pd.read_csv(in_file, index_col='Date', parse_dates=True, usecols=['Date', 'Close', '10_MAC', '100_MAC'])
    # print df.info()

    dict = init_dict()

    for date in df.index:
        print date
    # print dict
    # dates = pd.bdate_range(start=sp500data.START, end=sp500data.END, closed='left')
    # print dates
    # for date in dates:
    #     print df.loc[date]

    # rng = pd.bdate_range(start=sp500data.START, periods=PERIOD)
    # df1 = df.loc[rng, 'Close']
    # print df1[0]
    # print df1.max()

    # for index, row in df.iterrows():
    #     print row

    # for row in df:
    #     if(>10% in 20 days):
    #         update_dict(ma10, ma100 ,)


if __name__ == '__main__':
    path = 'data/'
    for file in os.listdir(path):
        symbol = os.path.splitext(file)[0]
        print "working on " + symbol
        calculation(symbol)
        break

