#!/usr/bin/env python

import policyMA
import os
import csv
import operator


THRESH = 8


def do_job(s, r):
    csv_path = os.path.join('out', s + '.csv')
    with open(csv_path, "rb") as in_file:
        reader = csv.reader(in_file)
        for row in reader:
            r[row[0]] += int(row[1])


def show_result(r):
    result_x = sorted(r.items(), key=operator.itemgetter(1), reverse=True)
    output = []
    for row in result_x:
        if row[1] > THRESH:
            output.append(row)
    if len(output) > 0:
        writer = csv.writer(open('result' + '.csv', 'wb'))
        writer.writerows(output)


if __name__ == '__main__':
    result = policyMA.init_dict()
    for files in os.listdir(policyMA.OUTPATH):
        symbol, ext = os.path.splitext(files)
        if ext == '.csv':
            print "working on " + symbol
            do_job(symbol, result)
    show_result(result)