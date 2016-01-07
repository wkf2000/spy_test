#!/usr/bin/env python

import urllib2
import pytz
import csv
import os
from bs4 import BeautifulSoup
from datetime import datetime
from pandas_datareader import data as web
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
import shutil


SITE = "http://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
START = datetime(2000, 1, 1, 0, 0, 0, 0, pytz.utc)
END = datetime(2016, 1, 1, 0, 0, 0, 0, pytz.utc)
EXTRA = ['SPY', 'CIEN']


def scrape_list(site):
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = urllib2.Request(site, headers=hdr)
    page = urllib2.urlopen(req)
    soup = BeautifulSoup(page, "lxml")

    table = soup.find('table', {'class': 'wikitable sortable'})
    sector_tickers = dict()
    for row in table.findAll('tr'):
        col = row.findAll('td')
        if len(col) > 0:
            sector = str(col[3].string.strip()).lower().replace(' ', '_')
            ticker = str(col[0].string.strip())
            if sector not in sector_tickers:
                sector_tickers[sector] = list()
            sector_tickers[sector].append(ticker)
    return sector_tickers


def get_snp500():
    sector_tickers = scrape_list(SITE)
    symbols = []
    with open('sp500.csv', 'w') as f:
        writer = csv.writer(f, delimiter=',')
        for sector, tickers in sector_tickers.iteritems():
            print "save sector: " + sector
            for tk in tickers:
                symbols.append(tk)
                writer.writerow([tk, sector])

    print "sp500 symbols downloaded"
    return symbols


def download_history(ticker):
    print '\tworking on ' + ticker
    try:
        data = web.DataReader(ticker, 'yahoo', START, END)
    except Exception:
        print '\t' + ticker + ' downloading error'

    data.drop(['Open'], axis=1, inplace=True)
    data.drop(['High'], axis=1, inplace=True)
    data.drop(['Low'], axis=1, inplace=True)
    data.drop(['Close'], axis=1, inplace=True)
    data.drop(['Volume'], axis=1, inplace=True)
    data.rename(columns={'Adj Close': 'Close'}, inplace=True)

    ma = pd.stats.moments.rolling_mean(data['Close'], 10)
    data['10_MA'] = ma
    data['10_MAC'] = (ma - data['Close']) / data['Close'] * -100

    ma = pd.stats.moments.rolling_mean(data['Close'], 100)
    data['100_MA'] = ma
    data['100_MAC'] = (ma - data['Close']) / data['Close'] * -100

    with open('data/' + ticker + '.csv', 'w') as f:
        data.to_csv(f)


if __name__ == '__main__':
    if os.path.exists('data'):
        shutil.rmtree('data')
    os.makedirs('data')

    symlist = get_snp500()
    symlist = symlist[:16]
    symlist += EXTRA

    pool = ThreadPool(1)
    pool.map(download_history, symlist)
    pool.close()
    pool.join()

    print 'Finished downloading data'
