import pandas as pd
import requests
import matplotlib.pylab as plt
import urllib
from datetime import datetime, timedelta
from flask import Flask, request
from time import perf_counter
from functools import reduce

from db import load_db


app = Flask(__name__)
DATE_FORMAT = '%Y-%m-%dT%H:%M'

candle_features = ['low', 'high', 'open', 'close', 'volume']

connect_str = 'dbname=cryptodata user=crypto password=secret port=5432 host=server'
candles_db = load_db('candles', connect_str)
page_views_db = load_db('pageviews', connect_str)
trends_db = load_db('trends', connect_str)


def create_candle_header(coin):
    return ['timestamp'] + [coin + '_' + f for f in candle_features]


def create_page_views_header(article):
    return ['timestamp'] + [article + '_pageviews']

def create_trends_header(keyword):
    return ['timestamp'] + [keyword + '_trend']


def get_dataframe(start_time, end_time, coins, currency, wiki_articles, trends_keywords):
    data = []
    
    for coin in coins:
        records = candles_db.fetch_candles(coin, currency, start_time, end_time)

        df = pd.DataFrame(records, columns=create_candle_header(coin))
        #df.fillna(0, inplace=True)
        data.append(df)

    for article in wiki_articles:
        start = datetime.strftime(datetime.strptime(start_time, DATE_FORMAT) - timedelta(days=1), DATE_FORMAT)
        end = datetime.strftime(datetime.strptime(end_time, DATE_FORMAT) - timedelta(days=1), DATE_FORMAT)
        records = page_views_db.fetch_page_views(article, start, end)

        df = pd.DataFrame(records, columns=create_page_views_header(article))
        df['timestamp'] = df['timestamp'] + pd.DateOffset(days=1) # Previous day pageviews are todays observation (because I can't get the pageviews for today, today)
        data.append(df)

    for keyword in trends_keywords:
        start = datetime.strftime(datetime.strptime(start_time, DATE_FORMAT) - timedelta(hours=1), DATE_FORMAT)
        end = datetime.strftime(datetime.strptime(end_time, DATE_FORMAT) - timedelta(hours=1), DATE_FORMAT)
        records = trends_db.fetch_trend(keyword, start, end)

        df = pd.DataFrame(records, columns=create_trends_header(keyword))
        df['timestamp'] = df['timestamp'] + pd.DateOffset(hour=1)
        data.append(df)

    df = reduce(lambda left, right: pd.merge_ordered(left, right, on='timestamp'), data)
    df.drop('timestamp', axis=1, inplace=True)
    return df


@app.route('/data')
def get_data():
    start_time = request.args.get('start_time', '')
    end_time = request.args.get('end_time', '')
    coins = request.args.get('coins', '').split(',')
    wiki_articles = request.args.get('wiki_articles', '').split(',')
    trend_keywords = request.args.get('trend_keywords', '').split(',')

    df = get_dataframe(start_time, end_time, coins, 'USD', wiki_articles, trend_keywords)
    df.dropna(inplace=True, how='any')
    df.reset_index(drop=True, inplace=True)
    #df.fillna(0, inplace=True)

    print(df)
    return df.to_json()



if __name__ == '__main__':
    """Tests"""
    coins = ['eth']
    wiki_articles = ['Ethereum']
    trend_keywords = ['ethereum']
    df = get_dataframe('2021-01-01T00:00', '2021-01-10T00:00', coins, 'USD', wiki_articles, trend_keywords)
    #df.dropna(inplace=True, how='any')
    #df.reset_index(drop=True, inplace=True)
    df.fillna(0, inplace=True)
    print(df)
    print(df.dtypes)
    print(df.columns)
    print(df.index)

    df['Ethereum_pageviews'].plot(label='eth', figsize=(20, 15))

    plt.show()