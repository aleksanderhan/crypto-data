import pandas as pd
import requests
import matplotlib.pylab as plt
import urllib
from datetime import timedelta
from flask import Flask, request
from time import perf_counter
from functools import reduce

from db import load_db


app = Flask(__name__)
DATE_FORMAT = '%Y-%m-%dT%H:%M'

candle_features = ['low', 'high', 'open', 'close', 'volume'] # 'market_cap', 'circulating_supply', 'keyword_freq', 'biz_sia', 'transactions'

connect_str = 'dbname=cryptodata user=crypto password=secret port=5432 host=server'
candles_db = load_db('candles', connect_str)
page_views_db = load_db('pageviews', connect_str)


def create_candle_header(coin):
    return ['timestamp'] + [coin + '_' + f for f in candle_features]


def create_page_views_header(article):
    return ['timestamp'] + [article + '_pageviews']


def get_dataframe(start_time, end_time, coins, currency, wiki_articles):
    data = []
    
    for coin in coins:
        records = candles_db.fetch_candles(coin, currency, start_time, end_time)
        df = pd.DataFrame(records, columns=create_candle_header(coin))
        data.append(df)

    for article in wiki_articles:
        records = page_views_db.fetch_page_views(article, start_time, end_time)
        df = pd.DataFrame(records, columns=create_page_views_header(article))
        df['timestamp'] = df['timestamp'] + pd.DateOffset(days=1) # Previous day pageviews are todays observation (because I can't get the pageviews for today)
        data.append(df)

    return reduce(lambda left, right: pd.merge_asof(left, right, on='timestamp', tolerance=timedelta(seconds=30)), data)


@app.route('/data')
def get_data():
    start_time = request.args.get('start_time', '')
    end_time = request.args.get('end_time', '')
    coins = request.args.get('coins', '').split(',')
    wiki_articles = request.args.get('wiki_articles', '').split(',')

    df = get_dataframe(start_time, end_time, coins, 'USD', wiki_articles)
    df.dropna(inplace=True, how='any')
    df.reset_index(drop=True, inplace=True)
    #df.fillna(0, inplace=True)

    print(df)
    return df.to_json()



if __name__ == '__main__':
    """Tests"""
    coins = ['btc', 'eth']
    wiki_articles = ['Bitcoin', 'Ethereum']
    df = get_dataframe('2021-01-01T00:00', '2021-02-01T00:00', coins, 'USD', wiki_articles)
    #df.dropna(inplace=True, how='any')
    #df.reset_index(drop=True, inplace=True)
    df.fillna(0, inplace=True)
    print(df)
    print(df.dtypes)

    df['Ethereum_pageviews'].plot(label='eth', figsize=(20, 15))

    plt.show()