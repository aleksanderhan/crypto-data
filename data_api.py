import pandas as pd
import requests
import matplotlib.pylab as plt
import urllib
from flask import Flask, request
from time import perf_counter
from functools import reduce

from db import load_db


app = Flask(__name__)
DATE_FORMAT = '%Y-%m-%dT%H:%M'

features = ['low', 'high', 'open', 'close', 'volume'] # 'market_cap', 'circulating_supply', 'keyword_freq', 'biz_sia', 'transactions'

connect_str = 'dbname=cryptodata user=crypto password=secret port=5432 host=localhost'
candles_db = load_db('candles', connect_str)


def create_header(coin):
    return ['timestamp'] + [coin + '_' + f for f in features]


def get_dataframe(start_time, end_time, coins, currency):
    candles = []
    
    for coin in coins:
        records = candles_db.fetch_candles(coin, currency, start_time, end_time)
        df = pd.DataFrame(records, columns=create_header(coin))
        candles.append(df)

    return reduce(lambda left, right: pd.merge_ordered(left, right, on='timestamp', ), candles)


@app.route('/data')
def get_data():
    start_time = request.args.get('start_time', '')
    end_time = request.args.get('end_time', '')
    coins = request.args.get('coins', '').split(',')

    df = get_dataframe(start_time, end_time, coins, 'USD')
    df.dropna(inplace=True, how='any')
    df.reset_index(drop=True, inplace=True)

    print(df)
    return df.to_json()



if __name__ == '__main__':
    """Tests"""
    coins = ['eth']
    df = get_dataframe('2021-01-01T00:00', '2021-02-01T00:00', coins, 'USD')
    df.dropna(inplace=True, how='any')
    df.reset_index(drop=True, inplace=True)
    print(df)

    df['eth_close'].plot(label='eth', figsize=(20, 15))

    plt.show()