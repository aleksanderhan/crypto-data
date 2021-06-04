from flask import Flask, request
import pandas as pd
import requests
import matplotlib.pylab as plt
import urllib
from time import perf_counter

from db import load_db


app = Flask(__name__)
DATE_FORMAT = '%Y-%m-%dT%H:%M'

features = ['low', 'high', 'open', 'close', 'volume'] # 'market_cap', 'circulating_supply', 'keyword_freq', 'biz_sia', 'transactions'

connect_str = 'dbname=cryptodata user=crypto password=secret port=5432 host=localhost'
candles_db = load_db('candles', connect_str)


def get_data_from_candles(candles, coins):
    data, timestamps = [], []
    num_frames = len(candles[coins[0]])
    
    for i in range(num_frames):
        frame = []
        for coin in coins:
            frame += candles[coin][i][1:]
        data.append(frame)
        timestamps.append(candles[coins[0]][i][0])

    return data, timestamps


def create_header(coins):
    header = []
    for coin in coins:
        header += [coin + '_' + f for f in features]
    return header


def get_candles(start_time, end_time, coins, currency):
    candles = {}
    
    for coin in coins:
        records = candles_db.fetch_candles(coin, currency, start_time, end_time)
        print(len(records))
        result = []
        for row in records:
            result.append((row[0], row[1], row[2], row[3], row[4], row[5]))
        candles[coin] = result

    return candles



@app.route('/data')
def get_data():
    start_time = request.args.get('start_time', '')
    end_time = request.args.get('end_time', '')
    coins = request.args.get('coins', '').split(',')

    candles = get_candles(start_time, end_time, coins, 'USD')
    data, timestamps = get_data_from_candles(candles, coins)

    index = list(range(len(timestamps)))

    df = pd.DataFrame(data, index=index, columns=create_header(coins))
    df['timestamp'] = timestamps
    df.dropna(inplace=True, how='any')
    df.reset_index(drop=True, inplace=True)
    print(df)
    return df.to_json()




if __name__ == '__main__':
    """Tests"""
    coins = ['eth']
    candles = get_candles('2021-01-01T00:00', '2021-05-01T00:00', coins, 'USD')

    data, timestamps = get_data_from_candles(candles, coins)

    timesteps = len(timestamps)
    index = list(range(timesteps))

    df = pd.DataFrame(data, index=index, columns=create_header(coins))
    df['timestamps'] = timestamps
    df.dropna(inplace=True, how='any')
    df.reset_index(drop=True, inplace=True)
    print(df)

    df['eth_close'].plot(label='eth', figsize=(20, 15))
    plt.show()