from flask import Flask
import numpy as np
import pandas as pd
import json
import requests


coins = ['btc', 'eth']
features = ['low', 'high', 'open', 'close', 'volume'] # 'market_cap', 'circulating_supply', 'keyword_freq', 'biz_sia', 'transactions'
other_features = []


def get_candles(price_pair, start_time, end_time, granularity):
    """
    [
        [ time, low, high, open, close, volume ],
        [ 1415398768, 0.32, 4.2, 0.35, 4.2, 12.3 ],
        ...
    ]
    """
    r = requests.get('https://api.pro.coinbase.com/products/{}/candles?start={}&stop={}&granularity={}'.format(price_pair, start_time, end_time, granularity))
    return r.json()


def create_header():
    header = []
    for coin in coins:
        header += [coin + '_' + f for f in features]
    header += other_features
    return header


header = create_header()

app = Flask(__name__)


@app.route('/data')
def get_data():
    candles = {}

    for coin in coins:
        price_pair = coin.upper() + '-USD'
        candles[coin] = get_candles(price_pair, '2021-01-01T00:00', '2021-05-22T00:00', 900)

    data = []
    try:
        i = 0
        while True:
            frame = []
            for coin in coins:
                frame += candles[coin][i][1:]
            data.append(frame)
            i += 1
    except IndexError:
        pass

    timesteps = len(data)
    print(timesteps)
    timestamps = list(range(timesteps))
    num_features = (len(coins)*len(features)) + len(other_features)

    df = pd.DataFrame(data, index=timestamps, columns=header)
    return df.to_json()


@app.route('/coins')
def get_coins():
    return json.dumps(coins)