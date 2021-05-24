from flask import Flask, request
import numpy as np
import pandas as pd
import json
import requests
from datetime import date, timedelta, datetime

DATE_FORMAT = '%Y-%m-%dT%H:%M'

coins = ['btc', 'eth'] #, 'ada', 'link', 'algo', 'ltc']
features = ['low', 'high', 'open', 'close', 'volume'] # 'market_cap', 'circulating_supply', 'keyword_freq', 'biz_sia', 'transactions'
other_features = []
num_features = (len(coins)*len(features)) + len(other_features)



def get_candles_for_coin(coin, start_time, end_time, granularity=900):
    """
    [
        [ time, low, high, open, close, volume ],
        [ 1415398768, 0.32, 4.2, 0.35, 4.2, 12.3 ],
        ...
    ]
    """

    t0 = datetime.strptime(start_time, DATE_FORMAT)
    t1 = t0 + timedelta(days=3)
    end_time = datetime.strptime(end_time, DATE_FORMAT)

    price_pair = coin.upper() + '-USD'
    ret = []
    while t1 < end_time:
        print(coin, t1)

        r = requests.get('https://api.pro.coinbase.com/products/{}/candles?start={}&stop={}&granularity={}'.format(
            price_pair, 
            datetime.strftime(t0, DATE_FORMAT), 
            datetime.strftime(t1, DATE_FORMAT), 
            granularity)
        )
    
        t0 = t0 + timedelta(days=3)
        t1 = t1 + timedelta(days=3)

        ret += r.json()

    return ret


def get_candles(start_time, end_time):
    candles = {}
    for coin in coins:        
        candles[coin] = get_candles_for_coin(coin, start_time, end_time)
    return candles


def get_data_from_candles(candles):
    data = []
    try:
        i = 0
        while True:
            frame = []
            for coin in coins:
                frame += candles[coin][i][1:] # comment out [1:]?
            data.append(frame)
            i += 1
    except IndexError:
        pass
    return data


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
    frame_size = request.args.get('frame_size', '')
    start_time = request.args.get('start_time', '')
    end_time = request.args.get('end_time', '')

    candles = get_candles(start_time, end_time)

    data = get_data_from_candles(candles)    

    timesteps = len(data)
    timestamps = list(range(timesteps))

    df = pd.DataFrame(data, index=timestamps, columns=header)
    print(df)
    return df.to_json()


@app.route('/coins')
def get_coins():
    return json.dumps(coins)



if __name__ == '__main__':
    """Tests"""
    candles = get_candles('2021-05-18T00:00', '2021-05-22T00:00')
    data = get_data_from_candles(candles)

    print()
    print(len(data))
    print(len(data[0]))
    print('num_features', num_features)
    print('header len', len(header))


    timesteps = len(data)
    timestamps = list(range(timesteps))

    df = pd.DataFrame(data, index=timestamps, columns=header)
    print(df)