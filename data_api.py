from flask import Flask, request
import pandas as pd
import json
import requests
from datetime import date, timedelta, datetime
from functools import lru_cache

DATE_FORMAT = '%Y-%m-%dT%H:%M'

features = ['low', 'high', 'open', 'close', 'volume'] # 'market_cap', 'circulating_supply', 'keyword_freq', 'biz_sia', 'transactions'


@lru_cache(maxsize=10000)
def make_candles_request(price_pair, t0, t1, granularity):
    r = requests.get('https://api.pro.coinbase.com/products/{}/candles?start={}&stop={}&granularity={}'.format(
            price_pair, 
            datetime.strftime(t0, DATE_FORMAT), 
            datetime.strftime(t1, DATE_FORMAT), 
            granularity)
        )
    return r.json()


def get_candles_for_coin(coin, start_time, end_time, granularity):
    """
    [
        [ time, low, high, open, close, volume ],
        [ 1415398768, 0.32, 4.2, 0.35, 4.2, 12.3 ],
        ...
    ]   
    """

    dt = granularity/60/5
    t0 = datetime.strptime(start_time, DATE_FORMAT)
    t1 = t0 + timedelta(days=dt)
    end_time = datetime.strptime(end_time, DATE_FORMAT)

    price_pair = coin.upper() + '-USD'
    ret = []
    while t1 < end_time:
        #print(datetime.strftime(t1, '%Y-%m-%d'), datetime.strftime(end_time, '%Y-%m-%d'))

        result = make_candles_request(price_pair, t0, t1, granularity)
    
        t0 = t0 + timedelta(days=dt)
        t1 = t1 + timedelta(days=dt)

        ret += result

    return ret


def get_candles(start_time, end_time, coins, granularity):
    candles = {}
    for i, coin in enumerate(coins):
        print(f'Getting candles for {i+1}/{len(coins)} coins')
        candles[coin] = get_candles_for_coin(coin, start_time, end_time, granularity)
    return candles


def get_data_from_candles(candles, coins):
    data, timestamps = [], []
    num_frames = len(candles[coins[0]])
    
    for i in range(num_frames):
        frame = []
        for coin in coins:
            frame += candles[coin][i][1:]
        data.append(frame)
        timestamps.append(candles[coins[0]][i][0])

    data.reverse()
    timestamps.reverse()
    return data, timestamps


def create_header(coins):
    header = []
    for coin in coins:
        header += [coin + '_' + f for f in features]
    return header



app = Flask(__name__)


@app.route('/data')
def get_data():
    start_time = request.args.get('start_time', '')
    end_time = request.args.get('end_time', '')
    coins = request.args.get('coins', '').split(',')
    granularity = int(request.args.get('granularity', ''))

    candles = get_candles(start_time, end_time, coins, granularity)
    data, timestamps = get_data_from_candles(candles, coins)
 
    index = list(range(len(timestamps)))

    df = pd.DataFrame(data, index=index, columns=create_header(coins))
    df['timestamp'] = timestamps
    print(df)
    return df.to_json()




if __name__ == '__main__':
    """Tests"""
    coins = ['btc']
    candles = get_candles('2021-04-01T00:00', '2021-04-22T00:00', coins, 900)

    data, timestamps = get_data_from_candles(candles, coins)

    print()
    print(len(data))
    print(len(data[0]))
    print('num_features', (len(coins)*len(features)))


    timesteps = len(timestamps)
    index = list(range(timesteps))

    df = pd.DataFrame(data, index=index, columns=create_header(coins))
    df['timestamps'] = timestamps
    print(df)