from flask import Flask, request
import pandas as pd
import json
import requests
from datetime import date, timedelta, datetime
from functools import lru_cache
from time import perf_counter
import matplotlib.pylab as plt
import urllib 


DATE_FORMAT = '%Y-%m-%dT%H:%M'

features = ['low', 'high', 'open', 'close', 'volume'] # 'market_cap', 'circulating_supply', 'keyword_freq', 'biz_sia', 'transactions'


#@lru_cache(maxsize=100000)
def make_candles_request(price_pair, t0, t1, granularity):
    params = {
        'start': datetime.strftime(t0, DATE_FORMAT),
        'end': datetime.strftime(t1, DATE_FORMAT),
        'granularity': granularity
    }

    r = requests.get(f'https://api.pro.coinbase.com/products/{price_pair}/candles?' + urllib.parse.urlencode(params))
    return r.json()


def get_candles_for_coin(coin, start_time, end_time, granularity):
    """
    [
        [ time, low, high, open, close, volume ],
        [ 1415398768, 0.32, 4.2, 0.35, 4.2, 12.3 ],
        ...
    ]   
    """

    start = datetime.strptime(start_time, DATE_FORMAT)
    end = datetime.strptime(end_time, DATE_FORMAT)

    requests_per_message = 300
    number_of_requests = abs((end - start).total_seconds())/granularity
    print(number_of_requests)


    price_pair = coin.upper() + '-USD'
    ret = []
    for i in range(int(number_of_requests/requests_per_message) + 1):
        t0 = start + timedelta(0, i*granularity*requests_per_message)
        t1 = start + timedelta(0, (i+1)*granularity*requests_per_message)

        result = make_candles_request(price_pair, t0, t1, granularity)
    

        ret += result

    return ret


def get_candles(start_time, end_time, coins, granularity):
    candles = {}
    for i, coin in enumerate(coins):
        print(f'Getting candles for {i+1}/{len(coins)} coins')
        t0 = perf_counter()
        candles[coin] = get_candles_for_coin(coin, start_time, end_time, granularity)
        t1 = perf_counter()
        print(f'Downloaded candles for {coin} in {t1-t0} s')
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
    candles = get_candles('2021-04-01T00:00', '2021-04-02T00:00', coins, 60)

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

    df['btc_close'].plot(label='btc', figsize=(20, 15))
    plt.show()