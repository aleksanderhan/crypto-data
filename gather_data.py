import requests
import urllib
import argparse
from datetime import timedelta, datetime
from time import perf_counter

from db import load_db

DATE_FORMAT = '%Y-%m-%dT%H:%M'


def make_candles_request(price_pair, t0, t1, granularity):
    params = {
        'start': datetime.strftime(t0, DATE_FORMAT),
        'end': datetime.strftime(t1, DATE_FORMAT),
        'granularity': granularity
    }

    r = requests.get(f'https://api.pro.coinbase.com/products/{price_pair}/candles?' + urllib.parse.urlencode(params))
    return r.json()


def get_candles_for_coin(coin, currency, start_time, end_time, granularity):
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

    price_pair = coin.upper() + '-' + currency
    result = []
    for i in range(int(number_of_requests/requests_per_message) + 1):
        t0 = start + timedelta(0, i*granularity*requests_per_message)
        t1 = start + timedelta(0, (i+1)*granularity*requests_per_message)

        result += make_candles_request(price_pair, t0, t1, granularity)

    return result



def main(connect_str, args):
    currency = 'USD'
    candles_db = load_db('candles', connect_str)

    coins = args.coins.split(',')
    for i, coin in enumerate(coins):
        print(f'Getting candles for {i+1}/{len(coins)} coins')
        t0 = perf_counter()
        candles = get_candles_for_coin(coin, currency, args.start, args.end, 60)
        t1 = perf_counter()
        print(f'Downloaded candles for {coin} in {t1-t0} s')

        for candle in candles:
            candles_db.insert_candle(datetime.fromtimestamp(candle[0]), coin, currency, candle[1], candle[2], candle[3], candle[4], candle[5])
        t2 = perf_counter()
        print(f'Inserted candles into db in {t2-t1} s')




if __name__ == '__main__':
    connect_str = 'dbname=cryptodata user=crypto password=secret port=5432 host=localhost'
    parser = argparse.ArgumentParser()
    parser.add_argument('--start')
    parser.add_argument('--end')
    parser.add_argument('--coins')
    args = parser.parse_args()
    print(args)

    main(connect_str, args)