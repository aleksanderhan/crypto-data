import argparse
from datetime import datetime

from db import load_db

DATE_FORMAT = '%Y-%m-%dT%H:%M'


def main(connect_str, args):
    currency = 'USD'
    candles_db = load_db('candles', connect_str)

    if args.earliest:
        earliest = candles_db.find_earliest_candle(args.earliest, currency)
        print(f'earliest candle for {args.earliest}:', datetime.strftime(earliest, DATE_FORMAT))

    if args.latest:
        latest = candles_db.find_lateset_candle(args.latest, currency)
        print(f'latest candle for {args.earliest}:', datetime.strftime(latest, DATE_FORMAT))


if __name__ == '__main__':
    connect_str = 'dbname=cryptodata user=crypto password=secret port=5432 host=localhost'
    parser = argparse.ArgumentParser()
    parser.add_argument('--earliest')
    parser.add_argument('--latest')
    args = parser.parse_args()

    main(connect_str, args)