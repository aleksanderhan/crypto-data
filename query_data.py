import argparse
from datetime import datetime

from db import load_db

DATE_FORMAT = '%Y-%m-%dT%H:%M'


def main(connect_str, args):
    currency = 'USD'
    candles_db = load_db('candles', connect_str)
    page_views_db = load_db('pageviews', connect_str)

    if args.earliest:
        if args.coin:
            earliest = candles_db.find_earliest_candle(args.coin, currency)
            print(f'earliest candle for {args.coin}:', datetime.strftime(earliest, DATE_FORMAT))
        if args.article:
            earliest = page_views_db.find_earliest_pageview(args.article)
            print(f'earliest pageview for {args.article}:', datetime.strftime(earliest, DATE_FORMAT))

    if args.latest:
        if args.coin:
            latest = candles_db.find_lateset_candle(args.coin, currency)
            print(f'latest candle for {args.coin}:', datetime.strftime(latest, DATE_FORMAT))
        if args.article:
            latest = page_views_db.find_latest_pageview(args.article)
            print(f'latest pageview for {args.article}:', datetime.strftime(latest, DATE_FORMAT))


if __name__ == '__main__':
    connect_str = 'dbname=cryptodata user=crypto password=secret port=5432 host=server'
    parser = argparse.ArgumentParser()
    parser.add_argument('--earliest', action='store_true')
    parser.add_argument('--latest', action='store_true')
    parser.add_argument('--coin')
    parser.add_argument('--article')
    args = parser.parse_args()

    main(connect_str, args)