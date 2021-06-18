import argparse
import pandas as pd
import matplotlib.pylab as plt

from data_api import get_dataframe
from db import load_db



DATE_FORMAT = '%Y-%m-%dT%H:%M'

features = ['low', 'high', 'open', 'volume']


def main(connect_str, args):
    currency = 'USD'
    candles_db = load_db('candles', connect_str)

    if args.plot:
        coins = args.plot.split(',')
        df = get_dataframe(args.start, args.end, coins, currency, [])
        #df.timestamp = pd.to_datetime(df.timestamp)
        #df.dropna(inplace=True, how='any')
        print(df)
        df.drop(['timestamp'], axis = 1, inplace = True, errors = 'ignore')
        for coin in coins:
            for feature in features:
                df.drop([f'{coin}_{feature}'], axis = 1, inplace = True, errors = 'ignore')

        print(df)

        ax = df.plot(label='observed', figsize=(20, 15))

        plt.legend()
        plt.show()



if __name__ == '__main__':
    connect_str = 'dbname=cryptodata user=crypto password=secret port=5432 host=localhost'
    parser = argparse.ArgumentParser()
    parser.add_argument('--plot')
    parser.add_argument('--start')
    parser.add_argument('--end')
    args = parser.parse_args()


    main(connect_str, args)