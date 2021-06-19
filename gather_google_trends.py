import argparse
import pandas as pd
import matplotlib.pylab as plt
from time import sleep
from datetime import datetime, timedelta
from pytrends.request import TrendReq

from db import load_db

DATE_FORMAT = '%Y-%m-%d'


def get_trend(keywords, start, end):
    pytrends = TrendReq(hl='en-US', tz=0, retries=5, backoff_factor=0.2)

    trends = pytrends.get_historical_interest(
        [*keywords], 
        year_start=start.date().year, 
        month_start=start.date().month, 
        day_start=start.date().day, 
        hour_start=start.time().hour,
        year_end=end.date().year, 
        month_end=end.date().month,
        day_end=end.date().day,
        hour_end=end.time().hour
    )
    try:
        trends.drop('isPartial', axis=1, inplace=True)
    except:
        print(trends)
    return trends



def main(connect_str, args):
    trends_db = load_db('trends', connect_str)

    start = datetime.strptime(args.start, DATE_FORMAT)
    finish = datetime.strptime(args.end, DATE_FORMAT)

    dt = timedelta(days=1)
    end = start + dt
    trends = get_trend(args.keywords, start, end)

    while end < finish:
        print(datetime.strftime(end, DATE_FORMAT))
        start = start + dt
        end = end + dt

        try:
            temp = get_trend(args.keywords, start, end)

            for kw in args.keywords:
                k = trends[kw].iloc[-1] / temp[kw].iloc[0]
                temp.loc[:,kw] *= k

            temp.drop(temp.head(1).index, inplace=True)       
            trends = pd.concat([trends, temp])
        except Exception as error:
            print(error)
            print(temp)
            continue

    trends.index = trends.index.astype(str)
    for kw in args.keywords:
        trends = trends.astype({kw: int})

    trends_db.insert_trends(args.keywords, trends)

    #ax = trends.plot(label='observed', figsize=(20, 15), linestyle='none', marker='o')
    #plt.legend()
    #plt.show()


if __name__ == '__main__':
    connect_str = 'dbname=cryptodata user=crypto password=secret port=5432 host=server'
    parser = argparse.ArgumentParser()
    parser.add_argument('--start')
    parser.add_argument('--end')
    parser.add_argument('--keywords', nargs='+')
    args = parser.parse_args()
    print(args)

    main(connect_str, args)