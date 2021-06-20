import argparse
import pandas as pd
import matplotlib.pylab as plt
from time import sleep
from datetime import datetime, timedelta
from pytrends.request import TrendReq

from db import load_db

DATE_FORMAT = '%Y-%m-%d'


def get_trend(keyword, start, end):
    pytrends = TrendReq(tz=0, retries=10, backoff_factor=0.5)

    trends = pytrends.get_historical_interest(
        [keyword], 
        year_start=start.date().year, 
        month_start=start.date().month, 
        day_start=start.date().day, 
        hour_start=start.time().hour,
        year_end=end.date().year, 
        month_end=end.date().month,
        day_end=end.date().day,
        hour_end=end.time().hour
    )
    trends= trends[trends[keyword] != 0]
    trends.drop('isPartial', axis=1, inplace=True)
    return trends



def main(connect_str, args):
    csv_file = 'trends_' + args.keyword + '.csv'

    if args.start and args.end:
        start = datetime.strptime(args.start, DATE_FORMAT)
        finish = datetime.strptime(args.end, DATE_FORMAT)

        dt = timedelta(days=1)
        end = start + dt
        trends = get_trend(args.keyword, start, end)

        while end < finish:
            sleep(0.5)
            print(datetime.strftime(end, DATE_FORMAT))
            start = start + dt
            end = end + dt

            print(trends[args.keyword].iloc[-1])
            try:
                temp = get_trend(args.keyword, start, end)
                k = trends[args.keyword].iloc[-1] / temp[args.keyword].iloc[0]
                temp.loc[:,args.keyword] *= k

                #temp.drop(temp.head(1).index, inplace=True)       
                trends = pd.concat([trends, temp])
            except Exception as error:
                print(error)
                print(temp)
                continue

        trends.dropna(inplace=True, how='any')
        trends.index = trends.index.astype(str)
        trends = trends.astype({args.keyword: float})

        trends.to_csv(csv_file)

    elif args.insert:
        trends_db = load_db('trends', connect_str)
        
        trends = pd.read_csv(csv_file)
        trends = trends.set_index('date')
        trends.index = trends.index.astype(str)
        trends = trends.astype({args.keyword: float})
        print(trends)
        trends_db.insert_trends(args.keyword, trends)

    #ax = trends.plot(label='observed', figsize=(20, 15), linestyle='none', marker='o')
    #plt.legend()
    #plt.show()


if __name__ == '__main__':
    connect_str = 'dbname=cryptodata user=crypto password=secret port=5432 host=server'
    parser = argparse.ArgumentParser()
    parser.add_argument('--start')
    parser.add_argument('--end')
    parser.add_argument('--keyword')
    parser.add_argument('--insert', action='store_true')
    args = parser.parse_args()
    print(args)

    main(connect_str, args)