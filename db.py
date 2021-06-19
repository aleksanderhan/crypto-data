import psycopg2
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
from datetime import datetime


class DB(object):

    def __init__(self, connect_str):
        self.connect_str = connect_str
    
    def _create_connection(self):
        return psycopg2.connect(self.connect_str)

    def make_stmt(self, stmt, params):
        try:
            with self._create_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(stmt, params)
                    conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def query(self, query, params):    
        with self._create_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                records = cursor.fetchall()
                return records


class CandlesTable(DB):

    FIND_EARLIEST_CANDLE = "SELECT min(ts) FROM candles WHERE coin = %s AND currency = %s;"

    FIND_LATEST_CANDLE = "SELECT max(ts) FROM candles WHERE coin = %s AND currency = %s;"

    INSERT_CANDLE = "INSERT INTO candles(ts, coin, currency, low, high, open, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;"

    FETCH_CANDLES = """
                    SELECT
                        time_bucket_gapfill(
                            '1 minute', ts,
                            start => %s, 
                            finish => %s) AS time,
                        interpolate(avg(low)) AS low,
                        interpolate(avg(high)) AS high,
                        interpolate(avg(open)) AS open,
                        interpolate(avg(close)) AS close,
                        interpolate(avg(volume)) AS volume
                    FROM candles
                    WHERE coin = %s AND currency = %s
                    AND ts BETWEEN %s AND %s
                    GROUP BY time
                    ORDER BY time;
                    """

    def find_earliest_candle(self, coin, currency):
        return self.query(self.FIND_EARLIEST_CANDLE, (coin, currency))[0][0]

    def find_lateset_candle(self, coin, currency):
        return self.query(self.FIND_LATEST_CANDLE, (coin, currency))[0][0]

    def insert_candle(self, ts, coin, currency, low, high, open, close, volume):
        self.make_stmt(self.INSERT_CANDLE, (ts, coin, currency, low, high, open, close, volume))

    def insert_candles(self, coin, currency, candles):
        i = 0
        while i < len(candles):
            stmts, params = [], []
            batch = candles[i:i+1000]
            for candle in batch:
                stmts.append(self.INSERT_CANDLE)
                params += [datetime.fromtimestamp(candle[0]), coin, currency, candle[1], candle[2], candle[3], candle[4], candle[5]]

            self.make_stmt(' '.join(stmts), params)
            i += 1000

    def fetch_candles(self, coin, currency, start, finish):
        return self.query(self.FETCH_CANDLES, (start, finish, coin, currency, start, finish))


class WikiPageViews(DB):

    DATE_FORMAT = '%Y%m%d%H'

    FIND_EARLIEST_PAGEVIEW = "SELECT min(ts) FROM wiki_page_views WHERE article = %s;"

    FIND_LATEST_PAGEVIEW = "SELECT max(ts) FROM wiki_page_views WHERE article = %s;"

    INSERT_PAGE_VIEW = "INSERT INTO wiki_page_views(ts, article, views) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;"

    FETCH_PAGE_VIEWS = """
                        SELECT
                            time_bucket_gapfill(
                                '1 minute', ts,
                                start => %s, 
                                finish => %s) AS time,
                            locf(avg(views::FLOAT)) AS views
                        FROM wiki_page_views
                        WHERE article = %s
                        AND ts BETWEEN %s AND %s
                        GROUP BY time
                        ORDER BY time;
                        """

    def find_earliest_pageview(self, article):
        return self.query(self.FIND_EARLIEST_PAGEVIEW, (article,))[0][0]

    def find_latest_pageview(self, article):
        return self.query(self.FIND_LATEST_PAGEVIEW, (article,))[0][0]

    def insert_page_views(self, page_views):
        items = page_views['items']
        i = 0
        while i < len(items):
            stmts, params = [], []
            batch = items[i:i+1000]
            for item in batch:
                stmts.append(self.INSERT_PAGE_VIEW)
                params += [datetime.strptime(item['timestamp'], self.DATE_FORMAT), item['article'], float(item['views'])]

            self.make_stmt(' '.join(stmts), params)
            i += 1000

    def fetch_page_views(self, article, start, finish):
        return self.query(self.FETCH_PAGE_VIEWS, (start, finish, article, start, finish))


class GoogleTrends(DB):

    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

    FIND_EARLIEST_TREND = "SELECT min(ts) FROM google_trends WHERE keyword = %s;"

    FIND_LATEST_TREND = "SELECT max(ts) FROM google_trends WHERE keyword = %s;"

    INSERT_TREND = "INSERT INTO google_trends(ts, keyword, trend) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;"

    FETCH_TREND = """
                    SELECT
                        time_bucket_gapfill(
                            '1 minute', ts,
                            start => %s,
                            finish => %s) AS time,
                        locf(avg(trend::FLOAT)) AS trend
                    FROM google_trends
                    WHERE keyword = %s
                    AND ts BETWEEN %s AND %s
                    GROUP BY time
                    ORDER BY time;
                    """

    def find_earliest_trend(self, keyword):
        return self.query(self.FIND_EARLIEST_TREND, (keyword,))[0][0]

    def find_latest_trend(self, keyword):
        return self.query(self.FIND_LATEST_TREND, (keyword,))[0][0]

    def insert_trends(self, keywords, trend):
        for kw in keywords:
            stmts, params = [], []
            for index, row in trend.iterrows():
                stmts.append(self.INSERT_TREND)
                params += [datetime.strptime(index, self.DATE_FORMAT), kw, row[kw]]

            self.make_stmt(' '.join(stmts), params)

    def fetch_trend(self, keyword, start, finish):
        return self.query(self.FETCH_TREND, (start, finish, keyword, start, finish))


def load_db(db, connect_str):
    return db_interfaces[db](connect_str)


db_interfaces = {
    'candles': CandlesTable,
    'pageviews': WikiPageViews,
    'trends': GoogleTrends
}