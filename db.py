import psycopg2


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
        try:
            with self._create_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    records = cursor.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        return records


class CandlesTable(DB):

    INSERT_CANDLE = "INSERT INTO candles(ts, coin, currency, low, high, open, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
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
                    ORDER BY time
                    """

    def insert_candle(self, ts, coin, currency, low, high, open, close, volume):
        self.make_stmt(self.INSERT_CANDLE, (ts, coin, currency, low, high, open, close, volume))

    def fetch_candles(self, coin, currency, start, finish):
        return self.query(self.FETCH_CANDLES, (start, finish, coin, currency, start, finish))




def load_db(db, connect_str):
    return db_interfaces[db](connect_str)


db_interfaces = {
    'candles': CandlesTable
}