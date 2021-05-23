import requests
from datetime import date, timedelta, datetime


COINS = ['BTC', 'ETH']

def get_candles(price_pair, start_time, end_time, granularity):
	r = requests.get('https://api.pro.coinbase.com/products/{}/candles?start={}&stop={}&granularity={}'.format(price_pair, start_time, end_time, granularity))
	return r.json()


#print(get_candles('BTC-USD', '2021-05-01T00:00', '2021-05-02T00:00', 300))



def main(get_historical_data):

	now = datetime.today()
	print(print(type(now)), now)
	nowStr = datetime.strftime(now, '%Y-%m-%dT%H:%M')
	print(type(nowStr), nowStr)





	if get_historical_data:
		pass



if __name__ == '__main__':
	main(True)