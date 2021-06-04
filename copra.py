
import asyncio
from datetime import datetime
import sys

from copra.websocket import Channel, Client

from functools import wraps


# Decorator function
def coroutine(func):
	@wraps(func)
	def wrapper(*args, **kwargs):
		cr = func(*args, **kwargs)
		next(cr)
		return cr
	return wrapper

@coroutine
def process_trades(trades):
    while True:
        price = (yield)
        trades.append(price)
        print('open:', temp[0], 'high:', max(temp), 'low:', min(temp), 'close:', temp[-1])


candles = process_trades([])


class Trades(Client):

    def on_message(self, message):
    	if message['type'] == 'l2update':
	        changes = message['changes']
	        for c in changes:
	        	if c[0] == 'buy':
	        		candles.send(float(c[1]))

product_id = sys.argv[1]

loop = asyncio.get_event_loop()

channel = Channel('level2', product_id)

trades = Trades(loop, channel)

try:
    loop.run_forever()
except KeyboardInterrupt:
    loop.run_until_complete(trades.close())
    loop.close()