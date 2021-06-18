import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
nltk.download('vader_lexicon')
nltk.download('punkt')

KEYWORDS = dict(
	TRADABLE_COINS = set(['AAVE', 'ALGO', 'ATOM', 'BAL', 'BAND', 'BAT', 'BCH', 'BNT', 'BSV', 'BTC', 'CGLD', 'COMP', 'CVC', 'DAI', 'DASH', 'DNT', 'EOS', 'ETC', 'ETH', 'FIL', 'GRT', 'KNC', 'LINK', 'LRC', 'LTC', 'MANA', 'MKR', 'NMR', 'NU', 'OMG', 'OXT', 'REN', 'REP', 'SNX', 'UMA', 'UNI', 'USDC', 'WBTC', 'XLM', 'XTZ', 'YFI', 'ZEC', 'ZRX']),
	FULL_COIN_NAMES = set(['bitcoin', 'litecoin', 'ethereum']),
	ADDITIONAL_KEYWORDS = set(['pump', 'dump', 'sell', 'buy', 'bear', 'bull', 'bullish'])
)
keywords = set(list(map(lambda x: x.lower(), set([]).union(*KEYWORDS.values()))))


class SIA(object):
	def __init__(self):
		self.sia = SentimentIntensityAnalyzer()

	def get_sentiment(self, string):
		return self.sia.polarity_scores(string)


sia = SIA()


def get_keywords(text):
	words = map(lambda x: x.lower(), nltk.word_tokenize(text))
	ret = set([])
	for word in words:
		if word in keywords:
			ret.add(word)
	return ret

def get_sentiment(text):
	return sia.get_sentiment(text)

def get_word_freq(word, text):
	words = list(map(lambda x: x.lower(), nltk.word_tokenize(text)))
	return words.count(word)



if __name__ == '__main__':
	# Tests
	text = "afawf BTC http://localhost keyword. next!time, btc"
	assert get_keywords(text) == set(['btc'])
	assert get_word_freq('btc', text) == 2
	print(get_sentiment(text))