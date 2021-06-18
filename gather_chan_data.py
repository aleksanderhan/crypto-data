import queue, threading
import math
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
nltk.download('vader_lexicon')
#nltk.download('punkt')
from requests_html import HTMLSession, AsyncHTMLSession
from datetime import datetime


BASE_URL = 'https://archived.moe/'
DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'


class SentimentExtractor(threading.Thread):

	def __init__(self):
		super(SentimentExtractor, self).__init__()
		self.sia = SentimentIntensityAnalyzer()
		self.post_queue = queue.Queue(maxsize=1000)
		self.stopped = False

	def put(self, thread_id):
		self.q.put(thread_id, block=True, timeout=None)

	def stop(self):
		self.stopped = True

	def run(self):
		while not self.stopped:
			post = self.post_queue.get(block=True, timeout=None)

	def get_sentiment(self, text):
		return self.sia.polarity_scores(text)
			

	


class Scraper(object):

	def __init__(self, board):
		self.processor = SentimentExtractor()
		self.asession = AsyncHTMLSession()
		self.board = board
		self.processed_thread_ids = set([])

		self.stop_date = datetime.strptime('2021-06-15T00:00:00', DATE_FORMAT)
		self.done = False
		self.page = 1
		self.pages_at_a_time = 3
		self.threads_at_a_time = 5


	def run(self):

		while not self.done:
			thread_ids = self.get_thread_ids()
			new_ids = thread_ids - self.processed_thread_ids
			posts = self.get_posts(new_ids)


	def get_thread_ids(self):
		ids = set([])
		requests = [lambda url=BASE_URL+self.board+'/page/'+str(page): self._get_page(url) for page in range(self.page, self.page+self.pages_at_a_time+1)]

		try:
			rs = self.asession.run(*requests)
		except Exception as e:
			print(e)
			return

		for r in rs:
			main = r.html.find('#main', first=True)
			articles = main.find('article')
			
			if (len(articles) < 1):
				self.done = True
				break;

			ops = filter(lambda a: 'post_is_op' in a.attrs['class'], articles)
			for op in ops:
				ids.add(op.attrs['id'])

				header = op.find('header')[0]
				time = header.find('.time_wrap')[0].find('time')[0]
				timestamp = datetime.strptime(time.attrs['datetime'].split('+')[0], DATE_FORMAT)
				if timestamp < self.stop_date:
					self.done = True
		
		self.page += self.pages_at_a_time
		return ids


	def get_posts(self, thread_ids):
		requests = [lambda url=BASE_URL+self.board+'/'+str(thread_id): self._get_thread(url) for thread_id in thread_ids]
		try:
			rs = self.asession.run(*requests)
		except Exception as e:
			print(e)
			self.get_posts(thread_ids)
			return

		for r in rs:
			main = r.html.find('#main', first=True)
						
			articles = main.find('article')
			for a in articles:
				post = a.find('.text')[0].text
				print(post)
				print()

			

	async def _get_page(self, page_url):
		return await self.asession.get(page_url)

	async def _get_thread(self, thread_url):
		return await self.asession.get(thread_url)



if __name__ == '__main__':
	scraper = Scraper(board='biz')
	scraper.run()