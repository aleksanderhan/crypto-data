import requests
import urllib
import argparse
from datetime import datetime

from db import load_db

API_DATE_FORMAT = '%Y%m%d'
USER_DATE_FORMAT = '%Y-%m-%d'


def get_page_views(article, start, end):
	base_url = 'https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/{article}/daily/{start}/{end}'
	url = base_url.format(
		article=urllib.parse.quote(article),
		start=datetime.strftime(start, API_DATE_FORMAT),
		end=datetime.strftime(end, API_DATE_FORMAT)
	)
	headers = {
	    'User-Agent': 'aleksanderhan@gmail.com'
	}

	r = requests.get(url, headers=headers)
	return r.json()


def main(connect_str, args):
	page_views_db = load_db('pageviews', connect_str)

	start = datetime.strptime(args.start, USER_DATE_FORMAT)
	end = datetime.strptime(args.end, USER_DATE_FORMAT)

	articles = args.articles.split(',')
	for article in articles:
		page_views = get_page_views(article, start, end)
		page_views_db.insert_page_views(page_views)



if __name__ == '__main__':
    connect_str = 'dbname=cryptodata user=crypto password=secret port=5432 host=server'
    parser = argparse.ArgumentParser()
    parser.add_argument('--start')
    parser.add_argument('--end')
    parser.add_argument('--articles')
    args = parser.parse_args()
    print(args)

    main(connect_str, args)