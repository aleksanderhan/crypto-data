CREATE TABLE candles (
	ts TIMESTAMPTZ NOT NULL,
	coin VARCHAR(5) NOT NULL,
	currency VARCHAR(5) NOT NULL,
	low DECIMAL,
	high DECIMAL,
	open DECIMAL,
	close DECIMAL,
	volume DECIMAL,
	PRIMARY KEY (ts, coin, currency)
);
SELECT create_hypertable('candles', 'ts');


CREATE TABLE wiki_page_views (
	ts TIMESTAMPTZ NOT NULL,
	article TEXT NOT NULL,
	views DECIMAL,
	PRIMARY KEY (ts, article)
);
SELECT create_hypertable('wiki_page_views', 'ts');


CREATE TABLE google_trends (
	ts TIMESTAMPTZ NOT NULL,
	keyword TEXT NOT NULL,
	trend DECIMAL,
	PRIMARY KEY (ts, keyword)
);
SELECT create_hypertable('google_trends', 'ts');


CREATE TABLE biz_sentiment (
	ts TIMESTAMPTZ NOT NULL,
	post_id INTEGER NOT NULL,
	positive DECIMAL,
	negative DECIMAL,
	neutral DECIMAL,
	compound DECIMAL,
	PRIMARY KEY (ts, post_id)
);
SELECT create_hypertable('biz_sentiment', 'ts');