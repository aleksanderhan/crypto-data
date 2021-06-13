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
