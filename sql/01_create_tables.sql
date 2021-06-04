CREATE TABLE candles (
	ts TIMESTAMPTZ NOT NULL,
	coin VARCHAR(5) NOT NULL,
	currency VARCHAR(5) NOT NULL,
	low DECIMAL,
	high DECIMAL,
	open DECIMAL,
	close DECIMAL,
	volume DECIMAL,
	PRIMARY KEY(ts, coin, currency)
);
SELECT create_hypertable('candles', 'ts');