# crypto-data



## Setup db

docker network create --driver=bridge crypto-data
docker run -d --name timescaledb -p 5432:5432 --net=crypto-data -e POSTGRES_PASSWORD=password timescale/timescaledb:2.0.1-pg12

psql postgresql://postgres:password@localhost:5432/postgres -f sql/00_init_db.sql
psql postgresql://crypto:secret@localhost:5432/cryptodata -f sql/01_create_tables.sql


## Links

* https://pageviews.toolforge.org/
* https://wikimedia.org/api/rest_v1/#/Pageviews%20data/