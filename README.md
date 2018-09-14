### Modified version of [timescaledb-parallel-copy](https://github.com/timescale/timescaledb-parallel-copy) for LTE hourly data


#### How to install
```
$ go get github.com/yogawa/lte-hourly
```


#### How to use
```
lte-hourly --connection "host=192.168.2.5 user=demo password=demo sslmode=disable" --db-name db_demo --table lte_hourly --file 20180801.csv --workers 4
```
