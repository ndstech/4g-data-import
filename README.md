### 4G Data Import


#### How to install
```
$ go get github.com/ndstech/4g-data-import
```


#### How to use
```
4g-data-import --connection "host=192.168.2.5 user=demo password=demo sslmode=disable" --db-name db_demo --table 4g_hourly --file test.csv --workers 4
```
