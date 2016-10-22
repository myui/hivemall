a9a
===
http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary.html#a9a

---

preparation
=========

[conv.awk](https://raw.githubusercontent.com/myui/hivemall/master/resources/misc/conv.awk)

```
cd /mnt/archive/datasets/classification/a9a
awk -f conv.awk a9a | sed -e "s/+1/1/" | sed -e "s/-1/0/" > a9a.train
awk -f conv.awk a9a.t | sed -e "s/+1/1/" | sed -e "s/-1/0/" > a9a.test
```

## Putting data on HDFS
```
hadoop fs -mkdir -p /dataset/a9a/train
hadoop fs -mkdir -p /dataset/a9a/test

hadoop fs -copyFromLocal a9a.train /dataset/a9a/train
hadoop fs -copyFromLocal a9a.test /dataset/a9a/test
```

## Training/test data prepareation
```sql
create database a9a;
use a9a;

create external table a9atrain (
  rowid int,
  label float,
  features ARRAY<STRING>
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY "," STORED AS TEXTFILE LOCATION '/dataset/a9a/train';

create external table a9atest (
  rowid int, 
  label float,
  features ARRAY<STRING>
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY "," STORED AS TEXTFILE LOCATION '/dataset/a9a/test';
```