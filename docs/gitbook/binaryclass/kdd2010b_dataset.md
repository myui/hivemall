[http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary.html#kdd2010 (bridge to algebra)](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary.html#kdd2010 (bridge to algebra))

* # of classes: 2
* # of data: 19,264,097 / 748,401 (testing)
* # of features: 29,890,095 / 29,890,095 (testing)

---
# Define training/testing tables
```sql
add jar ./tmp/hivemall.jar;
source ./tmp/define-all.hive;

create database kdd2010;
use kdd2010;

create external table kdd10b_train (
  rowid int,
  label int,
  features ARRAY<STRING>
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY "," 
STORED AS TEXTFILE LOCATION '/dataset/kdd10b/train';

create external table kdd10b_test (
  rowid int, 
  label int,
  features ARRAY<STRING>
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY "," 
STORED AS TEXTFILE LOCATION '/dataset/kdd10b/test';
```

# Putting data into HDFS
[conv.awk](https://raw.githubusercontent.com/myui/hivemall/master/scripts/misc/conv.awk)
```sh
awk -f conv.awk kddb | hadoop fs -put - /dataset/kdd10b/train/kddb
awk -f conv.awk kddb.t | hadoop fs -put - /dataset/kdd10b/test/kddb.t
```

# Make auxiliary tables
```sql
create table kdd10b_test_exploded as
select 
  rowid,
  label,
  split(feature,":")[0] as feature,
  cast(split(feature,":")[1] as float) as value
from 
  kdd10b_test LATERAL VIEW explode(addBias(features)) t AS feature;

set hivevar:xtimes=3;
set hivevar:shufflebuffersize=1000;
create or replace view kdd10b_train_x3
as
select
   rand_amplify(${xtimes}, ${shufflebuffersize}, *) as (rowid, label, features)
from  
   kdd10b_train;
```
