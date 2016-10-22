Get the news20b dataset.
http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary.html#news20.binary

```sh
cat <<EOF > conv.awk
BEGIN{ FS=" " }
{
    label=\$1;
    features=\$2;
    for(i=3;i<=NF;i++)
    {
        features = features "," \$i;
    }
    print NR "\t" label "\t" features;
}
END{}
EOF

sort -R news20.binary > news20.random
# [mac]
# $ brew install coreutils
# $ gsort -R news20.binary > news20.random
head -15000 news20.random > news20.train
tail -4996 news20.random > news20.test
gawk -f conv.awk news20.train > news20.train.t
gawk -f conv.awk news20.test > news20.test.t
```

## Putting data on HDFS
```
hadoop fs -mkdir -p /dataset/news20-binary/train
hadoop fs -mkdir -p /dataset/news20-binary/test

hadoop fs -copyFromLocal news20.train.t /dataset/news20-binary/train
hadoop fs -copyFromLocal news20.test.t /dataset/news20-binary/test
```

## Training/test data prepareation
```sql
create database news20;
use news20;

delete jar /home/myui/tmp/hivemall.jar;
add jar /home/myui/tmp/hivemall.jar;

source /home/myui/tmp/define-all.hive;

Create external table news20b_train (
  rowid int,
  label int,
  features ARRAY<STRING>
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY "," STORED AS TEXTFILE LOCATION '/dataset/news20-binary/train';

Create external table news20b_test (
  rowid int, 
  label int,
  features ARRAY<STRING>
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY "," STORED AS TEXTFILE LOCATION '/dataset/news20-binary/test';

set hivevar:seed=31;
create or replace view news20b_train_x3
as
select 
  * 
from (
select
   amplify(3, *) as (rowid, label, features)
from  
   news20b_train 
) t
CLUSTER BY rand(${seed});

create table news20b_test_exploded as
select 
  rowid,
  label,
  cast(split(feature,":")[0] as int) as feature,
  cast(split(feature,":")[1] as float) as value
  -- hivemall v0.3.1 or later
  -- extract_feature(feature) as feature,
  -- extract_weight(feature) as value
from 
  news20b_test LATERAL VIEW explode(addBias(features)) t AS feature;
```