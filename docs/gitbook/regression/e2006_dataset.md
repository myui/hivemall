<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->
        
http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/regression.html#E2006-tfidf

Prerequisite
============
* [hivemall.jar](https://github.com/myui/hivemall/tree/master/target/hivemall.jar)
* [conv.awk](https://github.com/myui/hivemall/tree/master/scripts/misc/conv.awk)
* [define-all.hive](https://github.com/myui/hivemall/tree/master/scripts/ddl/define-all.hive)

Data preparation
================

```sh
cd /mnt/archive/datasets/regression/E2006-tfidf
awk -f conv.awk E2006.train > E2006.train.tsv
awk -f conv.awk  E2006.test > E2006.test.tsv

hadoop fs -mkdir -p /dataset/E2006-tfidf/train
hadoop fs -mkdir -p /dataset/E2006-tfidf/test
hadoop fs -put E2006.train.tsv /dataset/E2006-tfidf/train
hadoop fs -put E2006.test.tsv /dataset/E2006-tfidf/test
```

```sql
create database E2006;
use E2006;

delete jar /home/myui/tmp/hivemall.jar;
add jar /home/myui/tmp/hivemall.jar;

source /home/myui/tmp/define-all.hive;

Create external table e2006tfidf_train (
  rowid int,
  target float,
  features ARRAY<STRING>
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY "," 
STORED AS TEXTFILE LOCATION '/dataset/E2006-tfidf/train';

Create external table e2006tfidf_test (
  rowid int, 
  target float,
  features ARRAY<STRING>
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY "," 
STORED AS TEXTFILE LOCATION '/dataset/E2006-tfidf/test';

create table e2006tfidf_test_exploded as
select 
  rowid,
  target,
  split(feature,":")[0] as feature,
  cast(split(feature,":")[1] as float) as value
  -- hivemall v0.3.1 or later
  -- extract_feature(feature) as feature,
  -- extract_weight(feature) as value
from 
  e2006tfidf_test LATERAL VIEW explode(addBias(features)) t AS feature;
```

## Amplify training examples (global shuffle)
```sql
-- set mapred.reduce.tasks=32;
set hivevar:seed=31;
set hivevar:xtimes=3;
create or replace view e2006tfidf_train_x3 as 
select * from (
select amplify(${xtimes}, *) as (rowid, target, features) from e2006tfidf_train
) t
CLUSTER BY rand(${seed});
-- set mapred.reduce.tasks=-1;
```