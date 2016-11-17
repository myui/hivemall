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
        
[http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary.html#kdd2010 (algebra)](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary.html#kdd2010 (algebra))

* the number of classes: 2
* the number of data: 8,407,752 (training) / 510,302 (testing)
* the number of features: 20,216,830 in about 2.73 GB (training) / 20,216,830 (testing) 

---
# Define training/testing tables
```sql
add jar ./tmp/hivemall.jar;
source ./tmp/define-all.hive;

create database kdd2010;
use kdd2010;

create external table kdd10a_train (
  rowid int,
  label int,
  features ARRAY<STRING>
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY "," 
STORED AS TEXTFILE LOCATION '/dataset/kdd10a/train';

create external table kdd10a_test (
  rowid int, 
  label int,
  features ARRAY<STRING>
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY "," 
STORED AS TEXTFILE LOCATION '/dataset/kdd10a/test';
```

# Putting data into HDFS
[conv.awk](https://raw.githubusercontent.com/myui/hivemall/master/scripts/misc/conv.awk)
```sh
awk -f conv.awk kdda | hadoop fs -put - /dataset/kdd10a/train/kdda
awk -f conv.awk kdda.t | hadoop fs -put - /dataset/kdd10a/test/kdda.t
```

# Make auxiliary tables
```sql
create table kdd10a_train_orcfile (
 rowid bigint,
 label int,
 features array<string>
) STORED AS orc tblproperties ("orc.compress"="SNAPPY");

-- SET mapred.reduce.tasks=64;
INSERT OVERWRITE TABLE kdd10a_train_orcfile
select * from kdd10a_train
CLUSTER BY rand();
-- SET mapred.reduce.tasks=-1;

create table kdd10a_test_exploded as
select 
  rowid,
  label,
  split(feature,":")[0] as feature,
  cast(split(feature,":")[1] as float) as value
from 
  kdd10a_test LATERAL VIEW explode(addBias(features)) t AS feature;

set hivevar:xtimes=3;
set hivevar:shufflebuffersize=1000;
-- set hivemall.amplify.seed=32;
create or replace view kdd10a_train_x3
as
select
   rand_amplify(${xtimes}, ${shufflebuffersize}, *) as (rowid, label, features)
from  
   kdd10a_train_orcfile;
```