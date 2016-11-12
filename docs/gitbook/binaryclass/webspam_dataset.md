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
        
Get the dataset from 
http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary.html#webspam

# Putting data on HDFS
```sql
hadoop fs -mkdir -p /dataset/webspam/raw

awk -f conv.awk webspam_wc_normalized_trigram.svm | \
hadoop fs -put - /dataset/webspam/raw/
```

# Table preparation
```sql
create database webspam;
use webspam;

delete jar ./tmp/hivemall.jar;
add jar ./tmp/hivemall.jar;
source ./tmp/define-all.hive;

create external table webspam_raw (
  rowid int,
  label int,
  features ARRAY<STRING>
) ROW FORMAT 
DELIMITED FIELDS TERMINATED BY '\t' 
COLLECTION ITEMS TERMINATED BY "," 
STORED AS TEXTFILE LOCATION '/dataset/webspam/raw';

set hive.sample.seednumber=43;
create table webspam_test
as
select * from webspam_raw TABLESAMPLE(1000 ROWS) s
CLUSTER BY rand(43)
limit 70000;
```

# Make auxiliary tables
```sql
create table webspam_train_orcfile (
 rowid int,
 label int,
 features array<string>
) STORED AS orc tblproperties ("orc.compress"="SNAPPY");

-- SET mapred.reduce.tasks=128;
INSERT OVERWRITE TABLE webspam_train_orcfile
select
  s.rowid, 
  label,
  addBias(features) as features
from webspam_raw s
where not exists (select rowid from webspam_test t where s.rowid = t.rowid)
CLUSTER BY rand(43);
-- SET mapred.reduce.tasks=-1;

set hivevar:xtimes=3;
set hivevar:shufflebuffersize=100;
set hivemall.amplify.seed=32;
create or replace view webspam_train_x3
as
select
   rand_amplify(${xtimes}, ${shufflebuffersize}, *) as (rowid, label, features)
from  
   webspam_train_orcfile;

create table webspam_test_exploded as
select 
  rowid,
  label,
  split(feature,":")[0] as feature,
  cast(split(feature,":")[1] as float) as value
from 
  webspam_test LATERAL VIEW explode(addBias(features)) t AS feature;
```
*Caution:* For this dataset, use small *shufflebuffersize* because each training example has lots of features though (xtimes * shufflebuffersize * N) training examples are cached in memory.