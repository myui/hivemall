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
        
# Dataset prepration
Iris Dataset: https://archive.ics.uci.edu/ml/datasets/Iris

```sh
$ wget http://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data
$ less iris.data

   ...
5.3,3.7,1.5,0.2,Iris-setosa
5.0,3.3,1.4,0.2,Iris-setosa
7.0,3.2,4.7,1.4,Iris-versicolor
   ...
```

# Create training/test table in Hive

```sql
create database iris;
use iris;

create external table iris_raw (
  rowid int,
  label string,
  features array<float>
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' COLLECTION ITEMS TERMINATED BY "," STORED AS TEXTFILE LOCATION '/dataset/iris/raw';
```

# Loading data into HDFS

```sh
$ awk -F"," 'NF >0 {OFS="|"; print NR,$5,$1","$2","$3","$4}' iris.data | head -3

1|Iris-setosa|5.1,3.5,1.4,0.2
2|Iris-setosa|4.9,3.0,1.4,0.2
3|Iris-setosa|4.7,3.2,1.3,0.2
```

```sh
$ awk -F"," 'NF >0 {OFS="|"; print NR,$5,$1","$2","$3","$4}' iris.data | hadoop fs -put - /dataset/iris/raw/iris.data
```

```sql
select count(1) from iris_raw;

> 150
```

# Feature scaling

Normalization of feature weights is very important to get a good prediction in machine learning.

```sql
select 
  min(features[0]), max(features[0]),
  min(features[1]), max(features[1]),
  min(features[2]), max(features[2]),
  min(features[3]), max(features[3])
from
  iris_raw;

> 4.3     7.9     2.0     4.4     1.0     6.9     0.1     2.5
```

```sql
set hivevar:f0_min=4.3;
set hivevar:f0_max=7.9;
set hivevar:f1_min=2.0;
set hivevar:f1_max=4.4;
set hivevar:f2_min=1.0;
set hivevar:f2_max=6.9;
set hivevar:f3_min=0.1;
set hivevar:f3_max=2.5;

create or replace view iris_scaled
as
select
  rowid, 
  label,
  add_bias(array(
     concat("1:", rescale(features[0],${f0_min},${f0_max})), 
     concat("2:", rescale(features[1],${f1_min},${f1_max})), 
     concat("3:", rescale(features[2],${f2_min},${f2_max})), 
     concat("4:", rescale(features[3],${f3_min},${f3_max}))
  )) as features
from 
  iris_raw;
```

```sql
select * from iris_scaled limit 3;

> 1       Iris-setosa     ["1:0.22222215","2:0.625","3:0.0677966","4:0.041666664","0:1.0"]
> 2       Iris-setosa     ["1:0.16666664","2:0.41666666","3:0.0677966","4:0.041666664","0:1.0"]
> 3       Iris-setosa     ["1:0.11111101","2:0.5","3:0.05084745","4:0.041666664","0:1.0"]
```

_[LibSVM web page](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass.html#iris) provides a normalized (using [ZScore](https://github.com/myui/hivemall/wiki/Feature-scaling)) version of Iris dataset._

# Create training/test data

```sql
set hivevar:rand_seed=31;

create table iris_shuffled 
as
select rand(${rand_seed}) as rnd, * from iris_scaled;

-- 80% for training
create table train80p as
select * from  iris_shuffled 
order by rnd DESC
limit 120;

-- 20% for testing
create table test20p as
select * from  iris_shuffled 
order by rnd ASC
limit 30;

create table test20p_exploded 
as
select 
  rowid,
  label,
  extract_feature(feature) as feature,
  extract_weight(feature) as value
from 
  test20p LATERAL VIEW explode(features) t AS feature;
```

# Define an amplified view for the training input
```sql
set hivevar:xtimes=10;
set hivevar:shufflebuffersize=1000;

create or replace view training_x10
as
select
   rand_amplify(${xtimes}, ${shufflebuffersize}, rowid, label, features) as (rowid, label, features)
from  
   train80p;
```

# Training (multiclass classification)

```sql
create table model_scw1 as
select 
 label, 
 feature,
 argmin_kld(weight, covar) as weight
from 
 (select 
     train_multiclass_scw(features, label) as (label, feature, weight, covar)
  from 
     training_x10
 ) t 
group by label, feature;
```

# Predict

```sql
create or replace view predict_scw1
as
select 
  rowid, 
  m.col0 as score, 
  m.col1 as label
from (
select
   rowid, 
   maxrow(score, label) as m
from (
  select
    t.rowid,
    m.label,
    sum(m.weight * t.value) as score
  from 
    test20p_exploded t LEFT OUTER JOIN
    model_scw1 m ON (t.feature = m.feature)
  group by
    t.rowid, m.label
) t1
group by rowid
) t2;
```

# Evaluation

```sql
create or replace view eval_scw1 as
select 
  t.label as actual, 
  p.label as predicted
from 
  test20p t JOIN predict_scw1 p 
    on (t.rowid = p.rowid);

select count(1)/30 from eval_scw1 
where actual = predicted;
```
> 0.9666666666666667
