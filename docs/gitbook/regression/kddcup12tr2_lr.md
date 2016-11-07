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
        
The task is predicting the click through rate (CTR) of advertisement, meaning that we are to predict the probability of each ad being clicked.   
http://www.kddcup2012.org/c/kddcup2012-track2

_Caution: This example just shows a baseline result. Use token tables and amplifier to get better AUC score._

---
Logistic Regression
===============

## Training
```sql
use kdd12track2;

-- set mapred.max.split.size=134217728; -- [optional] set if OOM caused at mappers on training
-- SET mapred.max.split.size=67108864;
select count(1) from training_orcfile;
```
> 235582879

235582879 / 56 (mappers) = 4206837

```sql
set hivevar:total_steps=5000000;
-- set mapred.reduce.tasks=64; -- [optional] set the explicit number of reducers to make group-by aggregation faster

drop table lr_model;
create table lr_model 
as
select 
 feature,
 cast(avg(weight) as float) as weight
from 
 (select 
     logress(features, label, "-total_steps ${total_steps}") as (feature,weight)
     -- logress(features, label) as (feature,weight)
  from 
     training_orcfile
 ) t 
group by feature;

-- set mapred.max.split.size=-1; -- reset to the default value
```
_Note: Setting the "-total_steps" option is optional._

## Prediction
```
drop table lr_predict;
create table lr_predict
  ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY "\t"
    LINES TERMINATED BY "\n"
  STORED AS TEXTFILE
as
select
  t.rowid, 
  sigmoid(sum(m.weight)) as prob
from 
  testing_exploded  t LEFT OUTER JOIN
  lr_model m ON (t.feature = m.feature)
group by 
  t.rowid
order by 
  rowid ASC;
```
## Evaluation

[scoreKDD.py](https://github.com/myui/hivemall/blob/master/resources/examples/kddtrack2/scoreKDD.py)

```sh
hadoop fs -getmerge /user/hive/warehouse/kdd12track2.db/lr_predict lr_predict.tbl

gawk -F "\t" '{print $2;}' lr_predict.tbl > lr_predict.submit

pypy scoreKDD.py KDD_Track2_solution.csv  lr_predict.submit
```
_Note: You can use python instead of pypy._

| Measure | Score |
|:-----------|------------:|
| AUC  | 0.741111 |
| NWMAE | 0.045493 |
| WRMSE | 0.142395 |
---
Passive Aggressive
===============

## Training
```
drop table pa_model;
create table pa_model 
as
select 
 feature,
 cast(avg(weight) as float) as weight
from 
 (select 
     train_pa1a_regr(features,label) as (feature,weight)
  from 
     training_orcfile
 ) t 
group by feature;
```
_PA1a is recommended when using PA for regression._

## Prediction
```
drop table pa_predict;
create table pa_predict
  ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY "\t"
    LINES TERMINATED BY "\n"
  STORED AS TEXTFILE
as
select
  t.rowid, 
  sum(m.weight) as prob
from 
  testing_exploded  t LEFT OUTER JOIN
  pa_model m ON (t.feature = m.feature)
group by 
  t.rowid
order by 
  rowid ASC;
```
_The "prob" of PA can be used only for ranking and can have a negative value. A higher weight means much likely to be clicked. Note that AUC is sort a measure for evaluating ranking accuracy._

## Evaluation

```sh
hadoop fs -getmerge /user/hive/warehouse/kdd12track2.db/pa_predict pa_predict.tbl

gawk -F "\t" '{print $2;}' pa_predict.tbl > pa_predict.submit

pypy scoreKDD.py KDD_Track2_solution.csv  pa_predict.submit
```

| Measure | Score |
|:-----------|------------:|
| AUC  | 0.739722 |
| NWMAE | 0.049582 |
| WRMSE | 0.143698 |