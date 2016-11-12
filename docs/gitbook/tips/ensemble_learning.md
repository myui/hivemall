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
        
This example explains how to run ensemble learning in Hivemall.   
Two heads are better than one? Let's verify it by ensemble learning.

<!-- toc -->

---

## UDF preparation
```sql
delete jar /home/myui/tmp/hivemall.jar;
add jar /home/myui/tmp/hivemall.jar;

source /home/myui/tmp/define-all.hive;
```

# [Case1] Model ensemble/mixing

## training
```sql
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=8;
SET mapred.reduce.tasks=4;

drop table news20mc_ensemble_model1;
create table news20mc_ensemble_model1 as
select 
 label, 
 -- cast(feature as int) as feature, -- hivemall v0.1
 argmin_kld(feature, covar) as feature, -- hivemall v0.2 or later
 voted_avg(weight) as weight
from 
 (select 
     -- train_multiclass_cw(addBias(features),label) as (label,feature,weight)      -- hivemall v0.1
     train_multiclass_cw(addBias(features),label) as (label,feature,weight,covar)   -- hivemall v0.2 or later
  from 
     news20mc_train_x3
  union all
  select 
     -- train_multiclass_arow(addBias(features),label) as (label,feature,weight)    -- hivemall v0.1
     train_multiclass_arow(addBias(features),label) as (label,feature,weight,covar) -- hivemall v0.2 or later
  from 
     news20mc_train_x3
  union all
  select 
     -- train_multiclass_scw(addBias(features),label) as (label,feature,weight)     -- hivemall v0.1
     train_multiclass_scw(addBias(features),label) as (label,feature,weight,covar)  -- hivemall v0.2 or later
  from 
     news20mc_train_x3
 ) t 
group by label, feature;

-- reset to the default
SET hive.exec.parallel=false;
SET mapred.reduce.tasks=-1;
```

## prediction
```sql
create or replace view news20mc_ensemble_predict1 
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
    news20mc_test_exploded t LEFT OUTER JOIN
    news20mc_ensemble_model1 m ON (t.feature = m.feature)
  group by
    t.rowid, m.label
) t1
group by rowid
) t2;
```

## evaluation
```sql
create or replace view news20mc_ensemble_submit1 as
select 
  t.label as actual, 
  pd.label as predicted
from 
  news20mc_test t JOIN news20mc_ensemble_predict1 pd 
    on (t.rowid = pd.rowid);
```

```
select count(1)/3993 from news20mc_ensemble_submit1 
where actual == predicted;
```

> 0.8494866015527173

## Cleaning

```sql
drop table news20mc_ensemble_model1;
drop view news20mc_ensemble_predict1;
drop view news20mc_ensemble_submit1;
```
---

Unfortunately, too many cooks spoil the broth in this case :-(

| Algorithm | Accuracy |
|:-----------|------------:|
| AROW | 0.8474830954169797 |
| SCW2 |  0.8482344102178813 |
| Ensemble(model) | 0.8494866015527173 |
| CW |  0.850488354620586 |


---

# [Case2] Prediction ensemble

## prediction
```sql
create or replace view news20mc_pred_ensemble_predict1 
as
select 
  rowid, 
  m.col1 as label
from (
  select
    rowid, 
    maxrow(cnt, label) as m
  from (
    select
      rowid,
      label,
      count(1) as cnt
    from (
      select * from news20mc_arow_predict1
      union all
      select * from news20mc_scw2_predict1
      union all
      select * from news20mc_cw_predict1
    ) t1
    group by rowid, label
  ) t2
  group by rowid
) t3;
```

## evaluation
```sql
create or replace view news20mc_pred_ensemble_submit1 as
select 
  t.label as actual, 
  pd.label as predicted
from 
  news20mc_test t JOIN news20mc_pred_ensemble_predict1 pd 
    on (t.rowid = pd.rowid);
```

```
select count(1)/3993 from news20mc_pred_ensemble_submit1 
where actual == predicted;
```

> 0.8499874780866516

Unfortunately, too many cooks spoil the broth in this case too :-(

| Algorithm | Accuracy |
|:-----------|------------:|
| AROW | 0.8474830954169797 |
| SCW2 |  0.8482344102178813 |
| Ensemble(model) | 0.8494866015527173 |
| Ensemble(prediction) | 0.8499874780866516 |
| CW |  0.850488354620586 |