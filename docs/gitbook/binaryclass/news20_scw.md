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

## UDF preparation
```
use news20;

delete jar /home/myui/tmp/hivemall.jar;
add jar /home/myui/tmp/hivemall.jar;
source /home/myui/tmp/define-all.hive;
```

---
# Confidece Weighted (CW)

## training
```sql
drop table news20b_cw_model1;
create table news20b_cw_model1 as
select 
 feature,
 -- voted_avg(weight) as weight -- [hivemall v0.1]
 argmin_kld(weight, covar) as weight -- [hivemall v0.2 or later]
from 
 (select 
     -- train_cw(addBias(features), label) as (feature, weight) -- [hivemall v0.1]
     train_cw(addBias(features), label) as (feature, weight, covar) -- [hivemall v0.2 or later]
  from 
     news20b_train_x3
 ) t 
group by feature;
```

## prediction
```sql
create or replace view news20b_cw_predict1 
as
select
  t.rowid, 
  sum(m.weight * t.value) as total_weight,
  case when sum(m.weight * t.value) > 0.0 then 1 else -1 end as label
from 
  news20b_test_exploded t LEFT OUTER JOIN
  news20b_cw_model1 m ON (t.feature = m.feature)
group by
  t.rowid;
```

## evaluation
```sql
create or replace view news20b_cw_submit1 
as
select 
  t.rowid,
  t.label as actual, 
  pd.label as predicted
from 
  news20b_test t JOIN news20b_cw_predict1 pd 
    on (t.rowid = pd.rowid);
```

```sql
select count(1)/4996 from news20b_cw_submit1 
where actual = predicted;
```
> 0.9655724579663731

## Cleaning

```sql
drop table news20b_cw_model1;
drop view news20b_cw_predict1;
drop view news20b_cw_submit1;
```

---
# Adaptive Regularization of Weight Vectors (AROW)

## training
```sql
drop table news20b_arow_model1;
create table news20b_arow_model1 as
select 
 feature,
 -- voted_avg(weight) as weight -- [hivemall v0.1]
 argmin_kld(weight, covar) as weight -- [hivemall v0.2 or later]
from 
 (select 
     -- train_arow(addBias(features),label) as (feature,weight) -- [hivemall v0.1]
     train_arow(addBias(features),label) as (feature,weight,covar) -- [hivemall v0.2 or later]
  from 
     news20b_train_x3
 ) t 
group by feature;
```

## prediction
```sql
create or replace view news20b_arow_predict1 
as
select
  t.rowid, 
  sum(m.weight * t.value) as total_weight,
  case when sum(m.weight * t.value) > 0.0 then 1 else -1 end as label
from 
  news20b_test_exploded t LEFT OUTER JOIN
  news20b_arow_model1 m ON (t.feature = m.feature)
group by
  t.rowid;
```

## evaluation
```sql
create or replace view news20b_arow_submit1 as
select 
  t.rowid, 
  t.label as actual, 
  pd.label as predicted
from 
  news20b_test t JOIN news20b_arow_predict1 pd 
    on (t.rowid = pd.rowid);
```

```sql
select count(1)/4996 from news20b_arow_submit1 
where actual = predicted;
```
> 0.9659727782225781

## Cleaning

```sql
drop table news20b_arow_model1;
drop view news20b_arow_predict1;
drop view news20b_arow_submit1;
```

---
# Soft Confidence-Weighted (SCW1)

## training
```sql
drop table news20b_scw_model1;
create table news20b_scw_model1 as
select 
 feature,
 -- voted_avg(weight) as weight -- [hivemall v0.1]
 argmin_kld(weight, covar) as weight -- [hivemall v0.2 or later]
from 
 (select 
     -- train_scw(addBias(features),label) as (feature,weight) -- [hivemall v0.1]
     train_scw(addBias(features),label) as (feature,weight,covar) -- [hivemall v0.2 or later]
  from 
     news20b_train_x3
 ) t 
group by feature;
```

## prediction
```sql
create or replace view news20b_scw_predict1 
as
select
  t.rowid, 
  sum(m.weight * t.value) as total_weight,
  case when sum(m.weight * t.value) > 0.0 then 1 else -1 end as label
from 
  news20b_test_exploded t LEFT OUTER JOIN
  news20b_scw_model1 m ON (t.feature = m.feature)
group by
  t.rowid;
```

## evaluation
```sql
create or replace view news20b_scw_submit1 as
select 
  t.rowid, 
  t.label as actual, 
  pd.label as predicted
from 
  news20b_test t JOIN news20b_scw_predict1 pd 
    on (t.rowid = pd.rowid);
```

```sql
select count(1)/4996 from news20b_scw_submit1 
where actual = predicted;
```
> 0.9661729383506805

## Cleaning

```sql
drop table news20b_scw_model1;
drop view news20b_scw_predict1;
drop view news20b_scw_submit1;
```

---
# Soft Confidence-Weighted (SCW2)

## training
```sql
drop table news20b_scw2_model1;
create table news20b_scw2_model1 as
select 
 feature,
 -- voted_avg(weight) as weight -- [hivemall v0.1]
 argmin_kld(weight, covar) as weight -- [hivemall v0.2 or later]
from 
 (select 
     -- train_scw2(addBias(features),label) as (feature,weight)    -- [hivemall v0.1]
     train_scw2(addBias(features),label) as (feature,weight,covar) -- [hivemall v0.2 or later]
  from 
     news20b_train_x3
 ) t 
group by feature;
```

## prediction
```sql
create or replace view news20b_scw2_predict1 
as
select
  t.rowid, 
  sum(m.weight * t.value) as total_weight,
  case when sum(m.weight * t.value) > 0.0 then 1 else -1 end as label
from 
  news20b_test_exploded t LEFT OUTER JOIN
  news20b_scw2_model1 m ON (t.feature = m.feature)
group by
  t.rowid;
```

## evaluation
```sql
create or replace view news20b_scw2_submit1 as
select 
  t.rowid, 
  t.label as actual, 
  pd.label as predicted
from 
  news20b_test t JOIN news20b_scw2_predict1 pd 
    on (t.rowid = pd.rowid);
```

```sql
select count(1)/4996 from news20b_scw2_submit1 
where actual = predicted;
```
> 0.9579663730984788

## Cleaning

```sql
drop table news20b_scw2_model1;
drop view news20b_scw2_predict1;
drop view news20b_scw2_submit1;
```

--

| Algorithm | Accuracy |
|:-----------|------------:|
| Perceptron | 0.9459567654123299 |
| SCW2 | 0.9579663730984788 |
| PA2 | 0.9597678142514011 |
| PA1 | 0.9601681345076061 |
| PA | 0.9603682946357086 |
| CW | 0.9655724579663731 |
| AROW | 0.9659727782225781 |
| SCW1 | 0.9661729383506805 |

My recommendation is AROW for classification.