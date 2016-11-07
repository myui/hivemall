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
delete jar /home/myui/tmp/hivemall.jar;
add jar /home/myui/tmp/hivemall.jar;

source /home/myui/tmp/define-all.hive;
```

---
#[Perceptron]

## model building
```sql
drop table news20b_perceptron_model1;
create table news20b_perceptron_model1 as
select 
 feature,
 voted_avg(weight) as weight
from 
 (select 
     perceptron(addBias(features),label) as (feature,weight)
  from 
     news20b_train_x3
 ) t 
group by feature;
```

## prediction
```sql
create or replace view news20b_perceptron_predict1 
as
select
  t.rowid, 
  sum(m.weight * t.value) as total_weight,
  case when sum(m.weight * t.value) > 0.0 then 1 else -1 end as label
from 
  news20b_test_exploded t LEFT OUTER JOIN
  news20b_perceptron_model1 m ON (t.feature = m.feature)
group by
  t.rowid;
```

## evaluation
```sql
create or replace view news20b_perceptron_submit1 as
select 
  t.label as actual, 
  pd.label as predicted
from 
  news20b_test t JOIN news20b_perceptron_predict1 pd 
    on (t.rowid = pd.rowid);
```

```sql
select count(1)/4996 from news20b_perceptron_submit1 
where actual == predicted;
```
> 0.9459567654123299

## Cleaning

```sql
drop table news20b_perceptron_model1;
drop view news20b_perceptron_predict1;
drop view news20b_perceptron_submit1;
```

---
#[Passive Aggressive]

## model building
```sql
drop table news20b_pa_model1;
create table news20b_pa_model1 as
select 
 feature,
 voted_avg(weight) as weight
from 
 (select 
     train_pa(addBias(features),label) as (feature,weight)
  from 
     news20b_train_x3
 ) t 
group by feature;
```

## prediction
```sql
create or replace view news20b_pa_predict1 
as
select
  t.rowid, 
  sum(m.weight * t.value) as total_weight,
  case when sum(m.weight * t.value) > 0.0 then 1 else -1 end as label
from 
  news20b_test_exploded t LEFT OUTER JOIN
  news20b_pa_model1 m ON (t.feature = m.feature)
group by
  t.rowid;
```

## evaluation
```
create or replace view news20b_pa_submit1 as
select 
  t.label as actual, 
  pd.label as predicted
from 
  news20b_test t JOIN news20b_pa_predict1 pd 
    on (t.rowid = pd.rowid);
```

```sql
select count(1)/4996 from news20b_pa_submit1 
where actual == predicted;
```
> 0.9603682946357086

## Cleaning

```sql
drop table news20b_pa_model1;
drop view news20b_pa_predict1;
drop view news20b_pa_submit1;
```

---
#[Passive Aggressive (PA1)]

## model building
```sql
drop table news20b_pa1_model1;
create table news20b_pa1_model1 as
select 
 feature,
 voted_avg(weight) as weight
from 
 (select 
     train_pa1(addBias(features),label) as (feature,weight)
  from 
     news20b_train_x3
 ) t 
group by feature;
```

## prediction
```sql
create or replace view news20b_pa1_predict1 
as
select
  t.rowid, 
  sum(m.weight * t.value) as total_weight,
  case when sum(m.weight * t.value) > 0.0 then 1 else -1 end as label
from 
  news20b_test_exploded t LEFT OUTER JOIN
  news20b_pa1_model1 m ON (t.feature = m.feature)
group by
  t.rowid;
```

## evaluation
```sql
create or replace view news20b_pa1_submit1 as
select 
  t.label as actual, 
  pd.label as predicted
from 
  news20b_test t JOIN news20b_pa1_predict1 pd 
    on (t.rowid = pd.rowid);
```

```sql
select count(1)/4996 from news20b_pa1_submit1 
where actual == predicted;
```
> 0.9601681345076061

## Cleaning

```sql
drop table news20b_pa1_model1;
drop view news20b_pa1_predict1;
drop view news20b_pa1_submit1;
```

---
#[Passive Aggressive (PA2)]

## model building
```sql
drop table news20b_pa2_model1;
create table news20b_pa2_model1 as
select 
 feature,
 voted_avg(weight) as weight
from 
 (select 
     train_pa2(addBias(features),label) as (feature,weight)
  from 
     news20b_train_x3
 ) t 
group by feature;
```

## prediction
```sql
create or replace view news20b_pa2_predict1 
as
select
  t.rowid, 
  sum(m.weight * t.value) as total_weight,
  case when sum(m.weight * t.value) > 0.0 then 1 else -1 end as label
from 
  news20b_test_exploded t LEFT OUTER JOIN
  news20b_pa2_model1 m ON (t.feature = m.feature)
group by
  t.rowid;
```

## evaluation
```sql
create or replace view news20b_pa2_submit1 as
select 
  t.label as actual, 
  pd.label as predicted
from 
  news20b_test t JOIN news20b_pa2_predict1 pd 
    on (t.rowid = pd.rowid);
```

```sql
select count(1)/4996 from news20b_pa2_submit1 
where actual == predicted;
```
> 0.9597678142514011

## Cleaning

```sql
drop table news20b_pa2_model1;
drop view news20b_pa2_predict1;
drop view news20b_pa2_submit1;
```