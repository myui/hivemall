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

---
#[PA1a]

##Training
```sql
set mapred.reduce.tasks=64;
drop table e2006tfidf_pa1a_model ;
create table e2006tfidf_pa1a_model as
select 
 feature,
 avg(weight) as weight
from 
 (select 
     train_pa1a_regr(addBias(features),target) as (feature,weight)
  from 
     e2006tfidf_train_x3
 ) t 
group by feature;
set mapred.reduce.tasks=-1;
```
_Caution: Do not use voted_avg() for regression. voted_avg() is for classification._

## prediction
```sql
create or replace view e2006tfidf_pa1a_predict
as
select
  t.rowid, 
  sum(m.weight * t.value) as predicted
from 
  e2006tfidf_test_exploded t LEFT OUTER JOIN
  e2006tfidf_pa1a_model m ON (t.feature = m.feature)
group by
  t.rowid;
```

## evaluation
```sql
drop table e2006tfidf_pa1a_submit;
create table e2006tfidf_pa1a_submit as
select 
  t.target as actual, 
  p.predicted as predicted
from 
  e2006tfidf_test t JOIN e2006tfidf_pa1a_predict p 
    on (t.rowid = p.rowid);

select avg(actual), avg(predicted) from e2006tfidf_pa1a_submit;
```
> -3.8200363760415414     -3.8869923258589476

```sql
set hivevar:mean_actual=-3.8200363760415414;

select 
   sqrt(sum(pow(predicted - actual,2.0))/count(1)) as RMSE, 
   sum(pow(predicted - actual,2.0))/count(1) as MSE, 
   sum(abs(predicted - actual))/count(1) as MAE,
   1 - sum(pow(actual - predicted,2.0)) / sum(pow(actual - ${mean_actual},2.0)) as R2
from 
   e2006tfidf_pa1a_submit;
```
> 0.3797959864675519      0.14424499133686086     0.23846059576113587     0.5010367946980386

---
#[PA2a]

##Training
```sql
set mapred.reduce.tasks=64;
drop table e2006tfidf_pa2a_model;
create table e2006tfidf_pa2a_model as
select 
 feature,
 avg(weight) as weight
from 
 (select 
     train_pa2a_regr(addBias(features),target) as (feature,weight)
  from 
     e2006tfidf_train_x3
 ) t 
group by feature;
set mapred.reduce.tasks=-1;
```

## prediction
```sql
create or replace view e2006tfidf_pa2a_predict
as
select
  t.rowid, 
  sum(m.weight * t.value) as predicted
from 
  e2006tfidf_test_exploded t LEFT OUTER JOIN
  e2006tfidf_pa2a_model m ON (t.feature = m.feature)
group by
  t.rowid;
```

## evaluation
```sql
drop table e2006tfidf_pa2a_submit;
create table e2006tfidf_pa2a_submit as
select 
  t.target as actual, 
  pd.predicted as predicted
from 
  e2006tfidf_test t JOIN e2006tfidf_pa2a_predict pd 
    on (t.rowid = pd.rowid);

select avg(actual), avg(predicted) from e2006tfidf_pa2a_submit;
```
> -3.8200363760415414     -3.9124877451612488

```sql
set hivevar:mean_actual=-3.8200363760415414;

select 
   sqrt(sum(pow(predicted - actual,2.0))/count(1)) as RMSE, 
   sum(pow(predicted - actual,2.0))/count(1) as MSE, 
   sum(abs(predicted - actual))/count(1) as MAE,
   1 - sum(pow(actual - predicted,2.0)) / sum(pow(actual - ${mean_actual},2.0)) as R2
from 
   e2006tfidf_pa2a_submit;
```
> 0.38538660838804495     0.14852283792484033     0.2466732002711477      0.48623913673053565

---
#[AROW]

##Training
```sql
set mapred.reduce.tasks=64;
drop table e2006tfidf_arow_model ;
create table e2006tfidf_arow_model as
select 
 feature,
 -- avg(weight) as weight -- [hivemall v0.1]
 argmin_kld(weight, covar) as weight -- [hivemall v0.2 or later]
from 
 (select 
     -- train_arow_regr(addBias(features),target) as (feature,weight)    -- [hivemall v0.1]
     train_arow_regr(addBias(features),target) as (feature,weight,covar) -- [hivemall v0.2 or later]
  from 
     e2006tfidf_train_x3
 ) t 
group by feature;
set mapred.reduce.tasks=-1;
```

## prediction
```sql
create or replace view e2006tfidf_arow_predict
as
select
  t.rowid, 
  sum(m.weight * t.value) as predicted
from 
  e2006tfidf_test_exploded t LEFT OUTER JOIN
  e2006tfidf_arow_model m ON (t.feature = m.feature)
group by
  t.rowid;
```

## evaluation
```sql
drop table e2006tfidf_arow_submit;
create table e2006tfidf_arow_submit as
select 
  t.target as actual, 
  p.predicted as predicted
from 
  e2006tfidf_test t JOIN e2006tfidf_arow_predict p
    on (t.rowid = p.rowid);

select avg(actual), avg(predicted) from e2006tfidf_arow_submit;
```
> -3.8200363760415414     -3.8692518911517433

```sql
set hivevar:mean_actual=-3.8200363760415414;

select 
   sqrt(sum(pow(predicted - actual,2.0))/count(1)) as RMSE, 
   sum(pow(predicted - actual,2.0))/count(1) as MSE, 
   sum(abs(predicted - actual))/count(1) as MAE,
   1 - sum(pow(actual - predicted,2.0)) / sum(pow(actual - ${mean_actual},2.0)) as R2
from 
   e2006tfidf_arow_submit;
```
> 0.37862513029019407     0.14335698928726642     0.2368787001269389      0.5041085155590119

--- 
#[AROWe]
AROWe is a modified version of AROW that uses Hinge loss (epsilion = 0.1)

##Training
```sql
set mapred.reduce.tasks=64;
drop table e2006tfidf_arowe_model ;
create table e2006tfidf_arowe_model as
select 
 feature,
 -- avg(weight) as weight -- [hivemall v0.1]
 argmin_kld(weight, covar) as weight -- [hivemall v0.2 or later]
from 
 (select 
     -- train_arowe_regr(addBias(features),target) as (feature,weight)    -- [hivemall v0.1]
     train_arowe_regr(addBias(features),target) as (feature,weight,covar) -- [hivemall v0.2 or later]
  from 
     e2006tfidf_train_x3
 ) t 
group by feature;
set mapred.reduce.tasks=-1;
```

## prediction
```sql
create or replace view e2006tfidf_arowe_predict
as
select
  t.rowid, 
  sum(m.weight * t.value) as predicted
from 
  e2006tfidf_test_exploded t LEFT OUTER JOIN
  e2006tfidf_arowe_model m ON (t.feature = m.feature)
group by
  t.rowid;
```

## evaluation
```sql
drop table e2006tfidf_arowe_submit;
create table e2006tfidf_arowe_submit as
select 
  t.target as actual, 
  p.predicted as predicted
from 
  e2006tfidf_test t JOIN e2006tfidf_arowe_predict p
    on (t.rowid = p.rowid);

select avg(actual), avg(predicted) from e2006tfidf_arowe_submit;
```
> -3.8200363760415414     -3.86494905688414

```sql
set hivevar:mean_actual=-3.8200363760415414;

select 
   sqrt(sum(pow(predicted - actual,2.0))/count(1)) as RMSE, 
   sum(pow(predicted - actual,2.0))/count(1) as MSE, 
   sum(abs(predicted - actual))/count(1) as MAE,
   1 - sum(pow(actual - predicted,2.0)) / sum(pow(actual - ${mean_actual},2.0)) as R2
from 
   e2006tfidf_arowe_submit;
```
> 0.37789148212861856     0.14280197226536404     0.2357339155291536      0.5060283955470721
