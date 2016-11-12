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
        
# Preparation

```
use webspam;

delete jar ./tmp/hivemall.jar;
add jar ./tmp/hivemall.jar;
source ./tmp/define-all.hive;
```

# PA1

```sql
drop table webspam_pa1_model1;
create table webspam_pa1_model1 as
select 
 feature,
 cast(voted_avg(weight) as float) as weight
from 
 (select 
     train_pa1(features,label) as (feature,weight) -- sparse model
     -- train_pa1(features,label,"-dense -dims 33554432") as (feature,weight)
  from 
     webspam_train_x3
 ) t 
group by feature;

create or replace view webspam_pa1_predict1 
as
select
  t.rowid, 
  sum(m.weight * t.value) as total_weight,
  case when sum(m.weight * t.value) > 0.0 then 1 else -1 end as label
from 
  webspam_test_exploded t LEFT OUTER JOIN
  webspam_pa1_model1 m ON (t.feature = m.feature)
group by
  t.rowid;

create or replace view webspam_pa1_submit1 as
select 
  t.rowid, 
  t.label as actual, 
  pd.label as predicted
from 
  webspam_test t JOIN webspam_pa1_predict1 pd 
    on (t.rowid = pd.rowid);

select count(1)/70000 from webspam_pa1_submit1 
where actual = predicted;
```
> Prediction accuracy: 0.9628428571428571

# AROW

```sql
drop table webspam_arow_model1;
create table webspam_arow_model1 as
select 
 feature,
 argmin_kld(weight,covar)as weight
from 
 (select 
     train_arow(features,label) as (feature,weight,covar)
  from 
     webspam_train_x3
 ) t 
group by feature;

create or replace view webspam_arow_predict1 
as
select
  t.rowid, 
  sum(m.weight * t.value) as total_weight,
  case when sum(m.weight * t.value) > 0.0 then 1 else -1 end as label
from 
  webspam_test_exploded t LEFT OUTER JOIN
  webspam_arow_model1 m ON (t.feature = m.feature)
group by
  t.rowid;

create or replace view webspam_arow_submit1 as
select 
  t.rowid, 
  t.label as actual, 
  pd.label as predicted
from 
  webspam_test t JOIN webspam_arow_predict1 pd 
    on (t.rowid = pd.rowid);

select count(1)/70000 from webspam_arow_submit1 
where actual = predicted;
```
> Prediction accuracy: 0.9747428571428571

# SCW1

```sql
drop table webspam_scw_model1;
create table webspam_scw_model1 as
select 
 feature,
 argmin_kld(weight,covar)as weight
from 
 (select 
     train_scw(features,label) as (feature,weight,covar)
  from 
     webspam_train_x3
 ) t 
group by feature;

create or replace view webspam_scw_predict1 
as
select
  t.rowid, 
  sum(m.weight * t.value) as total_weight,
  case when sum(m.weight * t.value) > 0.0 then 1 else -1 end as label
from 
  webspam_test_exploded t LEFT OUTER JOIN
  webspam_scw_model1 m ON (t.feature = m.feature)
group by
  t.rowid;

create or replace view webspam_scw_submit1 as
select 
  t.rowid, 
  t.label as actual, 
  pd.label as predicted
from 
  webspam_test t JOIN webspam_scw_predict1 pd 
    on (t.rowid = pd.rowid);

select count(1)/70000 from webspam_scw_submit1 
where actual = predicted;
```
> Prediction accuracy: 0.9778714285714286