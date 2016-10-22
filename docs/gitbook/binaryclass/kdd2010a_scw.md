# PA1
## Train
```sql
-- SET mapred.reduce.tasks=32;
drop table kdd10a_pa1_model1;
create table kdd10a_pa1_model1 as
select 
 feature,
 voted_avg(weight) as weight
from 
 (select 
     train_pa1(addBias(features),label) as (feature,weight)
  from 
     kdd10a_train_x3
 ) t 
group by feature;
```

## Predict
```sql
create or replace view kdd10a_pa1_predict1 
as
select
  t.rowid, 
  sum(m.weight * t.value) as total_weight,
  case when sum(m.weight * t.value) > 0.0 then 1 else -1 end as label
from 
  kdd10a_test_exploded t LEFT OUTER JOIN
  kdd10a_pa1_model1 m ON (t.feature = m.feature)
group by
  t.rowid;
```

# Evaluate
```sql
create or replace view kdd10a_pa1_submit1 as
select 
  t.rowid, 
  t.label as actual, 
  pd.label as predicted
from 
  kdd10a_test t JOIN kdd10a_pa1_predict1 pd 
    on (t.rowid = pd.rowid);

select count(1)/510302 from kdd10a_pa1_submit1 
where actual = predicted;
```
> 0.8677782959894337

# CW
```sql
-- SET mapred.reduce.tasks=32;
drop table kdd10a_cw_model1;
create table kdd10a_cw_model1 as
select 
 feature,
 argmin_kld(weight, covar) as weight
from 
 (select 
     train_cw(addBias(features),label) as (feature,weight,covar)
  from 
     kdd10a_train_x3
 ) t 
group by feature;

create or replace view kdd10a_cw_predict1 
as
select
  t.rowid, 
  sum(m.weight * t.value) as total_weight,
  case when sum(m.weight * t.value) > 0.0 then 1 else -1 end as label
from 
  kdd10a_test_exploded t LEFT OUTER JOIN
  kdd10a_cw_model1 m ON (t.feature = m.feature)
group by
  t.rowid;

create or replace view kdd10a_cw_submit1 as
select 
  t.rowid, 
  t.label as actual, 
  pd.label as predicted
from 
  kdd10a_test t JOIN kdd10a_cw_predict1 pd 
    on (t.rowid = pd.rowid);

select count(1)/510302 from kdd10a_cw_submit1 
where actual = predicted;
```
> 0.8678037711002504

# AROW
```sql
-- SET mapred.reduce.tasks=32;
drop table kdd10a_arow_model1;
create table kdd10a_arow_model1 as
select 
 feature,
 -- voted_avg(weight) as weight
 argmin_kld(weight, covar) as weight -- [hivemall v0.2alpha3 or later]
from 
 (select 
     -- train_arow(addBias(features),label) as (feature,weight) -- [hivemall v0.1]
     train_arow(addBias(features),label) as (feature,weight,covar) -- [hivemall v0.2 or later]
  from 
     kdd10a_train_x3
 ) t 
group by feature;

create or replace view kdd10a_arow_predict1 
as
select
  t.rowid, 
  sum(m.weight * t.value) as total_weight,
  case when sum(m.weight * t.value) > 0.0 then 1 else -1 end as label
from 
  kdd10a_test_exploded t LEFT OUTER JOIN
  kdd10a_arow_model1 m ON (t.feature = m.feature)
group by
  t.rowid;

create or replace view kdd10a_arow_submit1 as
select 
  t.rowid, 
  t.label as actual, 
  pd.label as predicted
from 
  kdd10a_test t JOIN kdd10a_arow_predict1 pd 
    on (t.rowid = pd.rowid);

select count(1)/510302 from kdd10a_arow_submit1 
where actual = predicted;
```
> 0.8676038894615345

# SCW
```sql
-- SET mapred.reduce.tasks=32;
drop table kdd10a_scw_model1;
create table kdd10a_scw_model1 as
select 
 feature,
 argmin_kld(weight, covar) as weight
from 
 (select 
     train_scw(addBias(features),label) as (feature,weight,covar)
  from 
     kdd10a_train_x3
 ) t 
group by feature;

create or replace view kdd10a_scw_predict1 
as
select
  t.rowid, 
  sum(m.weight * t.value) as total_weight,
  case when sum(m.weight * t.value) > 0.0 then 1 else -1 end as label
from 
  kdd10a_test_exploded t LEFT OUTER JOIN
  kdd10a_scw_model1 m ON (t.feature = m.feature)
group by
  t.rowid;

create or replace view kdd10a_scw_submit1 as
select 
  t.rowid, 
  t.label as actual, 
  pd.label as predicted
from 
  kdd10a_test t JOIN kdd10a_scw_predict1 pd 
    on (t.rowid = pd.rowid);

select count(1)/510302 from kdd10a_scw_submit1 
where actual = predicted;
```
> 0.8678096499719774

---

| Algorithm | Accuracy |
|:-----------|------------:|
| AROW | 0.8676038894615345 |
| PA1 | 0.8677782959894337 |
| CW | 0.8678037711002504 |
| SCW1 | 0.8678096499719774 |