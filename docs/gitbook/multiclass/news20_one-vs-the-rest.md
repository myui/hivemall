A one-vs-the-rest classifier use the binary classifier for each class.

## UDF preparation
```sql
delete jar /home/myui/tmp/hivemall.jar;
add jar /home/myui/tmp/hivemall.jar;

source /home/myui/tmp/define-all.hive;
```

## training
```sql
SET mapred.reduce.tasks=4;

drop table news20_onevsrest_arow_model;
create table news20_onevsrest_arow_model 
as
select
  label,
  feature,
  -- voted_avg(weight) as weight -- [hivemall v0.1]
  argmin_kld(weight, covar) as weight -- [hivemall v0.2 or later]
from (
select
  1 as label,
  *
from (
select 
  -- train_arow(features, target) as (feature, weight)     -- [hivemall v0.1]
  train_arow(features, target) as (feature, weight, covar) -- [hivemall v0.2 or later]
from 
  news20_onevsrest_train_x3
where
  label = 1
) t1
union all
select
  2 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 2
) t2
union all
select
  3 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 3
) t3
union all
select
  4 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 4
) t4
union all
select
  5 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 5
) t5
union all
select
  6 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 6
) t6
union all
select
  7 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 7
) t7
union all
select
  8 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 8
) t8
union all
select
  9 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 9
) t9
union all
select
  10 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 10
) t10
union all
select
  11 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 11
) t11
union all
select
  12 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 12
) t12
union all
select
  13 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 13
) t13
union all
select
  14 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 14
) t14
union all
select
  15 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 15
) t15
union all
select
  16 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 16
) t16
union all
select
  17 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 17
) t17
union all
select
  18 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 18
) t18
union all
select
  19 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 19
) t19
union all
select
  20 as label,
  *
from (
select 
  train_arow(features, target) as (feature, weight, covar)
from 
  news20_onevsrest_train_x3
where
  label = 20
) t20
) t
group by 
  label, feature;

-- reset to the default
SET mapred.reduce.tasks=-1;
```
Note that the above query is optimized to scan news20_onevsrest_train_x3 once!

## prediction
```sql
create or replace view news20_onevsrest_arow_predict 
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
    news20_onevsrest_arow_model m ON (t.feature = m.feature)
  group by
    t.rowid, m.label
) t1
group by rowid
) t2;
```

## evaluation
```sql
create or replace view news20_onevsrest_arow_submit as
select 
  t.label as actual, 
  pd.label as predicted
from 
  news20mc_test t JOIN news20_onevsrest_arow_predict pd 
    on (t.rowid = pd.rowid);
```

```
select count(1)/3993 from news20_onevsrest_arow_submit
where actual == predicted;
```

> 0.8567493112947658

## Cleaning

```sql
drop table news20_onevsrest_arow_model1;
drop view news20_onevsrest_arow_predict1;
drop view news20_onevsrest_arow_submit1;
```

| Algorithm | Accuracy |
|:-----------|------------:|
| AROW(multi-class) | 0.8474830954169797 |
| CW |  0.850488354620586 |
| AROW(one-vs-rest) | 0.8567493112947658 |
