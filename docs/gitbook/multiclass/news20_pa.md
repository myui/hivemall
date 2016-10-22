Preparation
=========

## UDF preparation
```
delete jar /home/myui/tmp/hivemall.jar;
add jar /home/myui/tmp/hivemall.jar;

source /home/myui/tmp/define-all.hive;
```

---
#[Passive Aggressive (PA2)]

Training
======

## model building
```sql
drop table news20mc_pa2_model1;
create table news20mc_pa2_model1 as
select 
 label, 
 cast(feature as int) as feature,
 voted_avg(weight) as weight
from 
 (select 
     train_multiclass_pa2(addBias(features),label) as (label,feature,weight)
  from 
     news20mc_train_x3
 ) t 
group by label, feature;
```

## prediction
```
create or replace view news20mc_pa2_predict1 
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
    news20mc_pa2_model1 m ON (t.feature = m.feature)
  group by
    t.rowid, m.label
) t1
group by rowid
) t2;
```

## evaluation
```sql
create or replace view news20mc_pa2_submit1 as
select 
  t.label as actual, 
  pd.label as predicted
from 
  news20mc_test t JOIN news20mc_pa2_predict1 pd 
    on (t.rowid = pd.rowid);
```

```sql
select count(1)/3993 from news20mc_pa2_submit1 
where actual == predicted;
```

> 0.7478086651640371 (plain)

> 0.8204357625845229 (x3)

> 0.8204357625845229 (x3 + bagging)

## Cleaning

```sql
drop table news20mc_pa2_model1;
drop table news20mc_pa2_predict1;
drop view news20mc_pa2_submit1;
```