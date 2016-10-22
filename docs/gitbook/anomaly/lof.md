This article introduce how to find outliers using [Local Outlier Detection (LOF)](http://en.wikipedia.org/wiki/Local_outlier_factor) on Hivemall.

# Data Preparation

```sql
create database lof;
use lof;

create external table hundred_balls (
  rowid int, 
  weight double,
  specific_heat double,
  reflectance double
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ' '
STORED AS TEXTFILE LOCATION '/dataset/lof/hundred_balls';
```

Download [hundred_balls.txt](https://github.com/myui/hivemall/blob/master/resources/examples/lof/hundred_balls.txt) that is originally provides in [this article](http://next.rikunabi.com/tech/docs/ct_s03600.jsp?p=002259).

You can find outliers in [this picture](http://next.rikunabi.com/tech/contents/ts_report/img/201303/002259/part1_img1.jpg). As you can see, Rowid `87` is apparently an outlier.

```sh
awk '{FS=" "; OFS=" "; print NR,$0}' hundred_balls.txt | \
hadoop fs -put - /dataset/lof/hundred_balls/hundred_balls.txt
```

```sql
create table train
as
select rowid, array(concat("weight:", weight), concat("specific_heat:", specific_heat), concat("reflectance:", reflectance)) as features
from hundred_balls;
```

## Apply Data Normalization

```sql
create table train_normalized
as
WITH fv as (
select 
  rowid, 
  extract_feature(feature) as feature,
  extract_weight(feature) as value
from 
  train 
  LATERAL VIEW explode(features) exploded AS feature
), 
stats as (
select
  feature,
  -- avg(value) as mean, stddev_pop(value) as stddev
  min(value) as min, max(value) as max
from
  fv
group by
  feature
), 
norm as (
select 
  rowid, 
  t1.feature, 
  -- zscore(t1.value, t2.mean, t2.stddev) as zscore
  rescale(t1.value, t2.min, t2.max) as minmax
from 
  fv t1 JOIN
  stats t2 ON (t1.feature = t2.feature) 
),
norm_fv as (
select
  rowid, 
  -- concat(feature, ":", zscore) as feature
  concat(feature, ":", minmax) as feature
from
  norm
)
select 
  rowid, 
  collect_list(feature) as features
from
  norm_fv
group by
  rowid
;
```

```
hive> select * from train_normalized limit 3;

1       ["reflectance:0.5252967","specific_heat:0.19863537","weight:0.0"]
2       ["reflectance:0.5950446","specific_heat:0.09166764","weight:0.052084323"]
3       ["reflectance:0.6797837","specific_heat:0.12567581","weight:0.13255163"]
```

# Outlier Detection using Local Outlier Facotor (LOF)

```sql
-- workaround to deal with a bug in Hive/Tez
-- https://issues.apache.org/jira/browse/HIVE-10729
-- set hive.auto.convert.join=false;
set hive.mapjoin.optimized.hashtable=false;

-- parameter of LoF
set hivevar:k=12;

-- find topk outliers
set hivevar:topk=3;
```

```sql
create table list_neighbours
as
select
  each_top_k(
    -${k}, t1.rowid, euclid_distance(t1.features, t2.features), 
    t1.rowid, 
    t2.rowid
  ) as (rank, distance, target, neighbour)
from 
  train_normalized t1
  LEFT OUTER JOIN train_normalized t2
where
  t1.rowid != t2.rowid
;
```

_Note: `list_neighbours` table SHOULD be created because `list_neighbours` is used multiple times._

_Note: [`each_top_k`](https://github.com/myui/hivemall/pull/196) is supported from Hivemall v0.3.2-3 or later._

_Note: To parallelize a top-k computation, break LEFT-hand table into piece as describe in [this page](https://github.com/myui/hivemall/wiki/Efficient-Top-k-computation-on-Apache-Hive-using-Hivemall-UDTF#parallelization-of-similarity-computation-using-with-clause)._

```sql
WITH k_distance as (
select
  target, 
  max(distance) as k_distance
from
  list_neighbours
group by
  target
), 
reach_distance as (
select
  t1.target,
  max2(t2.k_distance, t1.distance) as reach_distance
from
  list_neighbours t1 JOIN 
  k_distance t2 ON (t1.neighbour = t2.target)
), 
lrd as (
select
  target,   
  1.0 / avg(reach_distance) as lrd
from
  reach_distance
group by
  target
), 
neighbours_lrd as (
select
  t1.target, 
  t2.lrd
from
  list_neighbours t1 JOIN
  lrd t2 on (t1.neighbour = t2.target)
)
select
  t1.target, 
  sum(t2.lrd / t1.lrd) / count(1) as lof
from
  lrd t1 JOIN
  neighbours_lrd t2 on (t1.target = t2.target)
group by
  t1.target
order by lof desc
limit ${topk};
```

```
> 87      3.031143749957831
> 16      1.9755564408378874
> 1       1.8415763570939774
```
