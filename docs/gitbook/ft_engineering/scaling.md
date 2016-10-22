# Min-Max Normalization
http://en.wikipedia.org/wiki/Feature_scaling#Rescaling
```sql
select min(target), max(target)
from (
select target from e2006tfidf_train 
-- union all
-- select target from e2006tfidf_test 
) t;
```

> -7.899578       -0.51940954

```sql
set hivevar:min_target=-7.899578;
set hivevar:max_target=-0.51940954;

create or replace view e2006tfidf_train_scaled 
as
select 
  rowid,
  rescale(target, ${min_target}, ${max_target}) as target, 
  features
from 
  e2006tfidf_train;
```

# Feature scaling by zscore
http://en.wikipedia.org/wiki/Standard_score

```sql
select avg(target), stddev_pop(target)
from (
select target from e2006tfidf_train 
-- union all
-- select target from e2006tfidf_test 
) t;
```
> -3.566241460963296      0.6278076335455348

```sql
set hivevar:mean_target=-3.566241460963296;
set hivevar:stddev_target=0.6278076335455348;

create or replace view e2006tfidf_train_scaled 
as
select 
  rowid,
  zscore(target, ${mean_target}, ${stddev_target}) as target, 
  features
from 
  e2006tfidf_train;
```

# Apply Normalization to more complex feature vector

Apply normalization to the following data.

```sql
select rowid, features from train limit 3;
```

```
1       ["weight:69.613","specific_heat:129.07","reflectance:52.111"]
2       ["weight:70.67","specific_heat:128.161","reflectance:52.446"]
3       ["weight:72.303","specific_heat:128.45","reflectance:52.853"]
```

We can create a normalized table as follows:

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
  -- concat(feature, ":", minmax) as feature  -- Before Hivemall v0.3.2-1
  feature(feature, minmax) as feature         -- Hivemall v0.3.2-1 or later
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
1       ["reflectance:0.5252967","specific_heat:0.19863537","weight:0.0"]
2       ["reflectance:0.5950446","specific_heat:0.09166764","weight:0.052084323"]
3       ["reflectance:0.6797837","specific_heat:0.12567581","weight:0.13255163"]
...
```

# Tips for using both min-max and zscore normalization

```sql
WITH quantative as (
  select id, true as minmax, "age" as feature, age as value from train
  union all
  select id, false as minmax, "balance" as feature, balance as value from train
  union all
  select id, true as minmax, "day" as feature, day as value from train
  union all
  select id, false as minmax, "duration" as feature, duration as value from train
  union all
  select id, false as minmax, "campaign" as feature, campaign as value from train
  union all
  select id, false as minmax, "pdays" as feature, if(pdays = -1, 0, pdays) as value from train
  union all
  select id, false as minmax,  "previous" as feature, previous as value from train  
),
quantative_stats as (
select
  feature,
  avg(value) as mean, stddev_pop(value) as stddev,
  min(value) as min, max(value) as max
from
  quantative
group by
  feature
), 
quantative_norm as (
select 
  t1.id,
  collect_list(
   feature(
      t1.feature, 
      if(t1.minmax,rescale(t1.value, t2.min, t2.max),zscore(t1.value, t2.mean, t2.stddev))
    )
  ) as features
from 
  quantative t1
  JOIN quantative_stats t2 ON (t1.feature = t2.feature)   
group by
  t1.id
)
...
```