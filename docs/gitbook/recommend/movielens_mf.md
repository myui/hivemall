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
        
This page explains how to run matrix factorization on [MovieLens 1M dataset](../recommend/movielens_dataset.html).

<!-- toc -->

## Calculate the mean rating in the training dataset
```sql
use movielens;

select avg(rating) from training;
```
> 3.593565

## Set variables (hyperparameters) for training
```sql
-- mean rating
set hivevar:mu=3.593565;
-- number of factors
set hivevar:factor=10;
-- maximum number of training iterations
set hivevar:iters=50;
```

Note that there are no need to set an exact value for `$mu`. It actually works without setting `$mu` but recommended to set one for getting a better prediction.

_Due to [a bug](https://issues.apache.org/jira/browse/HIVE-8396) in Hive, do not issue comments in CLI._

## Training
```sql
create table sgd_model
as
select
  idx, 
  array_avg(u_rank) as Pu, 
  array_avg(m_rank) as Qi, 
  avg(u_bias) as Bu, 
  avg(m_bias) as Bi
from (
  select 
    train_mf_sgd(userid, movieid, rating, '-factor ${factor} -mu ${mu} -iter ${iters}') as (idx, u_rank, m_rank, u_bias, m_bias)
  from 
    training
) t
group by idx;
```

> #### Note
>
> Hivemall also provides *train_mf_adagrad* for training using AdaGrad. 
> `-help` option shows a complete list of hyperparameters.

# Predict

```sql
select
  t2.actual,
  mf_predict(t2.Pu, p2.Qi, t2.Bu, p2.Bi, ${mu}) as predicted
from (
  select
    t1.userid, 
    t1.movieid,
    t1.rating as actual,
    p1.Pu,
    p1.Bu
  from
    testing t1 LEFT OUTER JOIN sgd_model p1
    ON (t1.userid = p1.idx) 
) t2 
LEFT OUTER JOIN sgd_model p2
ON (t2.movieid = p2.idx);
```

# Evaluate (computes MAE and RMSE)
```sql
select
  mae(predicted, actual) as mae,
  rmse(predicted, actual) as rmse
from (
  select
    t2.actual,
    mf_predict(t2.Pu, p2.Qi, t2.Bu, p2.Bi, ${mu}) as predicted
  from (
    select
      t1.userid, 
      t1.movieid,
      t1.rating as actual,
      p1.Pu,
      p1.Bu
    from
      testing t1 LEFT OUTER JOIN sgd_model p1
      ON (t1.userid = p1.idx) 
  ) t2 
  LEFT OUTER JOIN sgd_model p2
  ON (t2.movieid = p2.idx)
) t;
```

| MAE | RMSE |
|:---:|:----:|
| 0.6728969407733578 | 0.8584162122694449 |

# Item Recommendation

Recommend top-k movies that a user have not ever seen.
```sql
set hivevar:userid=1;
set hivevar:topk=5;

select
  t1.movieid, 
  mf_predict(t2.Pu, t1.Qi, t2.Bu, t1.Bi, ${mu}) as predicted
from (
  select
    idx movieid,
    Qi, 
    Bi
  from
    sgd_model p
  where
    p.idx NOT IN 
      (select movieid from training where userid=${userid})
) t1 CROSS JOIN (
  select
    Pu,
    Bu
  from 
    sgd_model
  where
    idx = ${userid}
) t2
order by
  predicted DESC
limit ${topk};
```

| movieid | predicted |
|--------:|----------:|
| 318     | 4.8051853 |
| 2503    | 4.788541  |
| 53      | 4.7518783 |
| 904     | 4.7463417 |
| 953     | 4.732769  |