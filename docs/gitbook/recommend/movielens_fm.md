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
        
_Caution: Factorization Machine is supported from Hivemall v0.4 or later._

# Data preparation

First of all, please create `ratings` table described in [this article](../recommend/movielens_dataset.html).

```sql
use movielens;

SET hivevar:seed=31;

DROP TABLE ratings_fm;
CREATE TABLE ratings_fm
as
select
	rowid() as rowid,
	categorical_features(array("userid","movieid"), userid, movieid) 
	  as features,
	rating,
	rand(${seed}) as rnd
from
	ratings
CLUSTER BY rand(43); -- shuffle training input

select * from ratings_fm limit 2;
```

| rowid | features | rating | rnd |
|:-----:|:--------:|:------:|:---:|
| 1-383970 | ["userid#2244","movieid#1272"] | 5 | 0.33947035987020546 |
| 1-557913 | ["userid#3425","movieid#2791"] | 4 | 0.12344886396954391 |

```sql
-- use 80% for training
DROP TABLE training_fm;
CREATE TABLE training_fm
as
select * from ratings_fm
order by rnd DESC
limit 800000;

-- use 20% for testing
DROP TABLE testing_fm;
CREATE TABLE testing_fm
as
select * from ratings_fm
order by rnd ASC
limit 200209;

-- testing table for prediction
CREATE OR REPLACE VIEW testing_fm_exploded
as 
select 
  rowid,
  extract_feature(fv) as feature,
  extract_weight(fv) as Xi,
  rating
from
  testing_fm t1 LATERAL VIEW explode(add_bias(features)) t2 as fv;
```
_Caution: Don't forget to call `add_bias` in the above query. No need to call `add_bias` for preparing training data in Factorization Machines because it always considers it._

# Training

## Hyperparamters for Training
```sql
-- number of factors
set hivevar:factor=10;
-- maximum number of training iterations
set hivevar:iters=50;
```

## Build a prediction mdoel by Factorization Machine

```sql
drop table fm_model;
create table fm_model
as
select
  feature,
  avg(Wi) as Wi,
  array_avg(Vif) as Vif
from (
  select 
    train_fm(features, rating, "-factor ${factor} -iters ${iters} -eta 0.01") 
    	as (feature, Wi, Vif)
  from 
    training_fm
) t
group by feature;
```

_Note: setting eta option is optional. However, setting `-eta 0.01` usually works well._

## Usage of `train_fm`

You can get usages of `train_fm` by giving `-help` option as follows:
```sql
select 
  train_fm(features, rating, "-help") as (feature, Wi, Vif)
from 
  training_fm
```

```
usage: train_fm(array<string> x, double y [, const string options]) -
       Returns a prediction value [-adareg] [-c] [-cv_rate <arg>]
       [-disable_cv] [-eta <arg>] [-eta0 <arg>] [-f <arg>] [-help]
       [-init_v <arg>] [-int_feature] [-iters <arg>] [-lambda <arg>] [-max
       <arg>] [-maxval <arg>] [-min <arg>] [-min_init_stddev <arg>] [-p
       <arg>] [-power_t <arg>] [-seed <arg>] [-sigma <arg>] [-t <arg>]
       [-va_ratio <arg>] [-va_threshold <arg>]
 -adareg,--adaptive_regularizaion             Whether to enable adaptive
                                              regularization [default:
                                              OFF]
 -c,--classification                          Act as classification
 -cv_rate,--convergence_rate <arg>            Threshold to determine
                                              convergence [default: 0.005]
 -disable_cv,--disable_cvtest                 Whether to disable
                                              convergence check [default:
                                              OFF]
 -eta <arg>                                   The initial learning rate
 -eta0 <arg>                                  The initial learning rate
                                              [default 0.1]
 -f,--factor <arg>                            The number of the latent
                                              variables [default: 10]
 -help                                        Show function help
 -init_v <arg>                                Initialization strategy of
                                              matrix V [random, gaussian]
                                              (default: random)
 -int_feature,--feature_as_integer            Parse a feature as integer
                                              [default: OFF, ON if -p
                                              option is specified]
 -iters,--iterations <arg>                    The number of iterations
                                              [default: 1]
 -lambda,--lambda0 <arg>                      The initial lambda value for
                                              regularization [default:
                                              0.01]
 -max,--max_target <arg>                      The maximum value of target
                                              variable
 -maxval,--max_init_value <arg>               The maximum initial value in
                                              the matrix V [default: 1.0]
 -min,--min_target <arg>                      The minimum value of target
                                              variable
 -min_init_stddev <arg>                       The minimum standard
                                              deviation of initial matrix
                                              V [default: 0.1]
 -p,--size_x <arg>                            The size of x
 -power_t <arg>                               The exponent for inverse
                                              scaling learning rate
                                              [default 0.1]
 -seed <arg>                                  Seed value [default: -1
                                              (random)]
 -sigma <arg>                                 The standard deviation for
                                              initializing V [default:
                                              0.1]
 -t,--total_steps <arg>                       The total number of training
                                              examples
 -va_ratio,--validation_ratio <arg>           Ratio of training data used
                                              for validation [default:
                                              0.05f]
 -va_threshold,--validation_threshold <arg>   Threshold to start
                                              validation. At least N
                                              training examples are used
                                              before validation [default:
                                              1000]
```

# Prediction

```sql
-- workaround for a bug 
-- https://issues.apache.org/jira/browse/HIVE-11051
set hive.mapjoin.optimized.hashtable=false;

drop table fm_predict;
create table fm_predict
as
select
  t1.rowid,
  fm_predict(p1.Wi, p1.Vif, t1.Xi) as predicted
from 
  testing_fm_exploded t1
  LEFT OUTER JOIN fm_model p1 ON (t1.feature = p1.feature)
group by
  t1.rowid;
```

# Evaluation

```sql
select
  mae(p.predicted, rating) as mae,
  rmse(p.predicted, rating) as rmse
from
  testing_fm as t
  JOIN fm_predict as p on (t.rowid = p.rowid);
```

> 0.6736798239047873 (mae)     0.858938110314545 (rmse)

# Fast Factorization Machines Training using Int Features

Training of Factorization Machines (FM) can be done more efficietly, in term of speed, by using INT features.
In this section, we show how to run FM training by using int features, more specifically by using [feature hashing](../ft_engineering/hashing.html).

```sql
set hivevar:factor=10;
set hivevar:iters=50;

drop table fm_model;
create table fm_model
as
select
  feature,
  avg(Wi) as Wi,
  array_avg(Vif) as Vif
from (
  select 
    train_fm(feature_hashing(features), rating, "-factor ${factor} -iters ${iters} -eta 0.01 -int_feature")  -- internally use a sparse map
 -- train_fm(feature_hashing(features), rating, "-factor ${factor} -iters ${iters} -eta 0.01 -int_feature -num_features 16777216") -- internally use a dense array 
        as (feature, Wi, Vif)
  from 
    training_fm
) t
group by feature;
```

```sql
set hive.mapjoin.optimized.hashtable=false; -- workaround for https://issues.apache.org/jira/browse/HIVE-11051

WITH predicted as (
  select
    t1.rowid,
    fm_predict(p1.Wi, p1.Vif, t1.Xi) as predicted
  from 
    testing_fm_exploded t1
    LEFT OUTER JOIN fm_model p1 ON (feature_hashing(t1.feature) = p1.feature)
  group by
    t1.rowid
)
select
  mae(p.predicted, rating) as mae,
  rmse(p.predicted, rating) as rmse
from
  testing_fm as t
  JOIN predicted as p on (t.rowid = p.rowid);
```