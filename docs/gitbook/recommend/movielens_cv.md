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
        
[Cross-validation](http://en.wikipedia.org/wiki/Cross-validation_(statistics)#k-fold_cross-validationk-fold cross validation) is a model validation technique for assessing how a prediction model will generalize to an independent data set. This example shows a way to perform [k-fold cross validation](http://en.wikipedia.org/wiki/Cross-validation_(statistics)#k-fold_cross-validation) to evaluate prediction performance.

*Caution:* Matrix factorization is supported in Hivemall v0.3 or later.

# Data set creating for 10-folds cross validation.
```sql
use movielens;

set hivevar:kfold=10;
set hivevar:seed=31;

-- Adding group id (gid) to each training instance
drop table ratings_groupded;
create table ratings_groupded
as
select
  rand_gid2(${kfold}, ${seed}) gid, -- generates group id ranging from 1 to 10
  userid, 
  movieid, 
  rating
from
  ratings
cluster by gid, rand(${seed});
```

## Set training hyperparameters

```sql
-- latent factors
set hivevar:factor=10;
-- maximum number of iterations
set hivevar:iters=50;
-- regularization parameter
set hivevar:lambda=0.05;
-- learning rate
set hivevar:eta=0.005;
-- conversion rate (if changes between iterations became less or equals to ${cv_rate}, the training will stop)
set hivevar:cv_rate=0.001;
```
_Due to [a bug](https://issues.apache.org/jira/browse/HIVE-8396) in Hive, do not issue comments in CLI._

```sql
select avg(rating) from ratings;
```
> 3.581564453029317

```sql
-- mean rating value (Optional but recommended to set ${mu})
set hivevar:mu=3.581564453029317;
```
_Note that it is not necessary to set an exact value for ${mu}._

## SQL-generation for 10-folds cross validation

Run [generate_cv.sh](https://gist.github.com/myui/c2009e5791cca650a4d0) and create [generate_cv.sql](https://gist.github.com/myui/2e2018217e2188222655).

Then, issue SQL queies in [generate_cv.sql](https://gist.github.com/myui/2e2018217e2188222655) to get MAE/RMSE.

> 0.6695442192077673 (MAE)

> 0.8502739040257945 (RMSE)

_We recommend to use [Tez](http://tez.apache.org/) for running queries having many stages._