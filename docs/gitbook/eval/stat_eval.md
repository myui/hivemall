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
        
Using the [E2006 tfidf regression example](../regression/e2006_arow.html), we explain how to evaluate the prediction model on Hive.

<!-- toc -->

# Scoring by evaluation metrics

```sql
select avg(actual), avg(predicted) from e2006tfidf_pa2a_submit;
```
> -3.8200363760415414     -3.9124877451612488

```sql
set hivevar:mean_actual=-3.8200363760415414;

select 
-- Root Mean Squared Error
   rmse(predicted, actual) as RMSE, 
   -- sqrt(sum(pow(predicted - actual,2.0))/count(1)) as RMSE,
-- Mean Squared Error
   mse(predicted, actual) as MSE, 
   -- sum(pow(predicted - actual,2.0))/count(1) as MSE,
-- Mean Absolute Error
   mae(predicted, actual) as MAE, 
   -- sum(abs(predicted - actual))/count(1) as MAE,
-- coefficient of determination (R^2)
   -- 1 - sum(pow(actual - predicted,2.0)) / sum(pow(actual - ${mean_actual},2.0)) as R2
   r2(actual, predicted) as R2 -- supported since Hivemall v0.4.1-alpha.5
from 
   e2006tfidf_pa2a_submit;
```
> 0.38538660838804495     0.14852283792484033     0.2466732002711477      0.48623913673053565

# Logarithmic Loss

[Logarithmic Loss](https://www.kaggle.com/wiki/LogarithmicLoss) can be computed as follows:

```sql
WITH t as ( 
  select 
    0 as actual,
    0.01 as predicted
  union all
  select 
    1 as actual,
    0.02 as predicted
)
select 
   -SUM(actual*LN(predicted)+(1-actual)*LN(1-predicted))/count(1) as logloss1,
  logloss(predicted, actual) as logloss2 -- supported since Hivemall v0.4.2-rc.1
from 
from t;
```
> 1.9610366706408238	1.9610366706408238

# References

* R2 http://en.wikipedia.org/wiki/Coefficient_of_determination
* Evaluation Metrics https://www.kaggle.com/wiki/Metrics
