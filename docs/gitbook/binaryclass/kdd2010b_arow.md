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
        
## training
```sql
-- SET mapred.reduce.tasks=32;
drop table kdd10b_arow_model1;
create table kdd10b_arow_model1 as
select 
 feature,
 -- voted_avg(weight) as weight
 argmin_kld(weight, covar) as weight -- [hivemall v0.2alpha3 or later]
from 
 (select 
     -- train_arow(addBias(features),label) as (feature,weight) -- [hivemall v0.1]
     train_arow(addBias(features),label) as (feature,weight,covar) -- [hivemall v0.2 or later]
  from 
     kdd10b_train_x3
 ) t 
group by feature;
```

## prediction
```sql
create or replace view kdd10b_arow_predict1 
as
select
  t.rowid, 
  sum(m.weight * t.value) as total_weight,
  case when sum(m.weight * t.value) > 0.0 then 1 else -1 end as label
from 
  kdd10b_test_exploded t LEFT OUTER JOIN
  kdd10b_arow_model1 m ON (t.feature = m.feature)
group by
  t.rowid;
```

## evaluation
```sql
create or replace view kdd10b_arow_submit1 as
select 
  t.rowid, 
  t.label as actual, 
  pd.label as predicted
from 
  kdd10b_test t JOIN kdd10b_arow_predict1 pd 
    on (t.rowid = pd.rowid);
```

```sql
select count(1)/748401 from kdd10b_arow_submit1 
where actual = predicted;
```
> 0.8565808971393678