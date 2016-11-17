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

<!-- toc -->

# UDF preparation

```sql
select count(1) from a9atrain;
-- set total_steps ideally be "count(1) / #map tasks"
set hivevar:total_steps=32561;

select count(1) from a9atest;
set hivevar:num_test_instances=16281;
```

# training
```sql
create table a9a_model1 
as
select 
 cast(feature as int) as feature,
 avg(weight) as weight
from 
 (select 
     logress(addBias(features),label,"-total_steps ${total_steps}") as (feature,weight)
  from 
     a9atrain
 ) t 
group by feature;
```
_"-total_steps" option is optional for logress() function._  
_I recommend you NOT to use options (e.g., total_steps and eta0) if you are not familiar with those options. Hivemall then uses an autonomic ETA (learning rate) estimator._

# prediction
```sql
create or replace view a9a_predict1 
as
WITH a9atest_exploded as (
select 
  rowid,
  label,
  extract_feature(feature) as feature,
  extract_weight(feature) as value
from 
  a9atest LATERAL VIEW explode(addBias(features)) t AS feature
)
select
  t.rowid, 
  sigmoid(sum(m.weight * t.value)) as prob,
  CAST((case when sigmoid(sum(m.weight * t.value)) >= 0.5 then 1.0 else 0.0 end) as FLOAT) as label
from 
  a9atest_exploded t LEFT OUTER JOIN
  a9a_model1 m ON (t.feature = m.feature)
group by
  t.rowid;
```

# evaluation
```sql
create or replace view a9a_submit1 as
select 
  t.label as actual, 
  pd.label as predicted, 
  pd.prob as probability
from 
  a9atest t JOIN a9a_predict1 pd 
    on (t.rowid = pd.rowid);
```

```sql
select count(1) / ${num_test_instances} from a9a_submit1 
where actual == predicted;
```
> 0.8430071862907684
