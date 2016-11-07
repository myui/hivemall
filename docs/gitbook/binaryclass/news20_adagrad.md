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
        
_Note that this feature is supported since Hivemall v0.3-beta2 or later._

## UDF preparation
```
add jar ./tmp/hivemall-with-dependencies.jar;
source ./tmp/define-all.hive;

use news20;
```

#[AdaGradRDA]

_Note that the current AdaGradRDA implmenetation can only be applied to classification, not to regression, because it uses hinge loss for the loss function._


## model building
```sql
drop table news20b_adagrad_rda_model1;
create table news20b_adagrad_rda_model1 as
select 
 feature,
 voted_avg(weight) as weight
from 
 (select 
     train_adagrad_rda(addBias(features),label) as (feature,weight)
  from 
     news20b_train_x3
 ) t 
group by feature;
```

## prediction
```sql
create or replace view news20b_adagrad_rda_predict1 
as
select
  t.rowid, 
  sum(m.weight * t.value) as total_weight,
  case when sum(m.weight * t.value) > 0.0 then 1 else -1 end as label
from 
  news20b_test_exploded t LEFT OUTER JOIN
  news20b_adagrad_rda_model1 m ON (t.feature = m.feature)
group by
  t.rowid;
```

## evaluation
```sql
create or replace view news20b_adagrad_rda_submit1 as
select 
  t.label as actual, 
  pd.label as predicted
from 
  news20b_test t JOIN news20b_adagrad_rda_predict1 pd 
    on (t.rowid = pd.rowid);
```

```sql
select count(1)/4996 from news20b_adagrad_rda_submit1 
where actual == predicted;
```
> SCW1 0.9661729383506805 

> ADAGRAD+RDA 0.9677742193755005

#[AdaGrad]

_Note that AdaGrad is better suited for a regression problem because the current implementation only support logistic loss._

## model building
```sql
drop table news20b_adagrad_model1;
create table news20b_adagrad_model1 as
select 
 feature,
 voted_avg(weight) as weight
from 
 (select 
     adagrad(addBias(features),convert_label(label)) as (feature,weight)
  from 
     news20b_train_x3
 ) t 
group by feature;
```
_adagrad takes 0/1 for a label value and convert_label(label) converts a label value from -1/+1 to 0/1._
## prediction
```sql
create or replace view news20b_adagrad_predict1 
as
select
  t.rowid, 
  case when sigmoid(sum(m.weight * t.value)) >= 0.5 then 1 else -1 end as label
from 
  news20b_test_exploded t LEFT OUTER JOIN
  news20b_adagrad_model1 m ON (t.feature = m.feature)
group by
  t.rowid;
```

## evaluation
```sql
create or replace view news20b_adagrad_submit1 as
select 
  t.label as actual, 
  p.label as predicted
from 
  news20b_test t JOIN news20b_adagrad_predict1 p
    on (t.rowid = p.rowid);
```

```sql
select count(1)/4996 from news20b_adagrad_submit1 
where actual == predicted;
```
> 0.9549639711769415 (adagrad)

#[AdaDelta]

_Note that AdaDelta is better suited for regression problem because the current implementation only support logistic loss._

## model building
```sql
drop table news20b_adadelta_model1;
create table news20b_adadelta_model1 as
select 
 feature,
 voted_avg(weight) as weight
from 
 (select 
     adadelta(addBias(features),convert_label(label)) as (feature,weight)
  from 
     news20b_train_x3
 ) t 
group by feature;
```

## prediction
```sql
create or replace view news20b_adadelta_predict1 
as
select
  t.rowid, 
  case when sigmoid(sum(m.weight * t.value)) >= 0.5 then 1 else -1 end as label
from 
  news20b_test_exploded t LEFT OUTER JOIN
  news20b_adadelta_model1 m ON (t.feature = m.feature)
group by
  t.rowid;
```

## evaluation
```sql
create or replace view news20b_adadelta_submit1 as
select 
  t.label as actual, 
  p.label as predicted
from 
  news20b_test t JOIN news20b_adadelta_predict1 p
    on (t.rowid = p.rowid);
```

```sql
select count(1)/4996 from news20b_adadelta_submit1 
where actual == predicted;
```
> 0.9549639711769415 (adagrad)

> 0.9545636509207366 (adadelta)

_Note that AdaDelta often performs better than AdaGrad._