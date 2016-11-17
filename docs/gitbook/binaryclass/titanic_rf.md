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

This examples gives a basic usage of RandomForest on Hivemall using [Kaggle Titanic](https://www.kaggle.com/c/titanic) dataset.
The example gives a baseline score without any feature engineering.

<!-- toc -->

# Data preparation

```sql
create database titanic;
use titanic;

drop table train;
create external table train (
  passengerid int, -- unique id
  survived int, -- target label
  pclass int,
  name string,
  sex string,
  age int,
  sibsp int, -- Number of Siblings/Spouses Aboard
  parch int, -- Number of Parents/Children Aboard
  ticket string,
  fare double,
  cabin string,
  embarked string
) 
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY '|'
   LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '/dataset/titanic/train';

hadoop fs -rm /dataset/titanic/train/train.csv
awk '{ FPAT="([^,]*)|(\"[^\"]+\")";OFS="|"; } NR >1 {$1=$1;$4=substr($4,2,length($4)-2);print $0}' train.csv | hadoop fs -put - /dataset/titanic/train/train.csv

drop table test_raw;
create external table test_raw (
  passengerid int,
  pclass int,
  name string,
  sex string,
  age int,
  sibsp int, -- Number of Siblings/Spouses Aboard
  parch int, -- Number of Parents/Children Aboard
  ticket string,
  fare double,
  cabin string,
  embarked string
)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY '|'
   LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '/dataset/titanic/test_raw';

hadoop fs -rm /dataset/titanic/test_raw/test.csv
awk '{ FPAT="([^,]*)|(\"[^\"]+\")";OFS="|"; } NR >1 {$1=$1;$3=substr($3,2,length($3)-2);print $0}' test.csv | hadoop fs -put - /dataset/titanic/test_raw/test.csv
```

## Data preparation for RandomForest

```sql
set hivevar:output_row=true;

drop table train_rf;
create table train_rf
as
WITH train_quantified as (
  select    
    quantify(
      ${output_row}, passengerid, survived, pclass, name, sex, age, sibsp, parch, ticket, fare, cabin, embarked
    ) as (passengerid, survived, pclass, name, sex, age, sibsp, parch, ticket, fare, cabin, embarked)
  from (
    select * from train
    order by passengerid asc
  ) t
)
select
  rand(31) as rnd,
  passengerid, 
  array(pclass, name, sex, age, sibsp, parch, ticket, fare, cabin, embarked) as features,
  survived
from
  train_quantified
;

drop table test_rf;
create table test_rf
as
WITH test_quantified as (
  select 
    quantify(
      output_row, passengerid, pclass, name, sex, age, sibsp, parch, ticket, fare, cabin, embarked
    ) as (passengerid, pclass, name, sex, age, sibsp, parch, ticket, fare, cabin, embarked)
  from (
    -- need training data to assign consistent ids to categorical variables
    select * from (
      select
        1 as train_first, false as output_row, passengerid, pclass, name, sex, age, sibsp, parch, ticket, fare, cabin, embarked
      from
        train
      union all
      select
        2 as train_first, true as output_row, passengerid, pclass, name, sex, age, sibsp, parch, ticket, fare, cabin, embarked
      from
        test_raw
    ) t0
    order by train_first asc, passengerid asc
  ) t1
)
select
  passengerid, 
  array(pclass, name, sex, age, sibsp, parch, ticket, fare, cabin, embarked) as features
from
  test_quantified
;
```

---

# Training

`select guess_attribute_types(pclass, name, sex, age, sibsp, parch, ticket, fare, cabin, embarked) from train limit 1;`
> Q,C,C,Q,Q,Q,C,Q,C,C

`Q` and `C` represent quantitative variable and categorical variables, respectively.

*Caution:* Note that the output of `guess_attribute_types` is not perfect. Revise it by your self.
For example, `pclass` is a categorical variable.

```sql
set hivevar:attrs=C,C,C,Q,Q,Q,C,Q,C,C;

drop table model_rf;
create table model_rf
AS
select
  train_randomforest_classifier(features, survived, "-trees 500 -attrs ${attrs}") 
    -- as (model_id, model_type, pred_model, var_importance, oob_errors, oob_tests)
from
  train_rf
;

select
  array_sum(var_importance) as var_importance,
  sum(oob_errors) / sum(oob_tests) as oob_err_rate
from
  model_rf;

> [137.00242639169272,1194.2140119834373,328.78017188176966,628.2568660509628,200.31275032394072,160.12876797647078,1083.5987543408116,664.1234312561456,422.89449844090393,130.72019667694784]     0.18742985409652077
```

# Prediction

```sql
SET hivevar:classification=true;
set hive.auto.convert.join=true;
SET hive.mapjoin.optimized.hashtable=false;
SET mapred.reduce.tasks=16;

drop table predicted_rf;
create table predicted_rf
as
SELECT 
  passengerid,
  predicted.label,
  predicted.probability,
  predicted.probabilities
FROM (
  SELECT
    passengerid,
    rf_ensemble(predicted) as predicted
  FROM (
    SELECT
      t.passengerid, 
      -- hivemall v0.4.1-alpha.2 or before
      -- tree_predict(p.model, t.features, ${classification}) as predicted
　　   -- hivemall v0.4.1-alpha.3 or later
      tree_predict(p.model_id, p.model_type, p.pred_model, t.features, ${classification}) as predicted
    FROM (
      SELECT model_id, model_type, pred_model FROM model_rf 
      DISTRIBUTE BY rand(1)
    ) p
    LEFT OUTER JOIN test_rf t
  ) t1
  group by
    passengerid
) t2
;
```

# Kaggle submission

```sql
drop table predicted_rf_submit;
create table predicted_rf_submit
  ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY ","
    LINES TERMINATED BY "\n"
  STORED AS TEXTFILE
as
SELECT passengerid, label as survived
FROM predicted_rf
ORDER BY passengerid ASC;
```

```sh
hadoop fs -getmerge /user/hive/warehouse/titanic.db/predicted_rf_submit predicted_rf_submit.csv

sed -i -e "1i PassengerId,Survived" predicted_rf_submit.csv
```

Accuracy would gives `0.76555` for a Kaggle submission.

---

# Test by dividing training dataset

```sql
drop table train_rf_07;
create table train_rf_07 
as
select * from train_rf 
where rnd < 0.7;

drop table test_rf_03;
create table test_rf_03
as
select * from train_rf
where rnd >= 0.7;

drop table model_rf_07;
create table model_rf_07
AS
select
  train_randomforest_classifier(features, survived, "-trees 500 -attrs ${attrs}") 
from
  train_rf_07;

select
  array_sum(var_importance) as var_importance,
  sum(oob_errors) / sum(oob_tests) as oob_err_rate
from
  model_rf_07;
> [116.12055542977338,960.8569891444097,291.08765260103837,469.74671636586226,163.721292772701,120.784769882858,847.9769298113661,554.4617571355476,346.3500941757221,97.42593940113392]    0.1838351822503962

SET hivevar:classification=true;
SET hive.mapjoin.optimized.hashtable=false;
SET mapred.reduce.tasks=16;

drop table predicted_rf_03;
create table predicted_rf_03
as
SELECT 
  passengerid,
  predicted.label,
  predicted.probability,
  predicted.probabilities
FROM (
  SELECT
    passengerid,
    rf_ensemble(predicted) as predicted
  FROM (
    SELECT
      t.passengerid, 
      -- hivemall v0.4.1-alpha.2 or before
      -- tree_predict(p.model, t.features, ${classification}) as predicted
      -- hivemall v0.4.1-alpha.3 or later
      tree_predict(p.model_id, p.model_type, p.pred_model, t.features, ${classification}) as predicted
    FROM (
      SELECT model_id, model_type, pred_model FROM model_rf_07
      DISTRIBUTE BY rand(1)
    ) p
    LEFT OUTER JOIN test_rf_03 t
  ) t1
  group by
    passengerid
) t2
;

create or replace view rf_submit_03 as
select 
  t.survived as actual, 
  p.label as predicted,
  p.probabilities
from 
  test_rf_03 t 
  JOIN predicted_rf_03 p on (t.passengerid = p.passengerid)
;

select count(1) from test_rf_03;
> 260

set hivevar:testcnt=260;

select count(1)/${testcnt} as accuracy 
from rf_submit_03 
where actual = predicted;

> 0.8
```
