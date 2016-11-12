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
        
*NOTE: RandomForest is being supported from Hivemall v0.4 or later.*

# Dataset

* https://archive.ics.uci.edu/ml/datasets/Iris

```
Attribute Information:
   1. sepal length in cm
   2. sepal width in cm
   3. petal length in cm
   4. petal width in cm
   5. class: 
      -- Iris Setosa
      -- Iris Versicolour
      -- Iris Virginica
```

# Table preparation

```sql
create database iris;
use iris;

create external table raw (
  sepal_length int,
  sepal_width int,
  petal_length int,
  petak_width int,
  class string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '/dataset/iris/raw';

$ sed '/^$/d' iris.data | hadoop fs -put - /dataset/iris/raw/iris.data
```

```sql
create table label_mapping 
as
select
  class,
  rank - 1 as label
from (
select
  distinct class,
  dense_rank() over (order by class) as rank
from 
  raw
) t
;
```

```sql
create table training
as
select
  rowid() as rowid,
  array(t1.sepal_length, t1.sepal_width, t1.petal_length, t1.petak_width) as features,
  t2.label
from
  raw t1
  JOIN label_mapping t2 ON (t1.class = t2.class)
;
```

# Training

`train_randomforest_classifier` takes a dense `features` in double[] and a `label` starting from 0.

```sql
CREATE TABLE model 
STORED AS SEQUENCEFILE 
AS
select 
  train_randomforest_classifier(features, label) 
  -- hivemall v0.4.1-alpha.2 and before
  -- train_randomforest_classifier(features, label) as (pred_model, var_importance, oob_errors, oob_tests)
  -- hivemall v0.4.1 and later
  -- train_randomforest_classifier(features, label) as (model_id, model_type, pred_model, var_importance, oob_errors, oob_tests)
from
  training;
```
*Note: The default TEXTFILE should not be used for model table when using Javascript output through "-output javascript" option.*

```
hive> desc model;
model_id                int                                         
model_type              int                                         
pred_model              string                                      
var_importance          array<double>                               
oob_errors              int                                         
oob_tests               int  
```

## Training options

"-help" option shows usage of the function.

```
select train_randomforest_classifier(features, label, "-help") from training;

> FAILED: UDFArgumentException 
usage: train_randomforest_classifier(double[] features, int label [,
       string options]) - Returns a relation consists of <int model_id,
       int model_type, string pred_model, array<double> var_importance,
       int oob_errors, int oob_tests> [-attrs <arg>] [-depth <arg>]
       [-disable_compression] [-help] [-leafs <arg>] [-output <arg>]
       [-rule <arg>] [-seed <arg>] [-splits <arg>] [-trees <arg>] [-vars
       <arg>]
 -attrs,--attribute_types <arg>   Comma separated attribute types (Q for
                                  quantitative variable and C for
                                  categorical variable. e.g., [Q,C,Q,C])
 -depth,--max_depth <arg>         The maximum number of the tree depth
                                  [default: Integer.MAX_VALUE]
 -disable_compression             Whether to disable compression of the
                                  output script [default: false]
 -help                            Show function help
 -leafs,--max_leaf_nodes <arg>    The maximum number of leaf nodes
                                  [default: Integer.MAX_VALUE]
 -output,--output_type <arg>      The output type (serialization/ser or
                                  opscode/vm or javascript/js) [default:
                                  serialization]
 -rule,--split_rule <arg>         Split algorithm [default: GINI, ENTROPY]
 -seed <arg>                      seed value in long [default: -1
                                  (random)]
 -splits,--min_split <arg>        A node that has greater than or equals
                                  to `min_split` examples will split
                                  [default: 2]
 -trees,--num_trees <arg>         The number of trees for each task
                                  [default: 50]
 -vars,--num_variables <arg>      The number of random selected features
                                  [default: ceil(sqrt(x[0].length))].
                                  int(num_variables * x[0].length) is
                                  considered if num_variable is (0,1]
```
*Caution: "-num_trees" controls the number of trees for each task, not the total number of trees.*

### Parallelize Training

To parallelize RandomForest training, you can use UNION ALL as follows:

```sql
CREATE TABLE model 
STORED AS SEQUENCEFILE 
AS
select 
  train_randomforest_classifier(features, label, '-trees 25') 
from
  training
UNION ALL
select 
  train_randomforest_classifier(features, label, '-trees 25')
from
  training
;
```

### Learning stats

[`Variable importance`](https://www.stat.berkeley.edu/~breiman/RandomForests/cc_home.htm#varimp) and [`Out Of Bag (OOB) error rate`](https://www.stat.berkeley.edu/~breiman/RandomForests/cc_home.htm#ooberr) of RandomForest can be shown as follows:

```sql
select
  array_sum(var_importance) as var_importance,
  sum(oob_errors) / sum(oob_tests) as oob_err_rate
from
  model;
```
> [2.81010338879605,0.4970357753626371,23.790369091407698,14.315316390235273]     0.05333333333333334

### Output prediction model by Javascipt

```sql
CREATE TABLE model_javascript
STORED AS SEQUENCEFILE 
AS
select train_randomforest_classifier(features, label, "-output_type js -disable_compression")
from training;

select model from model_javascript limit 1;
```

```js
if(x[3] <= 0.5) {
  0;
} else  {
  if(x[2] <= 4.5) {
    if(x[3] <= 1.5) {
      if(x[0] <= 4.5) {
        1;
      } else  {
        if(x[0] <= 5.5) {
          1;
        } else  {
          if(x[1] <= 2.5) {
            1;
          } else  {
            1;
          }
        }
      }
    } else  {
      2;
    }
  } else  {
    if(x[3] <= 1.5) {
      2;
    } else  {
      2;
    }
  }
}
```

# Prediction

```sql
set hivevar:classification=true;
set hive.auto.convert.join=true;
set hive.mapjoin.optimized.hashtable=false;

create table predicted_vm
as
SELECT
  rowid,
  rf_ensemble(predicted) as predicted
FROM (
  SELECT
    rowid, 
    -- hivemall v0.4.1-alpha.2 and before
    -- tree_predict(p.model, t.features, ${classification}) as predicted
    -- hivemall v0.4.1 and later
    tree_predict(p.model_id, p.model_type, p.pred_model, t.features, ${classification}) as predicted
  FROM
    model p
    LEFT OUTER JOIN -- CROSS JOIN
    training t
) t1
group by
  rowid
;
```
_Note: Javascript outputs can be evaluated by `js_tree_predict`._

### Parallelize Prediction

The following query runs predictions in N-parallel. It would reduce elapsed time for prediction almost by N.

```sql
SET hivevar:classification=true;
set hive.auto.convert.join=true;
SET hive.mapjoin.optimized.hashtable=false;
SET mapred.reduce.tasks=8;

create table predicted_vm
as
SELECT
  rowid,
  rf_ensemble(predicted) as predicted
FROM (
  SELECT
    t.rowid, 
    -- hivemall v0.4.1-alpha.2 and before
    -- tree_predict(p.pred_model, t.features, ${classification}) as predicted
    -- hivemall v0.4.1 and later
    tree_predict(p.model_id, p.model_type, p.pred_model, t.features, ${classification}) as predicted
  FROM (
    SELECT model_id, model_type, pred_model
    FROM model
    DISTRIBUTE BY rand(1)
  ) p 
  LEFT OUTER JOIN training t
) t1
group by
  rowid
;
```

# Evaluation

```sql
select count(1) from training;
> 150

set hivevar:total_cnt=150;

WITH t1 as (
SELECT
  t.rowid,
  t.label as actual,
  p.predicted.label as predicted
FROM
  predicted_vm p
  LEFT OUTER JOIN training t ON (t.rowid = p.rowid)
)
SELECT
  count(1) / ${total_cnt}
FROM
  t1
WHERE
  actual = predicted
;
```
> 0.9533333333333334