_Note adagrad/adadelta is supported from hivemall v0.3b2 or later (or in the master branch)._

# Preparation 
```sql
add jar ./tmp/hivemall-with-dependencies.jar;
source ./tmp/define-all.hive;

use kdd12track2;

-- SET mapreduce.framework.name=yarn;
-- SET hive.execution.engine=mr;
-- SET mapreduce.framework.name=yarn-tez;
-- SET hive.execution.engine=tez;
SET mapred.reduce.tasks=32; -- [optional] set the explicit number of reducers to make group-by aggregation faster
```

# AdaGrad
```sql
drop table adagrad_model;
create table adagrad_model 
as
select 
 feature,
 avg(weight) as weight
from 
 (select 
     adagrad(features,label) as (feature,weight)
  from 
     training_orcfile
 ) t 
group by feature;

drop table adagrad_predict;
create table adagrad_predict
  ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY "\t"
    LINES TERMINATED BY "\n"
  STORED AS TEXTFILE
as
select
  t.rowid, 
  sigmoid(sum(m.weight)) as prob
from 
  testing_exploded  t LEFT OUTER JOIN
  adagrad_model m ON (t.feature = m.feature)
group by 
  t.rowid
order by 
  rowid ASC;
```

```sh
hadoop fs -getmerge /user/hive/warehouse/kdd12track2.db/adagrad_predict adagrad_predict.tbl

gawk -F "\t" '{print $2;}' adagrad_predict.tbl > adagrad_predict.submit

pypy scoreKDD.py KDD_Track2_solution.csv adagrad_predict.submit
```
>AUC(SGD) : 0.739351

>AUC(ADAGRAD) : 0.743279

# AdaDelta
```sql
drop table adadelta_model;
create table adadelta_model 
as
select 
 feature,
 cast(avg(weight) as float) as weight
from 
 (select 
     adadelta(features,label) as (feature,weight)
  from 
     training_orcfile
 ) t 
group by feature;

drop table adadelta_predict;
create table adadelta_predict
  ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY "\t"
    LINES TERMINATED BY "\n"
  STORED AS TEXTFILE
as
select
  t.rowid, 
  sigmoid(sum(m.weight)) as prob
from 
  testing_exploded  t LEFT OUTER JOIN
  adadelta_model m ON (t.feature = m.feature)
group by 
  t.rowid
order by 
  rowid ASC;
```

```sh
hadoop fs -getmerge /user/hive/warehouse/kdd12track2.db/adadelta_predict adadelta_predict.tbl

gawk -F "\t" '{print $2;}' adadelta_predict.tbl > adadelta_predict.submit

pypy scoreKDD.py KDD_Track2_solution.csv adadelta_predict.submit
```
>AUC(SGD) : 0.739351

>AUC(ADAGRAD) : 0.743279

> AUC(AdaDelta) : 0.746878