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
        
*One-vs-the-rest* is a multiclass classification method that uses binary classifiers independently for each class.
http://en.wikipedia.org/wiki/Multiclass_classification#one_vs_all

## UDF preparation
```sql
delete jar /home/myui/tmp/hivemall.jar;
add jar /home/myui/tmp/hivemall.jar;

source /home/myui/tmp/define-all.hive;
```

## Dataset preparation for one-vs-the-rest classifiers

```sql
select collect_set(label) from news20mc_train;
```
> [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,17,16,19,18,20]

```sql
SET hivevar:possible_labels="1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,17,16,19,18,20";
```

[one-vs-rest.awk](https://github.com/myui/hivemall/blob/master/resources/misc/one-vs-rest.awk)

```
create or replace view news20_onevsrest_train
as
select transform(${possible_labels}, rowid, label, addBias(features))
  ROW FORMAT DELIMITED
    FIELDS TERMINATED BY "\t"
    COLLECTION ITEMS TERMINATED BY ","
    LINES TERMINATED BY "\n"
using 'gawk -f one-vs-rest.awk'
  as (rowid BIGINT, label INT, target INT, features ARRAY<STRING>)
  ROW FORMAT DELIMITED
    FIELDS TERMINATED BY "\t"
    COLLECTION ITEMS TERMINATED BY ","
    LINES TERMINATED BY "\n"
from news20mc_train;

create or replace view news20_onevsrest_train_x3
as
select
 *
from (
  select
    amplify(3, *) as (rowid, label, target, features)
  from
    news20_onevsrest_train
) t
CLUSTER BY rand();
```
