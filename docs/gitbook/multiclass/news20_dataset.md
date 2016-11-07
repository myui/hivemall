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
        
Get the news20 dataset.
http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass.html#news20

```sh
$ cat <<EOF > conv.awk
BEGIN{ FS=" " }
{
    label=\$1;
    features=\$2;
    for(i=3;i<=NF;i++)
    {
        features = features "," \$i;
    }
    print NR "\t" label "\t" features;
}
END{}
EOF

$ gawk -f conv.awk news20.scale > news20_scale.train
$ gawk -f conv.awk news20.t.scale > news20_scale.test
```

## Putting data on HDFS
```sh
hadoop fs -mkdir -p /dataset/news20-multiclass/train
hadoop fs -mkdir -p /dataset/news20-multiclass/test

hadoop fs -copyFromLocal news20_scale.train /dataset/news20-multiclass/train
hadoop fs -copyFromLocal news20_scale.test /dataset/news20-multiclass/test
```

## Training/test data prepareation
```sql
use news20;

delete jar /home/myui/tmp/hivemall.jar;
add jar /home/myui/tmp/hivemall.jar;

source /home/myui/tmp/define-all.hive;

Create external table news20mc_train (
  rowid int,
  label int,
  features ARRAY<STRING>
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY "," STORED AS TEXTFILE LOCATION '/dataset/news20-multiclass/train';

Create external table news20mc_test (
  rowid int, 
  label int,
  features ARRAY<STRING>
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY "," STORED AS TEXTFILE LOCATION '/dataset/news20-multiclass/test';

set hivevar:seed=31;
create or replace view news20mc_train_x3
as
select 
  * 
from (
select
   amplify(3, *) as (rowid, label, features)
from  
   news20mc_train 
) t
CLUSTER BY rand(${seed});

create table news20mc_test_exploded as
select 
  rowid,
  label,
  cast(split(feature,":")[0] as int) as feature,
  cast(split(feature,":")[1] as float) as value
  -- hivemall v0.3.1 or later
  -- cast(extract_feature(feature) as int) as feature,
  -- extract_weight(feature) as value
from 
  news20mc_test LATERAL VIEW explode(addBias(features)) t AS feature;
```