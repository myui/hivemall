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
        
a9a
===
http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary.html#a9a

---

preparation
=========

[conv.awk](https://raw.githubusercontent.com/myui/hivemall/master/resources/misc/conv.awk)

```
cd /mnt/archive/datasets/classification/a9a
awk -f conv.awk a9a | sed -e "s/+1/1/" | sed -e "s/-1/0/" > a9a.train
awk -f conv.awk a9a.t | sed -e "s/+1/1/" | sed -e "s/-1/0/" > a9a.test
```

## Putting data on HDFS
```
hadoop fs -mkdir -p /dataset/a9a/train
hadoop fs -mkdir -p /dataset/a9a/test

hadoop fs -copyFromLocal a9a.train /dataset/a9a/train
hadoop fs -copyFromLocal a9a.test /dataset/a9a/test
```

## Training/test data prepareation
```sql
create database a9a;
use a9a;

create external table a9atrain (
  rowid int,
  label float,
  features ARRAY<STRING>
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY "," STORED AS TEXTFILE LOCATION '/dataset/a9a/train';

create external table a9atest (
  rowid int, 
  label float,
  features ARRAY<STRING>
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY "," STORED AS TEXTFILE LOCATION '/dataset/a9a/test';
```