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
        
# Extract clusters and assign N cluster IDs to each article
```
create or replace view news20_cluster
as
select 
  minhash(rowid, features) as (clusterId, rowid)
from 
  news20mc_train;

create table news20_with_clusterid
as
select 
  t1.clusterid, 
  t1.rowid, 
  o1.features
from 
  news20_cluster t1
  JOIN news20mc_train o1 ON (t1.rowid = o1.rowid);
```

# Query expression with cluster id
```
set hivevar:noWeight=false;

create table extract_target_cluster
as
select 
  features,
  clusterid
from (
  select 
     features,
     minhashes(features,${noWeight}) as clusters
  from 
     news20mc_test 
  where 
     rowid = 1
) t1
LATERAL VIEW explode(clusters) t2 AS clusterid;
```

# kNN search using minhashing
```sql
set hivevar:topn=10;

select 
  t1.rowid, 
  cosine_similarity(t1.features, q1.features, false) as similarity
from
  news20_with_clusterid t1
  JOIN extract_target_cluster q1 ON (t1.clusterid = q1.clusterid)
order by
  similarity DESC
limit ${topn};
```

> Time taken: 22.161 seconds

|rowid  | similarity |
|:------|-----------:|
| 2182  | 0.21697778  |
| 5622  | 0.21483186  |
| 962   |  0.13240485 |
| 12242 |  0.12158953 |
| 5102  | 0.11168713  |
| 8562  | 0.107470974 |
| 14396 |0.09949879   |
| 2542  | 0.09011547  |
| 1645  | 0.08894014  |
| 2862  | 0.08800333  |

# Brute force kNN search (based on cosine similarity)
```sql
select
  t1.rowid,
  cosine_similarity(t1.features, q1.features) as similarity -- hive v0.3.2 or later
  -- cosine_similarity(t1.features, q1.features, false) as similarity -- hive v0.3.1 or before
from 
  news20mc_train t1
  CROSS JOIN
  (select features from news20mc_test where rowid = 1) q1
ORDER BY
  similarity DESC
limit ${topn};
```

> Time taken: 24.335 seconds

|rowid  | similarity |
|:------|-----------:|
| 12902 | 0.47759432 |
| 7922  | 0.4184913  |
| 2382  | 0.21919869 |
| 2182  | 0.21697778 |
| 5622  | 0.21483186 |
| 9562  | 0.21223815 |
| 3222  | 0.164399   |
| 11202 | 0.16439897 |
| 10122 | 0.1620197  |
| 8482  | 0.15229382 |


Refer [this page](../misc/topk.html#top-k-similarity-computation) for efficient top-k kNN computation.
