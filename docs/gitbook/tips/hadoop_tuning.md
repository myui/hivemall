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
        
# Prerequisites 

Please refer the following guides for Hadoop tuning:

* http://hadoopbook.com/
* http://www.slideshare.net/cloudera/mr-perf

---
# Mapper-side configuration
_Mapper configuration is important for hivemall when training runs on mappers (e.g., when using rand_amplify())._

```
mapreduce.map.java.opts="-Xmx2048m -XX:+PrintGCDetails" (YARN)
mapred.map.child.java.opts="-Xmx2048m -XX:+PrintGCDetails" (MR v1)

mapreduce.task.io.sort.mb=1024 (YARN)
io.sort.mb=1024 (MR v1)
```

Hivemall can use at max 1024MB in the above case.
> mapreduce.map.java.opts - mapreduce.task.io.sort.mb = 2048MB - 1024MB = 1024MB

Moreover, other Hadoop components consumes memory spaces. It would be about 1024MB * 0.5 or so is available for Hivemall. We recommend to set at least -Xmx2048m for a mapper.
 
So, make `mapreduce.map.java.opts - mapreduce.task.io.sort.mb` as large as possible.

# Reducer-side configuration
_Reducer configuration is important for hivemall when training runs on reducers (e.g., when using amplify())._

```
mapreduce.reduce.java.opts="-Xmx2048m -XX:+PrintGCDetails" (YARN)
mapred.reduce.child.java.opts="-Xmx2048m -XX:+PrintGCDetails" (MR v1)

mapreduce.reduce.shuffle.input.buffer.percent=0.6 (YARN)
mapred.reduce.shuffle.input.buffer.percent=0.6 (MR v1)

-- mapreduce.reduce.input.buffer.percent=0.2 (YARN)
-- mapred.job.reduce.input.buffer.percent=0.2 (MR v1)
```

Hivemall can use at max 820MB in the above case.
> mapreduce.reduce.java.opts * (1 - mapreduce.reduce.input.buffer.percent) = 2048 * (1 - 0.6) ≈ 820 MB

Moreover, other Hadoop components consumes memory spaces. It would be about 820MB * 0.5 or so is available for Hivemall. We recommend to set at least -Xmx2048m for a reducer.

So, make `mapreduce.reduce.java.opts * (1 - mapreduce.reduce.input.buffer.percent)` as large as possible.

---
# Formula to estimate consumed memory in Hivemall

For a dense model, the consumed memory in Hivemall is as follows:
```
feature_dimensions (2^24 by the default) * 4 bytes (float) * 2 (iff covariance is calculated) * 1.2 (heuristics)
```
> 2^24 * 4 bytes * 2 * 1.2 ≈ 161MB

When [SpaceEfficientDenseModel](https://github.com/myui/hivemall/blob/master/src/main/java/hivemall/io/SpaceEfficientDenseModel.java) is used, the formula changes as follows:
```
feature_dimensions (assume here 2^25) * 2 bytes (short) * 2 (iff covariance is calculated) * 1.2 (heuristics)
```
> 2^25 * 2 bytes * 2 * 1.2 ≈ 161MB

Note: Hivemall uses a [sparse representation](https://github.com/myui/hivemall/blob/master/src/main/java/hivemall/io/SparseModel.java) of prediction model (using a hash table) by the default. Use "[-densemodel](https://github.com/myui/hivemall/blob/master/src/main/java/hivemall/LearnerBaseUDTF.java#L87)" option to use a dense model.

# Execution Engine of Hive

We recommend to use Apache Tez for execute engine of Hive for Hivemall queries.

```sql
set mapreduce.framework.name=yarn-tez;
set hive.execution.engine=tez;
```

You can use the plain old MapReduce by setting following setting:

```sql
set mapreduce.framework.name=yarn;
set hive.execution.engine=mr;
```