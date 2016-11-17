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
        
This page explains how to apply [Mini-Batch Gradient Descent](https://class.coursera.org/ml-003/lecture/106) for the training of logistic regression explained in [this example](./a9a_lr.html). 
So, refer [this page](./a9a_lr.html) first. This content depends on it.

# Training

Replace `a9a_model1` of [this example](./a9a_lr.html).

```sql
set hivevar:total_steps=32561;
set hivevar:mini_batch_size=10;

create table a9a_model1 
as
select 
 cast(feature as int) as feature,
 avg(weight) as weight
from 
 (select 
     logress(addBias(features),label,"-total_steps ${total_steps} -mini_batch ${mini_batch_size}") as (feature,weight)
  from 
     a9atrain
 ) t 
group by feature;
```

# Evaluation

```sql
select count(1) / ${num_test_instances} from a9a_submit1 
where actual == predicted;
```


| Stochastic Gradient Descent | Minibatch Gradient Descent |
| ------------- | ------------- |
| 0.8430071862907684 | 0.8463239358761747 |