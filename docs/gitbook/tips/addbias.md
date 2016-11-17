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
        
A trainer learns the function f(x)=y, or weights _W_, of the following form to predict a label y where x is a feature vector.
_y=f(x)=Wx_

Without a bias clause (or regularization), f(x) cannot make a hyperplane that divides (1,1) and (2,2) becuase f(x) crosses the origin point (0,0).

With bias clause b, a trainer learns the following f(x).
_f(x)=Wx+b_ 
Then, the predicted model considers bias existing in the dataset and the predicted hyperplane does not always cross the origin.

**addBias()** of Hivemall, adds a bias to a feature vector. 
To enable a bias clause, use addBias() for **both**_(important!)_ training and test data as follows.
The bias _b_ is a feature of "0" ("-1" in before v0.3) by the default. See [AddBiasUDF](../tips/addbias.html) for the detail.

Note that Bias is expressed as a feature that found in all training/testing examples.

# Adding a bias clause to test data
```sql
create table e2006tfidf_test_exploded as
select 
  rowid,
  target,
  split(feature,":")[0] as feature,
  cast(split(feature,":")[1] as float) as value
  -- extract_feature(feature) as feature, -- hivemall v0.3.1 or later
  -- extract_weight(feature) as value     -- hivemall v0.3.1 or later
from 
  e2006tfidf_test LATERAL VIEW explode(addBias(features)) t AS feature;
```

# Adding a bias clause to training data
```
create table e2006tfidf_pa1a_model as
select 
 feature,
 avg(weight) as weight
from 
 (select 
     pa1a_regress(addBias(features),target) as (feature,weight)
  from 
     e2006tfidf_train_x3
 ) t 
group by feature;
```