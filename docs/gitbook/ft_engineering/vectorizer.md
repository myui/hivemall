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
        
## Feature Vectorizer

`array<string> vectorize_feature(array<string> featureNames, ...)` is useful to generate a feature vector for each row, from a table.

```sql
select vectorize_features(array("a","b"),"0.2","0.3") from dual;
>["a:0.2","b:0.3"]

-- avoid zero weight
select vectorize_features(array("a","b"),"0.2",0) from dual;
> ["a:0.2"]

-- true boolean value is treated as 1.0 (a categorical value w/ its column name)
select vectorize_features(array("a","b","bool"),0.2,0.3,true) from dual;
> ["a:0.2","b:0.3","bool:1.0"]

-- an example to generate feature vectors from table
select * from dual;
> 1                                         
select vectorize_features(array("a"),*) from dual;
> ["a:1.0"]

-- has categorical feature
select vectorize_features(array("a","b","wheather"),"0.2","0.3","sunny") from dual;
> ["a:0.2","b:0.3","whether#sunny"]
```

```sql
select
  id,
  vectorize_features(
    array("age","job","marital","education","default","balance","housing","loan","contact","day","month","duration","campaign","pdays","previous","poutcome"), 
    age,job,marital,education,default,balance,housing,loan,contact,day,month,duration,campaign,pdays,previous,poutcome
  ) as features,
  y
from
  train
limit 2;

> 1       ["age:39.0","job#blue-collar","marital#married","education#secondary","default#no","balance:1756.0","housing#yes","loan#no","contact#cellular","day:3.0","month#apr","duration:939.0","campaign:1.0","pdays:-1.0","poutcome#unknown"]   1
> 2       ["age:51.0","job#entrepreneur","marital#married","education#primary","default#no","balance:1443.0","housing#no","loan#no","contact#cellular","day:18.0","month#feb","duration:172.0","campaign:10.0","pdays:-1.0","poutcome#unknown"]   1
```