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
        
Map-side join on Tez causes [ClassCastException](http://markmail.org/message/7cwbgupnhah6ggkv) when a serialized table contains array column(s).

[Workaround] Try setting _hive.mapjoin.optimized.hashtable_ off as follows:
```sql
set hive.mapjoin.optimized.hashtable=false;
```

Caution: Fixed in Hive 1.3.0. Refer [HIVE_11051](https://issues.apache.org/jira/browse/HIVE-11051) for the detail.