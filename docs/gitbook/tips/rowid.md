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

# Rowid generator provided in Hivemall
You can use [rowid() function](https://github.com/myui/hivemall/blob/master/src/main/java/hivemall/tools/mapred/RowIdUDF.java) to generate an unique rowid in Hivemall v0.2 or later.
```sql
select
  rowid() as rowid, -- returns ${task_id}-${sequence_number}
  *
from 
  xxx
```

# Other Rowid generation schemes using SQL

```sql
CREATE TABLE xxx
AS
SELECT 
  regexp_replace(reflect('java.util.UUID','randomUUID'), '-', '') as rowid,
  *
FROM
  ..;
```

Another option to generate rowid is to use row_number(). 
However, the query execution would become too slow for large dataset because the rowid generation is executed on a single reducer.
```sql
CREATE TABLE xxx
AS
select 
  row_number() over () as rowid, 
  * 
from a9atest;
```
