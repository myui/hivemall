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
        
From Hive 0.11.0, **hive.auto.convert.join** is [enabled by the default](https://issues.apache.org/jira/browse/HIVE-3297).

When using complex queries using views, the auto conversion sometimes throws SemanticException, cannot serialize object.

Workaround for the exception is to disable **hive.auto.convert.join** before the execution as follows.
```
set hive.auto.convert.join=false;
```