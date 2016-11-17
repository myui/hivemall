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

# create a dual table

Create a [dual table](http://en.wikipedia.org/wiki/DUAL_table) as follows:
```sql
CREATE TABLE dual (
  dummy int
);
INSERT INTO TABLE dual SELECT count(*)+1 FROM dual;
```

# Sparse dataset generation by a single task
```sql
create table regression_data1
as
select lr_datagen('-n_examples 10k -n_features 10 -seed 100') as (label,features)
from dual;
```
Find the details of the option, run `lr_datagen('-help')`.

You can generate a sparse dataset as well as a dense dataset. By the default, a sparse dataset is generated.
```sql
hive> desc regression_data1;
OK
label                   float                   None
features                array<string>           None

hive> select * from regression_data1 limit 2;
OK
0.7220096       ["140:2.8347101","165:3.0056276","179:4.030076","112:3.3919246","99:3.98914","16:3.5653272","128:3.046535","124:2.7708225","78:2.4960368","6:1.7866131"]
0.7346627       ["139:1.9607254","110:2.958568","186:3.2524762","31:3.9243593","167:0.72854257","26:1.8355447","117:2.7663715","3:2.1551287","179:3.1099443","19:3.6411424"]
Time taken: 0.046 seconds, Fetched: 2 row(s)
```

# Classification dataset generation
You can use "-cl" option to generation 0/1 label.
```sql
select lr_datagen("-cl") as (label,features)
from dual 
limit 5;
OK
1.0     ["84:3.4227803","80:3.8875976","58:3.2909582","123:3.1056073","194:3.3360343","199:2.20207","75:3.5469763","74:3.3869767","126:0.9969454","93:2.5352612"]
0.0     ["84:-0.5568947","10:0.621897","6:-0.13126314","190:0.18610542","131:1.7232913","24:-2.7551131","113:-0.9842969","177:0.062993184","176:-0.19020283","21:-0.54811275"]
1.0     ["73:3.4391513","198:4.42387","164:4.248151","66:3.5224934","84:1.9026604","76:0.79803777","18:2.2168183","163:2.248695","119:1.5906067","72:2.0267224"]
1.0     ["34:2.9269936","35:0.37033868","39:3.771989","47:2.2087111","28:2.9445739","55:4.134555","14:2.4297745","164:3.0913055","52:2.0519433","128:2.9108515"]
1.0     ["98:4.2451696","4:3.486905","133:2.4589922","26:2.7301126","103:2.6827147","2:3.6198254","34:3.7042716","47:2.5515237","68:2.4294896","197:4.4958663"]
```

# Dense dataset generation
```sql
create table regression_data_dense
as
select lr_datagen("-dense -n_examples 9999 -n_features 100 -n_dims 100") as (label,features)
from dual;

hive> desc regression_data_dense;
OK
label                   float                   None
features                array<float>            None

hive> select * from regression_data_dense limit 1;
OK
0.7274741       [4.061373,3.9373128,3.5195694,3.3604698,3.7698417,4.2518,3.8796813,1.6020582,4.937072,1.5513933,3.0289552,2.6674519,3.432688,2.980945,1.8897587,2.9770515,3.3435504,1.7867403,3.4057906,1.2151588,5.0587463,2.1410913,2.8097973,2.4518871,3.175268,3.3347685,3.728993,3.1443396,3.5506077,3.6357877,4.248151,3.5224934,3.2423255,2.5188355,1.8626233,2.8432152,2.2762651,4.57472,2.2168183,2.248695,3.3636255,2.8359523,2.0327945,1.5917025,2.9269936,0.37033868,2.6151125,4.545956,2.0863252,3.7857852,2.9445739,4.134555,3.0660007,3.4279037,2.0519433,2.9108515,3.5171766,3.4708095,3.161707,2.39229,2.4589922,2.7301126,3.5303073,2.7398396,3.7042716,2.5515237,3.0943663,0.41565156,4.672767,3.1461313,3.0443575,3.4023938,2.2205734,1.8950733,2.1664586,4.8654623,2.787029,4.0460386,2.4455893,3.464298,1.062505,3.0513604,4.382525,2.771433,3.2828436,3.803544,2.178681,4.2466116,3.5440445,3.1546876,3.4248536,0.9067459,3.0134914,1.9528451,1.7175893,2.7029774,2.5759792,3.643847,3.0799,3.735559]
Time taken: 0.044 seconds, Fetched: 1 row(s)
```

# Parallel and scalable data generation using multiple reducers (RECOMMENDED)
Dataset generation using (at max) 10 reducers.

```sql
set hivevar:n_parallel_datagen=10;

create or replace view seq10 
as
select * from (
  select generate_series(1,${n_parallel_datagen})
  from dual 
) t
DISTRIBUTE BY value;

set mapred.reduce.tasks=${n_parallel_datagen};
create table lrdata1k
as
select lr_datagen("-n_examples 100")
from seq10;
set mapred.reduce.tasks=-1; -- reset to the default setting

hive> select count(1) from lrdata1k;
OK
1000
```