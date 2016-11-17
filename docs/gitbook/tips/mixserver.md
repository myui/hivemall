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
        
In this page, we will explain how to use model mixing on Hivemall. The model mixing is useful for a better prediction performance and faster convergence in training classifiers. 
You can find a brief explanation of the internal design of MIX protocol in [this slide](http://www.slideshare.net/myui/hivemall-mix-internal).

<!-- toc -->

Prerequisite
============

* Hivemall v0.3 or later

    We recommend to use Mixing in a cluster with fast networking. The current standard GbE is enough though.

Running Mix Server
===================

First, put the following files on server(s) that are accessible from Hadoop worker nodes:
* [target/hivemall-mixserv.jar](https://github.com/myui/hivemall/releases)
* [bin/run_mixserv.sh](https://github.com/myui/hivemall/raw/master/bin/run_mixserv.sh)

_Caution: hivemall-mixserv.jar is large in size and thus only used for Mix servers._

```sh
# run a Mix Server
./run_mixserv.sh
```

We assume in this example that Mix servers are running on host01, host03 and host03.
The default port used by Mix server is 11212 and the port is configurable through "-port" option of run_mixserv.sh. 

See [MixServer.java](https://github.com/myui/hivemall/blob/master/mixserv/src/main/java/hivemall/mix/server/MixServer.java#L90) to get detail of the Mix server options.

We recommended to use multiple MIX servers to get better MIX throughput (3-5 or so would be enough for normal cluster size). The MIX protocol of Hivemall is *horizontally scalable* by adding MIX server nodes.

Using Mix Protocol through Hivemall
===================================

[Install Hivemall](../getting_started/installation.html) on Hive.

_Make sure that [hivemall-with-dependencies.jar](https://github.com/myui/hivemall/raw/master/target/hivemall-with-dependencies.jar) is used for installation. The jar contains minimum requirement jars (netty,jsr305) for running Hivemall on Hive._

Now, we explain that how to use mixing in [an example using KDD2010a dataset](../binaryclass/kdd2010a_dataset.html).

Enabling the mixing on Hivemall is simple as follows:
```sql
use kdd2010;

create table kdd10a_pa1_model1 as
select 
 feature,
 cast(voted_avg(weight) as float) as weight
from 
 (select 
     train_pa1(addBias(features),label,"-mix host01,host02,host03") as (feature,weight)
  from 
     kdd10a_train_x3
 ) t 
group by feature;
```

All you have to do is just adding "*-mix*" training option as seen in the above query.

The effect of model mixing
===========================

In my experience, the MIX improved the prediction accuracy of the above KDD2010a PA1 training on a 32 nodes cluster from 0.844835019263103 (w/o mix) to 0.8678096499719774 (w/ mix).

The overhead of using the MIX protocol is *almost negligible* because the MIX communication is efficiently handled using asynchronous non-blocking I/O. Furthermore, the training time could be improved on certain settings because of the faster convergence due to mixing. 