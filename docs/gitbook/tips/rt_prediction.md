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

Apache Hivemall provides a batch learning scheme that builds prediction models on Apache Hive.
The learning process itself is a batch process; however, an online/real-time prediction can be achieved by carrying a prediction on a transactional relational DBMS.

In this article, we explain how to run a real-time prediction using a relational DBMS. 
We assume that you have already run the [a9a binary classification task](../binaryclass/a9a.html).

<!-- toc -->

# Prerequisites

- MySQL

    Put mysql-connector-java.jar (JDBC driver) on $SQOOP_HOME/lib.

- [Sqoop](http://sqoop.apache.org/)

    Sqoop 1.4.5 does not support Hadoop v2.6.0. So, you need to build packages for Hadoop 2.6.
    To do that you need to edit build.xml and ivy.xml as shown in [this patch](https://gist.github.com/myui/e8db4a31b574103133c6).

# Preparing Model Tables on MySQL

```sql
create database a9a;
use a9a;

create user sqoop identified by 'sqoop';
grant all privileges on a9a.* to 'sqoop'@'%' identified by 'sqoop';
flush privileges;

create table a9a_model1 (
  feature int, 
  weight double
);
```

Do not forget to edit bind_address in the MySQL configuration file (/etc/mysql/my.conf) accessible from master and slave nodes of Hadoop.

# Exporting Hive table to MySQL

Check the connectivity to MySQL server using Sqoop.

```sh
export MYSQL_HOST=dm01

export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop/
export HADOOP_COMMON_HOME=${HADOOP_HOME}

bin/sqoop list-tables --connect jdbc:mysql://${MYSQL_HOST}/a9a --username sqoop --password sqoop
```

Create TSV table because Sqoop cannot directory read Hive tables.

```sql
create table a9a_model1_tsv 
  ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY "\t"
    LINES TERMINATED BY "\n"
  STORED AS TEXTFILE
AS
select * from a9a_model1;
```

Check the location of 'a9a_model1_tsv' as follows:

```sql
desc extended a9a_model1_tsv;
> location:hdfs://dm01:9000/user/hive/warehouse/a9a.db/a9a_model1_tsv
```

```sh
bin/sqoop export \
--connect jdbc:mysql://${MYSQL_HOST}/a9a \
--username sqoop --password sqoop \
--table a9a_model1 \
--export-dir /user/hive/warehouse/a9a.db/a9a_model1_tsv \
--input-fields-terminated-by '\t' --input-lines-terminated-by '\n' \
--batch
```

When the exporting successfully finishes, you can find entries in the model table in MySQL.

```sql
mysql> select * from a9a_model1 limit 3;
+---------+---------------------+
| feature | weight              |
+---------+---------------------+
|       0 | -0.5761121511459351 |
|       1 | -1.5259535312652588 |
|      10 | 0.21053194999694824 |
+---------+---------------------+
3 rows in set (0.00 sec)
```

We recommend to create an index of model tables to boost lookups in online prediction.

```sql
CREATE UNIQUE INDEX a9a_model1_feature_index on a9a_model1 (feature);
-- USING BTREE;
```

# Exporting test data from Hadoop to MySQL (optional step)

Prepare a testing data table in Hive which is being exported.

```sql
create table a9atest_exploded_tsv
  ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY "\t"
    LINES TERMINATED BY "\n"
  STORED AS TEXTFILE
AS
select
  rowid, 
  -- label, 
  extract_feature(feature) as feature,
  extract_weight(feature) as value
from
  a9atest LATERAL VIEW explode(addBias(features)) t AS feature;

desc extended a9atest_exploded_tsv;
> location:hdfs://dm01:9000/user/hive/warehouse/a9a.db/a9atest_exploded_tsv,
```

Prepare a test table, importing data from Hadoop.

```sql
use a9a;

create table a9atest_exploded (
  rowid bigint,
  feature int, 
  value double
);
```

Then, run Sqoop to export data from HDFS to MySQL.

```sh
export MYSQL_HOST=dm01

bin/sqoop export \
--connect jdbc:mysql://${MYSQL_HOST}/a9a \
--username sqoop --password sqoop \
--table a9atest_exploded \
--export-dir /user/hive/warehouse/a9a.db/a9atest_exploded_tsv \
--input-fields-terminated-by '\t' --input-lines-terminated-by '\n' \
--batch
```

Better to add an index to the rowid column to boost selection by rowids.
```sql
CREATE INDEX a9atest_exploded_rowid_index on a9atest_exploded (rowid) USING BTREE;
```

When the exporting successfully finishes, you can find entries in the test table in MySQL.

```sql
mysql> select * from a9atest_exploded limit 10;
+-------+---------+-------+
| rowid | feature | value |
+-------+---------+-------+
| 12427 |      67 |     1 |
| 12427 |      73 |     1 |
| 12427 |      74 |     1 |
| 12427 |      76 |     1 |
| 12427 |      82 |     1 |
| 12427 |      83 |     1 |
| 12427 |       0 |     1 |
| 12428 |       5 |     1 |
| 12428 |       7 |     1 |
| 12428 |      16 |     1 |
+-------+---------+-------+
10 rows in set (0.00 sec)
```

# Online/realtime prediction on MySQL

Define sigmoid function used for a prediction of logistic regression as follows: 

```sql
DROP FUNCTION IF EXISTS sigmoid;
DELIMITER $$
CREATE FUNCTION sigmoid(x DOUBLE)
  RETURNS DOUBLE
  LANGUAGE SQL
BEGIN
  RETURN 1.0 / (1.0 + EXP(-x));
END;
$$
DELIMITER ;
```

We assume here that doing prediction for a 'features' having (0,1,10) and each of them is a categorical feature (i.e., the weight is 1.0). Then, you can get the probability by logistic regression simply as follows:

```sql
select
  sigmoid(sum(m.weight)) as prob
from
  a9a_model1 m
where
  m.feature in (0,1,10);
```

```
+--------------------+
| prob               |
+--------------------+
| 0.1310696931351625 |
+--------------------+
1 row in set (0.00 sec)
```

Similar to [the way in Hive](../binaryclass/a9a_lr.html#prediction), you can run prediction as follows:

```sql
select
  sigmoid(sum(t.value * m.weight)) as prob, 
  if(sigmoid(sum(t.value * m.weight)) > 0.5, 1.0, 0.0) as predicted
from
  a9atest_exploded t LEFT OUTER JOIN
  a9a_model1 m ON (t.feature = m.feature)
where
  t.rowid = 12427; -- prediction on a particular id
```

Alternatively, you can use SQL views for testing target 't' in the above query.

```
+---------------------+-----------+
| prob                | predicted |
+---------------------+-----------+
| 0.05595205126313402 |       0.0 |
+---------------------+-----------+
1 row in set (0.00 sec)
```