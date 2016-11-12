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
        
# Data preparation

First, downlod MovieLens dataset from the following site.
> http://www.grouplens.org/system/files/ml-1m.zip

Get detail about the dataset in the README.
> http://files.grouplens.org/papers/ml-1m-README.txt

You can find three dat file in the archive: 
> movies.dat, ratings.dat, users.dat.

Change column separator as follows:
```sh
sed 's/::/#/g' movies.dat > movies.t
sed 's/::/#/g' ratings.dat > ratings.t
sed 's/::/#/g' users.dat > users.t
```

Create a file named occupations.t with the following contents:
```
0#other/not specified
1#academic/educator
2#artist
3#clerical/admin
4#college/grad student
5#customer service
6#doctor/health care
7#executive/managerial
8#farmer
9#homemaker
10#K-12 student
11#lawyer
12#programmer
13#retired
14#sales/marketing
15#scientist
16#self-employed
17#technician/engineer
18#tradesman/craftsman
19#unemployed
20#writer
```

# Importing data as Hive tables

## create tables
```sql
create database movielens;
use movielens;

CREATE EXTERNAL TABLE ratings (
  userid INT, 
  movieid INT,
  rating INT, 
  tstamp STRING
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '#'
STORED AS TEXTFILE
LOCATION '/dataset/movielens/ratings';

CREATE EXTERNAL TABLE movies (
  movieid INT, 
  title STRING,
  genres ARRAY<STRING>
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '#'
COLLECTION ITEMS TERMINATED BY "|"
STORED AS TEXTFILE
LOCATION '/dataset/movielens/movies';

CREATE EXTERNAL TABLE users (
  userid INT, 
  gender STRING, 
  age INT,
  occupation INT,
  zipcode STRING
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '#'
STORED AS TEXTFILE
LOCATION '/dataset/movielens/users';

CREATE EXTERNAL TABLE occupations (
  id INT,
  occupation STRING
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '#'
STORED AS TEXTFILE
LOCATION '/dataset/movielens/occupations';
```

## load data into tables
```sh
hadoop fs -put ratings.t /dataset/movielens/ratings
hadoop fs -put movies.t /dataset/movielens/movies
hadoop fs -put users.t /dataset/movielens/users
hadoop fs -put occupations.t /dataset/movielens/occupations
```

# Create a concatenated table 
```sql
CREATE TABLE rating_full
as
select 
  r.*, 
  m.title as m_title,
  concat_ws('|',sort_array(m.genres)) as m_genres, 
  u.gender as u_gender,
  u.age as u_age,
  u.occupation as u_occupation,
  u.zipcode as u_zipcode
from
  ratings r 
  JOIN movies m ON (r.movieid = m.movieid)
  JOIN users u ON (r.userid = u.userid);
```

hive> desc rating_full;
```
userid                  int                     None
movieid                 int                     None
rating                  int                     None
tstamp                  string                  None
m_title                 string                  None
m_genres                string                  None
u_gender                string                  None
u_age                   int                     None
u_occupation            int                     None
u_zipcode               string                  None
```

---
# Creating training/testing data

Create a training/testing table such that each has 80%/20% of the original rating data.

```sql
-- Adding rowids to the rating table
SET hivevar:seed=31;
CREATE TABLE ratings2
as
select
  rand(${seed}) as rnd, 
  userid, 
  movieid, 
  rating
from 
  ratings;

CREATE TABLE training
as
select * from ratings2
order by rnd DESC
limit 800000;

CREATE TABLE testing
as
select * from ratings2
order by rnd ASC
limit 200209;
```
