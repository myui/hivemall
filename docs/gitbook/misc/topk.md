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
        
`each_top_k(int k, ANY group, double value, arg1, arg2, ..., argN)` returns a top-k records for each `group`. It returns a relation consists of `(int rank, double value, arg1, arg2, .., argN)`.

This function is particularly useful for applying a similarity/distance function where the computation complexity is **O(nm)**.

`each_top_k` is very fast when compared to other methods running top-k queries (e.g., [`rank/distribute by`](https://ragrawal.wordpress.com/2011/11/18/extract-top-n-records-in-each-group-in-hadoophive/)) in Hive.

<!-- toc -->

# Caution

* `each_top_k` is supported from Hivemall v0.3.2-3 or later.
* This UDTF assumes that input records are sorted by `group`. Use `DISTRIBUTE BY group SORT BY group` to ensure that. Or, you can use `LEFT OUTER JOIN` for certain cases.
* It takes variable lengths arguments in `argN`. 
* The third argument `value` is used for the comparison.
* `Any number types` or `timestamp` are accepted for the type of `value`.
* If k is less than 0, reverse order is used and `tail-K` records are returned for each `group`.
* Note that this function returns [a pseudo ranking](http://www.michaelpollmeier.com/selecting-top-k-items-from-a-list-efficiently-in-java-groovy/) for top-k. It always returns `at-most K` records for each group. The ranking scheme is similar to `dense_rank` but slightly different in certain cases.

# Usage

## Efficient Top-k Query Processing using `each_top_k`

Efficient processing of Top-k queries is a crucial requirement in many interactive environments that involve massive amounts of data. 
Our Hive extension `each_top_k` helps running Top-k processing efficiently.

- Suppose the following table as the input

|student | class | score |
|:------:|:-----:|:-----:|
|1       | b     | 70    |
|2       | a     | 80    |
|3       | a     | 90    |
|4       | b     | 50    |
|5       | a     | 70    |
|6       | b     | 60    |

- Then, list top-2 students for each class

|student | class | score | rank |
|:------:|:-----:|:-----:|:----:|
|3       | a     | 90    | 1    |
|2       | a     | 80    | 2    |
|1       | b     | 70    | 1    |
|6       | b     | 60    | 2    |

The standard way using SQL window function would be as follows:

```sql
SELECT 
  student, class, score, rank
FROM (
  SELECT
    student, class, score, 
    rank() over (PARTITION BY class ORDER BY score DESC) as rank
  FROM
    table
) t
WHRE rank <= 2
```

An alternative and efficient way to compute top-k items using `each_top_k` is as follows:

```sql
SELECT 
  each_top_k(
    2, class, score,
    class, student -- output columns other in addition to rank and score
  ) as (rank, score, class, student)
FROM (
  SELECT * FROM table
  CLUSTER BY class -- Mandatory for `each_top_k`
) t
```

> #### Note
>
> `CLUSTER BY x` is a synonym of `DISTRIBUTE BY x CLASS SORT BY x` and required when using `each_top_k`.

The function signature of `each_top_k` is `each_top_k(int k, ANY group, double value, arg1, arg2, ..., argN)` and it returns a relation `(int rank, double value, arg1, arg2, .., argN)`.

Any number types or timestamp are accepted for the type of `value` but it MUST be not NULL. 
Do null hanlding like `if(value is null, -1, value)` to avoid null.

If `k` is less than 0, reverse order is used and tail-K records are returned for each `group`.

The ranking semantics of `each_top_k` follows SQL's `dense_rank` and then limits results by `k`. 

> #### Caution
>
> `each_top_k` is benefical where the number of grouping keys are large. If the number of grouping keys are not so large (e.g., less than 100), consider using `rank() over` instead.

## top-k clicks 

http://stackoverflow.com/questions/9390698/hive-getting-top-n-records-in-group-by-query/32559050#32559050

```sql
set hivevar:k=5;

select
  page-id, 
  user-id,
  clicks
from (
  select
    each_top_k(${k}, page-id, clicks, page-id, user-id)
      as (rank, clicks, page-id, user-id)
  from (
    select
      page-id, user-id, clicks
    from
      mytable
    DISTRIBUTE BY page-id SORT BY page-id
  ) t1
) t2
order by page-id ASC, clicks DESC;
```

## Top-k similarity computation

```sql
set hivevar:k=10;

SELECT
  each_top_k(
    ${k}, t2.id, angular_similarity(t2.features, t1.features), 
    t2.id, 
    t1.id,  
    t1.y
  ) as (rank, similarity, base_id, neighbor_id, y)
FROM
  test_hivemall t2 
  LEFT OUTER JOIN train_hivemall t1;
```

| rank | similarity | base_id | neighbor_id | y |
|:----:|:----------:|:-------:|:-----------:|:-:|
| 1  | 0.8594650626182556 | 12 | 10514 | 0 |
| 2  | 0.8585299849510193 | 12 | 11719 | 0 |
| 3  | 0.856602132320404  | 12 | 21009 | 0 |
| 4  | 0.8562054634094238 | 12 | 17582 | 0 |
| 5  | 0.8516314029693604 | 12 | 22006 | 0 |
| 6  | 0.8499397039413452 | 12 | 25364 | 0 |
| 7  | 0.8467264771461487 | 12 | 900   | 0 |
| 8  | 0.8463355302810669 | 12 | 8018  | 0 |
| 9  | 0.8439178466796875 | 12 | 7041  | 0 |
| 10 | 0.8438876867294312 | 12 | 21595 | 0 |
| 1  | 0.8390793800354004 | 25 | 21125 | 0 |
| 2  | 0.8344510793685913 | 25 | 14073 | 0 |
| 3  | 0.8340602517127991 | 25 | 9008  | 0 |
| 4  | 0.8328862190246582 | 25 | 6598  | 0 |
| 5  | 0.8301891088485718 | 25 | 943   | 0 |
| 6  | 0.8271955251693726 | 25 | 20400 | 0 |
| 7  | 0.8255619406700134 | 25 | 10922 | 0 |
| 8  | 0.8241575956344604 | 25 | 8477  | 0 |
| 9  | 0.822281539440155  | 25 | 25977 | 0 |
| 10 | 0.8205751180648804 | 25 | 21115 | 0 |
| 1  | 0.9761330485343933 | 34 | 2513  | 0 |
| 2  | 0.9536819458007812 | 34 | 8697  | 0 |
| 3  | 0.9531533122062683 | 34 | 7326  | 0 |
| 4  | 0.9493276476860046 | 34 | 15173 | 0 |
| 5  | 0.9480557441711426 | 34 | 19468 | 0 |
| .. | .. | .. | .. | .. |

### Explicit grouping using `distribute by` and `sort by`

```sql
SELECT
  each_top_k(
    10, id1, angular_similarity(features1, features2), 
    id1, 
    id2,  
    y
  ) as (rank, similarity, id, other_id, y)
FROM (
select
  t1.id as id1,
  t2.id as id2,
  t1.features as features1,
  t2.features as features2,
  t1.y
from
  train_hivemall t1
  CROSS JOIN test_hivemall t2
DISTRIBUTE BY id1 SORT BY id1
) t;
```

### Parallelization of similarity computation using WITH clause

```sql
create table similarities
as
WITH test_rnd as (
select
  rand(31) as rnd,
  id,
  features
from
  test_hivemall
),
t01 as (
select
 id,
 features
from
 test_rnd
where
 rnd < 0.2
),
t02 as (
select
 id,
 features
from
 test_rnd
where
 rnd >= 0.2 and rnd < 0.4
),
t03 as (
select
 id,
 features
from
 test_rnd
where
 rnd >= 0.4 and rnd < 0.6
),
t04 as (
select
 id,
 features
from
 test_rnd
where
 rnd >= 0.6 and rnd < 0.8
),
t05 as (
select
 id,
 features
from
 test_rnd
where
 rnd >= 0.8
),
s01 as (
SELECT
  each_top_k(
    10, t2.id, angular_similarity(t2.features, t1.features), 
    t2.id, 
    t1.id,  
    t1.y
  ) as (rank, similarity, base_id, neighbor_id, y)
FROM
  t01 t2 
  LEFT OUTER JOIN train_hivemall t1
),
s02 as (
SELECT
  each_top_k(
    10, t2.id, angular_similarity(t2.features, t1.features), 
    t2.id, 
    t1.id,  
    t1.y
  ) as (rank, similarity, base_id, neighbor_id, y)
FROM
  t02 t2 
  LEFT OUTER JOIN train_hivemall t1
),
s03 as (
SELECT
  each_top_k(
    10, t2.id, angular_similarity(t2.features, t1.features), 
    t2.id, 
    t1.id,  
    t1.y
  ) as (rank, similarity, base_id, neighbor_id, y)
FROM
  t03 t2 
  LEFT OUTER JOIN train_hivemall t1
),
s04 as (
SELECT
  each_top_k(
    10, t2.id, angular_similarity(t2.features, t1.features), 
    t2.id, 
    t1.id,  
    t1.y
  ) as (rank, similarity, base_id, neighbor_id, y)
FROM
  t04 t2 
  LEFT OUTER JOIN train_hivemall t1
),
s05 as (
SELECT
  each_top_k(
    10, t2.id, angular_similarity(t2.features, t1.features), 
    t2.id, 
    t1.id,  
    t1.y
  ) as (rank, similarity, base_id, neighbor_id, y)
FROM
  t05 t2 
  LEFT OUTER JOIN train_hivemall t1
)
select * from s01
union all
select * from s02
union all
select * from s03
union all
select * from s04
union all
select * from s05;
```

## tail-K

```sql
set hivevar:k=-10;

SELECT
  each_top_k(
    ${k}, t2.id, angular_similarity(t2.features, t1.features), 
    t2.id, 
    t1.id,  
    t1.y
  ) as (rank, similarity, base_id, neighbor_id, y)
FROM
  test_hivemall t2 
  LEFT OUTER JOIN train_hivemall t1
-- limit 25
```

| rank | similarity | base_id | neighbor_id | y |
|:----:|:----------:|:-------:|:-----------:|:-:|
| 1  | 0.4383084177970886  | 1 | 7503  | 0 |
| 2  | 0.44166821241378784 | 1 | 10143 | 0 |
| 3  | 0.4424300789833069  | 1 | 11073 | 0 |
| 4  | 0.44254064559936523 | 1 | 17782 | 0 |
| 5  | 0.4442034363746643  | 1 | 18556 | 0 |
| 6  | 0.45163780450820923 | 1 | 3786  | 0 |
| 7  | 0.45244503021240234 | 1 | 10242 | 0 |
| 8  | 0.4525672197341919  | 1 | 21657 | 0 |
| 9  | 0.4527127146720886  | 1 | 17218 | 0 |
| 10 | 0.45314133167266846 | 1 | 25141 | 0 |
| 1  | 0.44030147790908813 | 2 | 3786  | 0 |
| 2  | 0.4408798813819885  | 2 | 23386 | 0 |
| 3  | 0.44112563133239746 | 2 | 11073 | 0 |
| 4  | 0.4415401816368103  | 2 | 22853 | 0 |
| 5  | 0.4422193765640259  | 2 | 21657 | 0 |
| 6  | 0.4429032802581787  | 2 | 10143 | 0 |
| 7  | 0.4435907006263733  | 2 | 24413 | 0 |
| 8  | 0.44569307565689087 | 2 | 7503  | 0 |
| 9  | 0.4460843801498413  | 2 | 25141 | 0 |
| 10 | 0.4464914798736572  | 2 | 24289 | 0 |
| 1  | 0.43862903118133545 | 3 | 23150 | 1 |
| 2  | 0.4398220181465149  | 3 | 9881  | 1 |
| 3  | 0.44283604621887207 | 3 | 27121 | 0 |
| 4  | 0.4432108402252197  | 3 | 26220 | 1 |
| 5  | 0.44323229789733887 | 3 | 18541 | 0 |
| .. | .. | .. | .. | .. |
