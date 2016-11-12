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
        
This document explains how to compute [TF-IDF](http://en.wikipedia.org/wiki/Tf%E2%80%93idf) with Apache Hive/Hivemall.

What you need to compute TF-IDF is a table/view composing (docid, word) pair, 2 views, and 1 query.

_Note that this feature is supported since Hivemall v0.3-beta3 or later. Macro is supported since Hive 0.12 or later._

# Define macros used in the TF-IDF computation
```sql
create temporary macro max2(x INT, y INT)
if(x>y,x,y);

-- create temporary macro idf(df_t INT, n_docs INT)
-- (log(10, CAST(n_docs as FLOAT)/max2(1,df_t)) + 1.0);

create temporary macro tfidf(tf FLOAT, df_t INT, n_docs INT)
tf * (log(10, CAST(n_docs as FLOAT)/max2(1,df_t)) + 1.0);
```

# Data preparation
To calculate TF-IDF, you need to prepare a relation consists of (docid,word) tuples.
```sql
create external table wikipage (
  docid int,
  page string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

cd ~/tmp
wget https://gist.githubusercontent.com/myui/190b91a3a792ccfceda0/raw/327acd192da4f96da8276dcdff01b19947a4373c/tfidf_test.tsv

LOAD DATA LOCAL INPATH '/home/myui/tmp/tfidf_test.tsv' INTO TABLE wikipage;

create or replace view wikipage_exploded
as
select
  docid, 
  word
from
  wikipage LATERAL VIEW explode(tokenize(page,true)) t as word
where
  not is_stopword(word);
```
You can download the data of the wikipage table from [this link]( https://gist.githubusercontent.com/myui/190b91a3a792ccfceda0/raw/327acd192da4f96da8276dcdff01b19947a4373c/tfidf_test.tsv).

# Define views of TF/DF
```sql
create or replace view term_frequency 
as
select
  docid, 
  word,
  freq
from (
select
  docid,
  tf(word) as word2freq
from
  wikipage_exploded
group by
  docid
) t 
LATERAL VIEW explode(word2freq) t2 as word, freq;

create or replace view document_frequency
as
select
  word, 
  count(distinct docid) docs
from
  wikipage_exploded
group by
  word;
```

# TF-IDF calculation for each docid/word pair
```sql
-- set the total number of documents
select count(distinct docid) from wikipage;
set hivevar:n_docs=3;

create or replace view tfidf
as
select
  tf.docid,
  tf.word, 
  -- tf.freq * (log(10, CAST(${n_docs} as FLOAT)/max2(1,df.docs)) + 1.0) as tfidf
  tfidf(tf.freq, df.docs, ${n_docs}) as tfidf
from
  term_frequency tf 
  JOIN document_frequency df ON (tf.word = df.word)
order by 
  tfidf desc;
```

The result will be as follows:
```
docid  word     tfidf
1       justice 0.1641245850805637
3       knowledge       0.09484606645205085
2       action  0.07033910867777095
1       law     0.06564983513276658
1       found   0.06564983513276658
1       religion        0.06564983513276658
1       discussion      0.06564983513276658
  ...
  ...
2       act     0.017584777169442737
2       virtues 0.017584777169442737
2       well    0.017584777169442737
2       willingness     0.017584777169442737
2       find    0.017584777169442737
2       1       0.014001086678120098
2       experience      0.014001086678120098
2       often   0.014001086678120098
```
The above result is considered to be appropriate as docid 1, 2, and 3 are the Wikipedia entries of Justice, Wisdom, and Knowledge, respectively.

# Feature Vector with TF-IDF values

```sql
select
  docid, 
  -- collect_list(concat(word, ":", tfidf)) as features -- Hive 0.13 or later
  collect_list(feature(word, tfidf)) as features -- Hivemall v0.3.4 & Hive 0.13 or later
  -- collect_all(concat(word, ":", tfidf)) as features -- before Hive 0.13
from 
  tfidf
group by
  docid;
```

```
1       ["justice:0.1641245850805637","found:0.06564983513276658","discussion:0.06564983513276658","law:0.065
64983513276658","based:0.06564983513276658","religion:0.06564983513276658","viewpoints:0.03282491756638329","
rationality:0.03282491756638329","including:0.03282491756638329","context:0.03282491756638329","concept:0.032
82491756638329","rightness:0.03282491756638329","general:0.03282491756638329","many:0.03282491756638329","dif
fering:0.03282491756638329","fairness:0.03282491756638329","social:0.03282491756638329","broadest:0.032824917
56638329","equity:0.03282491756638329","includes:0.03282491756638329","theology:0.03282491756638329","ethics:
0.03282491756638329","moral:0.03282491756638329","numerous:0.03282491756638329","philosophical:0.032824917566
38329","application:0.03282491756638329","perspectives:0.03282491756638329","procedural:0.03282491756638329",
"realm:0.03282491756638329","divided:0.03282491756638329","concepts:0.03282491756638329","attainment:0.032824
91756638329","fields:0.03282491756638329","often:0.026135361945200226","philosophy:0.026135361945200226","stu
dy:0.026135361945200226"]
2       ["action:0.07033910867777095","wisdom:0.05275433288400458","one:0.05275433288400458","understanding:0
.04200326112968063","judgement:0.035169554338885474","apply:0.035169554338885474","disposition:0.035169554338
885474","given:0.035169554338885474"
...
```