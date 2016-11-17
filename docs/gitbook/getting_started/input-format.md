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
        
This page explains the input format of training data in Hivemall. 
Here, we use [EBNF](http://en.wikipedia.org/wiki/Extended_Backus%E2%80%93Naur_Form)-like notation for describing the format.

<!-- toc -->

# Input Format for Classification 

The classifiers of Hivemall takes 2 (or 3) arguments: *features*, *label*, and *options* (a.k.a. [hyperparameters](http://en.wikipedia.org/wiki/Hyperparameter)). The first two arguments of training functions represents training examples. 

In Statistics, *features* and *label* are called [Explanatory variable and Response Variable](http://www.oswego.edu/~srp/stats/variable_types.htm), respectively.

# Features format (for classification and regression)

The format of *features* is common between (binary and multi-class) classification and regression.
Hivemall accepts `ARRAY&lt;INT|BIGINT|TEXT>` for the type of *features* column.

Hivemall uses a *sparse* data format (cf. [Compressed Row Storage](http://netlib.org/linalg/html_templates/node91.html)) which is similar to [LIBSVM](http://stackoverflow.com/questions/12112558/read-write-data-in-libsvm-format) and [Vowpal Wabbit](https://github.com/JohnLangford/vowpal_wabbit/wiki/Input-format).

The format of each feature in an array is as follows:
```
feature ::= <index>:<weight> or <index>
```

Each element of *index* or *weight* then accepts the following format:
```
index ::= <INT | BIGINT | TEXT>
weight ::= <FLOAT>
```

The *index* are usually a number (INT or BIGINT) starting from 1. 
Here is an instance of a features.
```
10:3.4  123:0.5  34567:0.231
```

*Note:* As mentioned later, *index* "0" is reserved for a [Bias/Dummy variable](../tips/addbias.html).

In addition to numbers, you can use a TEXT value for an index. For example, you can use array("height:1.5", "length:2.0") for the features.
```
"height:1.5" "length:2.0"
```

## Quantitative and Categorical variables

A [quantitative variable](http://www.oswego.edu/~srp/stats/variable_types.htm) must have an *index* entry.

Hivemall (v0.3.1 or later) provides *add_feature_index* function which is useful for adding indexes to quantitative variables. 

```sql
select add_feature_index(array(3,4.0,5)) from dual;
```
> ["1:3.0","2:4.0","3:5.0"]

You can omit specifying *weight* for each feature e.g. for [Categorical variables](http://www.oswego.edu/~srp/stats/variable_types.htm) as follows:
```
feature ::= <index>
```
Note 1.0 is used for the weight when omitting *weight*. 

## Bias/Dummy Variable in features

Note that "0" is reserved for a Bias variable (called dummy variable in Statistics). 

The [addBias](../tips/addbias.html) function is Hivemall appends "0:1.0" as an element of array in *features*.

## Feature hashing

Hivemall supports [feature hashing/hashing trick](http://en.wikipedia.org/wiki/Feature_hashing) through [mhash function](../ft_engineering/hashing.html#mhash-function).

The mhash function takes a feature (i.e., *index*) of TEXT format and generates a hash number of a range from 1 to 2^24 (=16777216) by the default setting.

Feature hashing is useful where the dimension of feature vector (i.e., the number of elements in *features*) is so large. Consider applying [mhash function]((../ft_engineering/hashing.html#mhash-function)) when a prediction model does not fit in memory and OutOfMemory exception happens.

In general, you don't need to use mhash when the dimension of feature vector is less than 16777216.
If feature *index* is very long TEXT (e.g., "xxxxxxx-yyyyyy-weight:55.3") and uses huge memory spaces, consider using mhash as follows:
```sql
-- feature is v0.3.2 or before
concat(mhash(extract_feature("xxxxxxx-yyyyyy-weight:55.3")), ":", extract_weight("xxxxxxx-yyyyyy-weight:55.3"))

-- feature is v0.3.2-1 or later
feature(mhash(extract_feature("xxxxxxx-yyyyyy-weight:55.3")), extract_weight("xxxxxxx-yyyyyy-weight:55.3"))
```
> 43352:55.3

## Feature Normalization

Feature (weight) normalization is important in machine learning. Please refer [this article](../ft_engineering/scaling.html) for detail.

***

# Label format in Binary Classification

The *label* must be an *INT* typed column and the values are positive (+1) or negative (-1) as follows:
```
<label> ::= 1 | -1
```

Alternatively, you can use the following format that represents 1 for a positive example and 0 for a negative example: 
```
<label> ::= 0 | 1
```

# Label format in Multi-class Classification

You can used any PRIMITIVE type in the multi-class *label*.  

```
<label> ::= <primitive type>
```

Typically, the type of label column will be INT, BIGINT, or TEXT.

***

# Input format in Regression

In regression, response/predictor variable (we denote it as *target*) is a real number.

Before Hivemall v0.3, we accepts only FLOAT type for *target*.
```
<target> ::= <FLOAT> 
```

You need to explicitly cast a double value of *target* to float as follows:
```sql
CAST(target as FLOAT) 
```

On the other hand, Hivemall v0.3 or later accepts double compatible numbers in *target*.
```
<target> ::= <FLOAT | DOUBLE | INT | TINYINT | SMALLINT| BIGINT > 
```

## Target in Logistic Regression

Logistic regression is actually a binary classification scheme while it can produce probabilities of positive of a training example. 

A *target* value of a training input must be in range 0.0 to 1.0, specifically 0.0 or 1.0.

***

# Helper functions

```sql
-- hivemall v0.3.2 and before
select concat("weight",":",55.0);

-- hivemall v0.3.2-1 and later
select feature("weight", 55.0);
```
> weight:55.0

```sql
select extract_feature("weight:55.0"), extract_weight("weight:55.0");
```
> weight | 55.0

```sql
-- hivemall v0.4.0 and later
select feature_index(array("10:0.2","7:0.3","9"));
```
> [10,7,9]

```sql
select 
  convert_label(-1), convert_label(1), convert_label(0.0f), convert_label(1.0f)
from 
  dual;
```
> 0.0f | 1.0f | -1 | 1

## Quantitative Features

`array<string> quantitative_features(array<string> featureNames, ...)` is a helper function to create sparse quantitative features from a table.

```sql
select quantitative_features(array("apple","value"),1,120.3);
```
> ["apple:1.0","value:120.3"]

## Categorical Features

`array<string> categorical_features(array<string> featureNames, ...)` is a helper function to create sparse categorical features from a table.

```sql
select categorical_features(
  array("is_cat","is_dog","is_lion","is_pengin","species"),
  1, 0, 1.0, true, "dog"
);
```
> ["is_cat#1","is_dog#0","is_lion#1.0","is_pengin#true","species#dog"]

## Preparing training data table 

You can create a training data table as follows:

```sql
select 
  rowid() as rowid,
  concat_array(
    array("bias:1.0"),
    categorical_features( 
      array("id", "name"),
      id, name
    ),
    quantitative_features(
      array("height", "weight"),
      height, weight
    )
  ) as features, 
  click_or_not as label
from
  table;
```