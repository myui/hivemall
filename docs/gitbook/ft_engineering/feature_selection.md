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

Feature selection is the process which selects a subset consisting of influential features from miscellaneous ones.
It is an important technique to **enhance results**, **shorten training time** and **make features human-understandable**.

## Selecting methods supported by Hivemall
* Chi-square (Chi2)
    * For non-negative data only
* Signal Noise Ratio (SNR)
* ~~Minimum Redundancy Maximum Relevance (mRMR)~~
    * Contributions are welcome!

## Usage
1. Create importance list for feature selection
    * chi2/SNR
2. Filter features
    * Select top-k features based on importance list


## Example - Chi2
``` sql
CREATE TABLE input (
  X array<double>, -- features
  Y array<int> -- binarized label
);

WITH stats AS (
  SELECT
    -- [UDAF] transpose_and_dot(Y::array<number>, X::array<number>)::array<array<double>>
    transpose_and_dot(Y, X) AS observed, -- array<array<double>>, shape = (n_classes, n_features)
    array_sum(X) AS feature_count, -- n_features col vector, shape = (1, array<double>)
    array_avg(Y) AS class_prob -- n_class col vector, shape = (1, array<double>)
  FROM
    input
),
test AS (
  SELECT
    transpose_and_dot(class_prob, feature_count) AS expected -- array<array<double>>, shape = (n_class, n_features)
  FROM
    stats
),
chi2 AS (
  SELECT
    -- [UDAF] chi2(observed::array<array<double>>, expected::array<array<double>>)::struct<array<double>, array<double>>
    chi2(observed, expected) AS chi2s -- struct<array<double>, array<double>>, each shape = (1, n_features)
  FROM
    test JOIN stats;
)
SELECT
  -- [UDF] select_k_best(X::array<number>, importance_list::array<int> k::int)::array<double>
  select_k_best(X, chi2s.chi2, $[k}) -- top-k feature selection based on chi2 score
FROM
  input JOIN chi2;
```


## Example - SNR
``` sql
CREATE TABLE input (
  X array<double>, -- features
  Y array<int> -- binarized label
);

WITH snr AS (
  -- [UDAF] snr(features::array<number>, labels::array<int>)::array<double>
  SELECT snr(X, Y) AS snr FROM input -- aggregated SNR as array<double>, shape = (1, #features)
)
SELECT select_k_best(X, snr, ${k}) FROM input JOIN snr;
```


## UDF details
### Common
#### [UDAF] `transpose_and_dot(X::array<number>, Y::array<number>)::array<array<double>>`
##### Input

| array<number> X | array<number> Y |
| :-: | :-: |
| a row of matrix | a row of matrix |
##### Output

| array<array<double>> dotted |
| :-: |
| `dot(X.T, Y)`, shape = (X.#cols, Y.#cols) |
#### [UDF] `select_k_best(X::array<number>, importance_list::array<int> k::int)::array<double>`
##### Input

| array<number> X | array<int> importance list | int k |
| :-: | :-: | :-: |
| array | the larger, the more important | top-? |
##### Output

| array<array<double>> k-best elements |
| :-: |
| top-k elements from X based on indices of importance list |

#### Note
- Current implementation expects **_ALL each `importance_list` and `k` are equal**_. It maybe confuse us.
  - Future WA: add option showing use of common `importance_list` and `k`


### Chi2
#### [UDF] `chi2(observed::array<array<number>>, expected::array<array<number>>)::struct<array<double>, array<double>>`
##### Input

both `observed` and `expected`, shape = (#classes, #features)

| array<number> observed | array<number> expected |
| :-: | :-: |
| observed features | expected features, `dot(class_prob.T, feature_count)` |

##### Output

| struct<array<double>, array<double>> importance lists |
| :-: |
| chi2-values and p-values each feature, each shape = (1, #features) |


### SNR
#### [UDAF] `snr(X::array<number>, Y::array<int>)::array<double>`
##### Input

| array<number> X | array<int> Y |
| :-: | :-: |
| a row of matrix, overall shape = (#samples, #features) | a row of one-hot matrix, overall shape = (#samples, #classes) |

##### Output

| array<double> importance list |
| :-: |
| snr values of each feature, shape = (1, #features) |

#### Note
* Essentially, there is no need to one-hot vectorizing, but fitting its interface to chi2's one