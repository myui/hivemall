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

# Apache Hivemall Overview

Apache Hivemall is a scalable machine learning library that runs on Apache Hive/Pig/Spark. Apache Hivemall is designed to be scalable to the number of training instances as well as the number of training features.


Supported Algorithms
--------------------

Apache Hivemall provides machine learning functionality as well as feature engineering functions through UDFs/UDAFs/UDTFs of Hive. 

## Binary Classification

* [Perceptron](http://en.wikipedia.org/wiki/Perceptron)

* Passive Aggressive (PA, PA1, PA2)

* Confidence Weighted (CW)

* Adaptive Regularization of Weight Vectors (AROW)

* Soft Confidence Weighted (SCW1, SCW2)

* AdaGradRDA (w/ hinge loss)

* Factorization Machine (w/ logistic loss)

_My recommendation is AROW, SCW1, AdaGradRDA, and Factorization Machine while it depends._

## Multi-class Classification

* [Perceptron](http://en.wikipedia.org/wiki/Perceptron)

* Passive Aggressive (PA, PA1, PA2)

* Confidence Weighted (CW)

* Adaptive Regularization of Weight Vectors (AROW)

* Soft Confidence Weighted (SCW1, SCW2)

* Random Forest Classifier

* Gradient Tree Boosting (_Experimental_)

_My recommendation is AROW and SCW while it depends._

## Regression

* [Logistic Regression](http://en.wikipedia.org/wiki/Logistic_regression) using [Stochastic Gradient Descent](http://en.wikipedia.org/wiki/Stochastic_gradient_descent)

* AdaGrad, AdaDelta (w/ logistic Loss)
  
* Passive Aggressive Regression (PA1, PA2)

* AROW regression

* Random Forest Regressor

* Factorization Machine (w/ squared loss)

* [Polynomial Regression](http://en.wikipedia.org/wiki/Polynomial_regression)

_My recommendation for is AROW regression, AdaDelta, and Factorization Machine while it depends._

## Recommendation

* [Minhash](http://en.wikipedia.org/wiki/MinHash) ([LSH](http://en.wikipedia.org/wiki/Locality-sensitive_hashing) with jaccard index)

* [Matrix Factorization](http://en.wikipedia.org/wiki/Matrix_decomposition) (sgd, adagrad)

* Factorization Machine (squared loss for rating prediction)

## k-Nearest Neighbor

* [Minhash](http://en.wikipedia.org/wiki/MinHash) ([LSH](http://en.wikipedia.org/wiki/Locality-sensitive_hashing) with jaccard index)

* b-Bit minhash

* Brute-force search using Cosine similarity

## Anomaly Detection

* [Local Outlier Factor (LOF)](http://en.wikipedia.org/wiki/Local_outlier_factor)

## Natural Language Processing

* English/Japanese Text Tokenizer

## Feature engineering
  
* [Feature Hashing](http://en.wikipedia.org/wiki/Feature_hashing) (MurmurHash, SHA1)

* [Feature scaling](http://en.wikipedia.org/wiki/Feature_scaling) (Min-Max Normalization, Z-Score)

* [Polynomial Features](http://en.wikipedia.org/wiki/Polynomial_kernel)

* Feature instances amplifier that reduces iterations on training

* [TF-IDF](http://en.wikipedia.org/wiki/Tf%E2%80%93idf) vectorizer

* Bias clause

* Data generator for one-vs-the-rest classifiers

System requirements
--------------------

* Hive 0.13 or later

* Java 7 or later

* Spark 1.6 or 2.0 for Apache Hivemall on Spark

* Pig 0.15 or later for Apache Hivemall on Pig

More detail in [documentation](http://hivemall-docs.readthedocs.io/en/latest/).