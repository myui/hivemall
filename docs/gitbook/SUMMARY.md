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
        
# Summary

## TABLE OF CONTENTS

* [Getting Started](getting_started/README.md)
    * [Installation](getting_started/installation.md)
    * [Install as permanent functions](getting_started/permanent-functions.md) 
    * [Input Format](getting_started/input-format.md)

* [Tips for Effective Hivemall](tips/README.md)
    * [Explicit addBias() for better prediction](tips/addbias.md)
    * [Use rand_amplify() to better prediction results](tips/rand_amplify.md)
    * [Real-time Prediction on RDBMS](tips/rt_prediction.md)
    * [Ensemble learning for stable prediction](tips/ensemble_learning.md)
    * [Mixing models for a better prediction convergence (MIX server)](tips/mixserver.md)
    * [Run Hivemall on Amazon Elastic MapReduce](tips/emr.md)

* [General Hive/Hadoop tips](tips/general_tips.md)
    * [Adding rowid for each row](tips/rowid.md)
    * [Hadoop tuning for Hivemall](tips/hadoop_tuning.md)

* [Troubleshooting](troubleshooting/README.md)
    * [OutOfMemoryError in training](troubleshooting/oom.md)
    * [SemanticException Generate Map Join Task Error: Cannot serialize object](troubleshooting/mapjoin_task_error.md)
    * [Asterisk argument for UDTF does not work](troubleshooting/asterisk.md)
    * [The number of mappers is less than input splits in Hadoop 2.x](troubleshooting/num_mappers.md)
    * [Map-side Join causes ClassCastException on Tez](troubleshooting/mapjoin_classcastex.md)

## Part II - Generic Features

* [List of generic Hivemall functions](misc/generic_funcs.md)
* [Efficient Top-K query processing](misc/topk.md)
* [English/Japanese Text Tokenizer](misc/tokenizer.md)

## Part III - Feature Engineering

* [Feature Scaling](ft_engineering/scaling.md)
* [Feature Hashing](ft_engineering/hashing.md)
* [TF-IDF calculation](ft_engineering/tfidf.md)

* [FEATURE TRANSFORMATION](ft_engineering/ft_trans.md)
    * [Vectorize Features](ft_engineering/vectorizer.md)
    * [Quantify non-number features](ft_engineering/quantify.md)

## Part IV - Evaluation

* [Statistical evaluation of a prediction model](eval/stat_eval.md)

* [Data Generation](eval/datagen.md)
    * [Logistic Regression data generation](eval/lr_datagen.md)

## Part V - Binary classification

* [a9a Tutorial](binaryclass/a9a.md)
    * [Data preparation](binaryclass/a9a_dataset.md)
    * [Logistic Regression](binaryclass/a9a_lr.md)
    * [Mini-batch Gradient Descent](binaryclass/a9a_minibatch.md)

* [News20 Tutorial](binaryclass/news20.md)
    * [Data preparation](binaryclass/news20_dataset.md)
    * [Perceptron, Passive Aggressive](binaryclass/news20_pa.md)
    * [CW, AROW, SCW](binaryclass/news20_scw.md)
    * [AdaGradRDA, AdaGrad, AdaDelta](binaryclass/news20_adagrad.md)

* [KDD2010a Tutorial](binaryclass/kdd2010a.md)
    * [Data preparation](binaryclass/kdd2010a_dataset.md)
    * [PA, CW, AROW, SCW](binaryclass/kdd2010a_scw.md)
    
* [KDD2010b Tutorial](binaryclass/kdd2010b.md)
    * [Data preparation](binaryclass/kdd2010b_dataset.md)
    * [AROW](binaryclass/kdd2010b_arow.md)

* [Webspam Tutorial](binaryclass/webspam.md)
    * [Data pareparation](binaryclass/webspam_dataset.md)
    * [PA1, AROW, SCW](binaryclass/webspam_scw.md)

* [Kaggle Titanic Tutorial](binaryclass/titanic_rf.md)
    
## Part VI - Multiclass classification

* [News20 Multiclass Tutorial](multiclass/news20.md)
    * [Data preparation](multiclass/news20_dataset.md)
    * [Data preparation for one-vs-the-rest classifiers](multiclass/news20_one-vs-the-rest_dataset.md)
    * [PA](multiclass/news20_pa.md)
    * [CW, AROW, SCW](multiclass/news20_scw.md)
    * [Ensemble learning](multiclass/news20_ensemble.md)
    * [one-vs-the-rest classifier](multiclass/news20_one-vs-the-rest.md)
    
* [Iris Tutorial](multiclass/iris.md)
    * [Data preparation](multiclass/iris_dataset.md)
    * [SCW](multiclass/iris_scw.md)
    * [RandomForest](multiclass/iris_randomforest.md)
    
## Part VII - Regression

* [E2006-tfidf regression Tutorial](regression/e2006.md)
    * [Data preparation](regression/e2006_dataset.md)
    * [Passive Aggressive, AROW](regression/e2006_arow.md)

* [KDDCup 2012 track 2 CTR prediction Tutorial](regression/kddcup12tr2.md)
    * [Data preparation](regression/kddcup12tr2_dataset.md)
    * [Logistic Regression, Passive Aggressive](regression/kddcup12tr2_lr.md)
    * [Logistic Regression with Amplifier](regression/kddcup12tr2_lr_amplify.md)
    * [AdaGrad, AdaDelta](regression/kddcup12tr2_adagrad.md)

## Part VIII - Recommendation

* [Collaborative Filtering](recommend/cf.md)
    * [Item-based Collaborative Filtering](recommend/item_based_cf.md)

* [News20 related article recommendation Tutorial](recommend/news20.md)
    * [Data preparation](multiclass/news20_dataset.md)
    * [LSH/Minhash and Jaccard Similarity](recommend/news20_jaccard.md)
    * [LSH/Minhash and Brute-Force Search](recommend/news20_knn.md)
    * [kNN search using b-Bits Minhash](recommend/news20_bbit_minhash.md)

* [MovieLens movie recommendation Tutorial](recommend/movielens.md)
    * [Data preparation](recommend/movielens_dataset.md)
    * [Matrix Factorization](recommend/movielens_mf.md)
    * [Factorization Machine](recommend/movielens_fm.md)
    * [10-fold Cross Validation (Matrix Factorization)](recommend/movielens_cv.md)    

## Part IX - Anomaly Detection

* [Outlier Detection using Local Outlier Factor (LOF)](anomaly/lof.md)

## Part X - External References

* [Hivemall on Apache Spark](https://github.com/maropu/hivemall-spark)
* [Hivemall on Apache Pig](https://github.com/daijyc/hivemall/wiki/PigHome)

