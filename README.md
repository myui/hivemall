Hivemall: Hive scalable machine learning library
=================================================
[![Build Status](https://travis-ci.org/myui/hivemall.svg?branch=master)](https://travis-ci.org/myui/hivemall)
[![Coversity](https://scan.coverity.com/projects/4549/badge.svg)](https://scan.coverity.com/projects/4549)
[![Documentation Status](https://readthedocs.org/projects/hivemall-docs/badge/?version=latest)](https://readthedocs.org/projects/hivemall-docs/?badge=latest)
[![License](http://img.shields.io/:license-Apache_v2-blue.svg)](https://github.com/myui/hivemall/blob/master/LICENSE)
[![Coverage Status](https://coveralls.io/repos/github/myui/hivemall/badge.svg)](https://coveralls.io/github/myui/hivemall)

**News:** Hivemall joins [Apache Incubator](http://incubator.apache.org/projects/hivemall.html)! :tada: Currently in the process of moving the project repository to ASF.
[![Incubator](http://incubator.apache.org/images/egg-logo2.png "Apache Incubator")](http://incubator.apache.org/projects/hivemall.html)

Hivemall is a scalable machine learning library that runs on Apache Hive.
Hivemall is designed to be scalable to the number of training instances as well as the number of training features.

![logo](https://raw.github.com/myui/hivemall/master/resources/hivemall-logo-color-small.png "Hivemall's cute(!?) logo")

Supported Algorithms
--------------------

Hivemall provides machine learning functionality as well as feature engineering functions through UDFs/UDAFs/UDTFs of Hive. 

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

* Hive 0.12 or later

* Java 7 or later

  _Note: It would work for Java 6 except hivemall-nlp but we recommend to use Java 7 or later._

Basic Usage
------------

[![Hivemall](https://gist.githubusercontent.com/myui/d29241262f9313dec706/raw/caead313efd829b42a4a4183285e8b53cf26ab62/hadoopsummit14_slideshare.png)](http://www.slideshare.net/myui/hivemall-hadoop-summit-2014-san-jose)

Find more examples on [our wiki page](https://github.com/myui/hivemall/wiki/) and find a brief introduction to Hivemall in [this slide](http://www.slideshare.net/myui/hivemall-hadoop-summit-2014-san-jose).

Copyright
---------

```
Copyright (C) 2015-2016 Makoto YUI
Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)  
```

Put the above copyrights for the services/softwares that use Hivemall.

Support
-------

Support is through the [issue list](https://github.com/myui/hivemall/issues?q=label%3Aquestion), not by a direct e-mail. 

References
----------

Please refer the following paper for research uses:

* Makoto Yui. ``Hivemall: Scalable Machine Learning Library for Apache Hive'', [2014 Hadoop Summit](http://hadoopsummit.org/san-jose/), June 2014. \[[slide](http://www.slideshare.net/myui/hivemall-hadoop-summit-2014-san-jose)]

* Makoto Yui and Isao Kojima. ``Hivemall: Hive scalable machine learning library'' (demo), [NIPS 2013 Workshop on Machine Learning Open Source Software: Towards Open Workflows](https://mloss.org/workshop/nips13/), Dec 2013.

* Makoto Yui and Isao Kojima. ``A Database-Hadoop Hybrid Approach to Scalable Machine Learning'', Proc. IEEE 2nd International Congress on Big Data, July 2013 \[[paper](http://staff.aist.go.jp/m.yui/publications/bigdata2013myui.pdf)\] \[[slide](http://www.slideshare.net/myui/bigdata2013myui)\]

Awards
------

* [InfoWorld Bossie Awards 2014: The best open source big data tools](http://www.infoworld.com/article/2688074/big-data/big-data-164727-bossie-awards-2014-the-best-open-source-big-data-tools.html)

Acknowledgment
--------------

This work was supported in part by a JSPS grant-in-aid for young scientists (B) #24700111 and a JSPS grant-in-aid for scientific research (A) #24240015.
