Hivemall: Hive scalable machine learning library
=================================================
[![Build Status](https://travis-ci.org/myui/hivemall.svg?branch=master)](https://travis-ci.org/myui/hivemall)
[![License](http://img.shields.io/:license-apache_v2-blue.svg)](https://github.com/myui/hivemall/blob/master/LICENSE)

Hivemall is a scalable machine learning library that runs on Apache Hive, licensed under the LGPL 2.1.
Hivemall is designed to be scalable to the number of training instances as well as the number of training features.

![logo](https://raw.github.com/myui/hivemall/master/resources/hivemall-logo_s.png "Hivemall's cute(!?) logo")

Supported Algorithms
--------------------

Hivemall provides machine learning functionality as well as feature engineering functions 
through UDFs/UDAFs/UDTFs of Hive. 

## Classfication

* Perceptron

* Passive Aggressive (PA, PA1, PA2)

* Confidence Weighted (CW)

* Adaptive Regularization of Weight Vectors (AROW)

* Soft Confidence Weighted (SCW1, SCW2)

* AdaGradRDA (with hinge loss)

_My recommendation is AROW, SCW1 and AdaGradRDA, while it depends._

## Regression

* Logistic Regression using Stochastic Gradient Descent

* AdaGrad / AdaDelta (with logistic loss)
  
* Passive Aggressive Regression (PA1, PA2)

* AROW regression

_My recommendation is AdaDelta and AdaGrad, while it depends._

## Recommendation

* Matrix Factorization (sgd, adagrad)

* Minhash (LSH with jaccard index)

## k-Nearest Neighbor

* Minhash (LSH with jaccard index)

* b-Bit minhash

* Brute-force search using cosine similarity

## Feature engineering
  
* Feature hashing (MurmurHash, SHA1)

* Feature scaling (Min-Max Normalization, Z-Score)

* Feature instances amplifier that reduces iterations on training

* TF-IDF vectorizer

* Bias clause

* Data generator for one-vs-the-rest classifiers

System requirements
--------------------

* Hive 0.11 or later

* Hive 0.9, 0.10 [out of support]

Basic Usage
------------

[![Hivemall](https://gist.githubusercontent.com/myui/d29241262f9313dec706/raw/caead313efd829b42a4a4183285e8b53cf26ab62/hadoopsummit14_slideshare.png)](http://www.slideshare.net/myui/hivemall-hadoop-summit-2014-san-jose)

Find more examples on [our wiki page](https://github.com/myui/hivemall/wiki/) and find a brief introduction to Hivemall in [this slide](http://www.slideshare.net/myui/hivemall-hadoop-summit-2014-san-jose).

Copyright
---------

Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)  

Put the above copyrights for the services/softwares that use Hivemall.

Support
-------

Support is through the [issue list](https://github.com/myui/hivemall/issues), not by a direct e-mail. Put a [question label](https://github.com/myui/hivemall/labels/question) to ask a question.

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

[![Analytics](https://ga-beacon.appspot.com/UA-104966-13/myui/hivemall)](https://github.com/myui/hivemall)
