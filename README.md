Hivemall: Hive scalable machine learning library
=================================================

Hivemall is a scalable machine learning library running on Hive/Hadoop, licensed under the LGPL 2.1.  
Hivemall is designed to be scalable to the number of training instances as well as the number of training features.

Supported Algorithms
--------------------

Hivemall provides machine learning functionality as well as feature engineering functions 
through UDFs/UDAFs/UDTFs of Hive.

## Classfication

* Perceptron

* Passive Aggressive (PA1, PA2)

## Regression

* Logistic Regression using Stochastic Gradient Descent
  (parameter mixing and iterative parameter mixing)
  
* Passive Aggressive Regression (PA1, PA2)

## Ensemble

* Bagging

## Loss functions

* SquaredLoss, LogisticLoss, HingeLoss, SquaredHingeLoss, QuantileLoss, EpsilonInsensitiveLoss

## Feature engineering
  
* Feature hashing (MurmurHash, SHA1)

* Feature scaling (Min-Max Normalization, Z-Score)

* Feature instances amplifier that reduces iterations on training

* Bias clause

System requirements
--------------------

* Hive 0.9 or later

* Hadoop 0.20.x (CDH3 is our target)

Basic Usage
------------

```sql
SELECT 
  feature, 
  CAST(avg(weight) as FLOAT) as weight
FROM
 (SELECT logress(features,label) as (feature,weight) FROM training_features) t
GROUP BY feature;
```

Find more examples on [our wiki page](https://github.com/myui/hivemall/wiki/_pages).

License
---------

GNU Lesser General Public License 2.1  
http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html

According to LGPL 2.1, softwares that modifies/copies Hivemall MUST open their source codes upon requests.  
On the other hand, softwares that uses the Hivemall jar package without any modification are no need to show their sources.  
See GNU's [LGPL and Java page](http://www.gnu.org/licenses/lgpl-java.en.html) for detail.

If you want other (commercial) licensing options, please contact me.  
[Makoto YUI](http://staff.aist.go.jp/m.yui/index_e.html) \<m.yui AT aist.go.jp\>

Copyright
---------

Copyright (C) 2013 National Institute of Advanced Industrial Science and Technology (AIST)  
Registration Number: [H25PRO-1520](http://itri.aist-go.jp/section/knowledge.html)

Put the above copyrights and the registration number for the services/softwares that use Hivemall.


References
----------

Please refer the following paper for research uses:

* Makoto Yui and Isao Kojima. ``A Database-Hadoop Hybrid Approach to Scalable Machine Learning'',  
 Proc. IEEE 2nd International Congress on Big Data, July 2013 \[[paper](http://staff.aist.go.jp/m.yui/publications/bigdata2013myui.pdf)\] \[[slide](http://www.slideshare.net/myui/bigdata2013myui)\]

Acknowledgment
--------------

This work was supported in part by a JSPS grant-in-aid for young scientists (B) #24700111 and a JSPS grant-in-aid for scientific research (A) #24240015.
