# Introduction

<div class="alert alert-info">
Apache Hivemall is a collection of machine learning algorithms and versatile data analytics functions. It provides a number of ease of use machine learning functionalities through the <a href="https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF">Apache Hive UDF/UDAF/UDTF interface</a>.
</div>

<div style="text-align:center"><img src="resources/images/hivemall-logo-color-small.png"/></div>

Apache Hivemall offers a variety of functionalities: <strong>regression, classification, recommendation, anomaly detection, k-nearest neighbor, and feature engineering</strong>. It also supports state-of-the-art machine learning algorithms such as Soft Confidence Weighted, Adaptive Regularization of Weight Vectors, Factorization Machines, and AdaDelta. 

## Architecture

Apache Hivemall is mainly designed to run on [Apache Hive](https://hive.apache.org/) but it also supports [Apache Pig](https://pig.apache.org/) and [Apache Spark](http://spark.apache.org/) for the runtime.
Thus, it can be considered as a cross platform library for machine learning; prediction models built by a batch query of Apache Hive can be used on Apache Spark/Pig, and conversely, prediction models build by Apache Spark can be used from Apache Hive/Pig.

<div style="text-align:center"><img src="resources/images/techstack.png" width="80%" height="80%"/></div>

---

<font color="gray">
<sub>Apache Hivemall is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the <a href="http://incubator.apache.org/">Apache Incubator</a>.</sub>
</font>