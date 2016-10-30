<!-- 
  Hivemall: Hive scalable Machine Learning Library
  
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
          http://www.apache.org/licenses/LICENSE-2.0
          
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

This tutorial uses [E2006 tfidf regression](https://github.com/myui/hivemall/wiki#e2006-tfidf-regression) as a reference.

Data preparation
--------------------
```
// Fetch training and test data
# wget http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/regression/E2006.train.bz2
# wget http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/regression/E2006.test.bz2

// Fetch a script to normalize the data
# wget https://raw.githubusercontent.com/maropu/hivemall-spark/master/scripts/misc/conv.awk
# bunzip2 -c E2006.train.bz2 | awk -f conv.awk > E2006.train.lp
# bunzip2 -c E2006.test.bz2 | awk -f conv.awk > E2006.test.lp

// Fetch an initialization script for hivemall-spark
# wget https://raw.githubusercontent.com/maropu/hivemall-spark/master/scripts/ddl/define-dfs.sh

// Invoke a spark-shell with hivemall-spark
# bin/spark-shell --packages maropu:hivemall-spark:0.0.6 --master=local-cluster[2,1,1024] --conf spark.executor.memory=1024

scala> :load define-dfs.sh

scala> :paste

// Load the training data as a RDD
val trainRdd = sc.textFile("E2006.train.lp")
  .map(HmLabeledPoint.parse)

// Create the DataFrame that has exactly 2 partitions and
// amplify the data by 3 times.
val trainDf = sqlContext.createDataFrame(trainRdd)
  .coalesce(2).part_amplify(3)

// Load the test data as a RDD
val testRdd = sc.textFile("E2006.test.lp")
  .map(HmLabeledPoint.parse)

// Transform into a DataFrame and transform features
// into a Spark Vector type.
val testDf = sqlContext.createDataFrame(testRdd)
  .select($"label".as("target"), ft2vec($"features").as("features"))
```

Training (PA1)
--------------------
```
// Make a model from the training data
val model = trainDf
  .train_pa1_regr(add_bias($"features"), $"label")
  .groupby("feature").agg("weight" -> "avg")
  .as("feature", "weight")

val modelUdf = HivemallUtils
  .funcModel(model)
```

Test
--------------------
```
// Do prediction
val predict = testDf
  .select($"target", modelUdf($"features").as("predicted"))
```

Evaluation
--------------------
```
predict
  .groupBy().agg(Map("target"->"avg", "predicted"->"avg"))
  .show()
```
