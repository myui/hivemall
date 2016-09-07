This tutorial uses [9a binary classification](https://github.com/myui/hivemall/wiki#a9a-binary-classification) as a reference.

Data preparation
--------------------
```
// Fetch training data
# wget http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary/a9a
# wget http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary/a9a.t

// Fetch a script to normalize the data
# wget https://raw.githubusercontent.com/maropu/hivemall-spark/master/scripts/misc/conv.awk
# awk -f conv.awk a9a | sed -e "s/+1/1/" | sed -e "s/-1/0/" > a9a.train
# awk -f conv.awk a9a.t | sed -e "s/+1/1/" | sed -e "s/-1/0/" > a9a.test

// Fetch an initialization script for hivemall-spark
# wget https://raw.githubusercontent.com/maropu/hivemall-spark/master/scripts/ddl/define-dfs.sh

// Invoke a spark-shell with hivemall-spark
# bin/spark-shell --packages maropu:hivemall-spark:0.0.6 --master=local-cluster[2,1,1024] --conf spark.executor.memory=1024

scala> :load define-dfs.sh

scala> :paste

// Load the training data as a RDD
val trainRdd = sc.textFile("a9a.train")
  .map(HmLabeledPoint.parse)

// Create the DataFrame that has exactly 2 partitions and
// amplify the data by 3 times.
val trainDf = sqlContext.createDataFrame(trainRdd)
  .coalesce(2).part_amplify(3)

// Load the test data as a RDD
val testRdd = sc.textFile("a9a.test")
  .map(HmLabeledPoint.parse)

// Transform into a DataFrame and transform features
// into a Spark Vector type.
val testDf = sqlContext.createDataFrame(testRdd)
  .select($"label".as("target"), ft2vec($"features").as("features"))
```

Training (Logistic Regression)
--------------------
```
// Make a model from the training data
val model = trainDf
  .train_logregr(add_bias($"features"), $"label", "-total_steps 32561")
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
  .select($"target", sigmoid(modelUdf($"features")).as("prob"))
  .select($"target", when($"prob" > 0.50, 1.0).otherwise(0.0).as("predict"), $"prob")
  .cache
```

Evaluation
--------------------
```
(predict.where($"target" === $"predict").count + 0.0) / predict.count
```
