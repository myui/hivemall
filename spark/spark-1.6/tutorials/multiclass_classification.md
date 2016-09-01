This tutorial uses [news20 multiclass classification](https://github.com/myui/hivemall/wiki#news20-multiclass-classification) as a reference.

Data preparation
--------------------
```
// Fetch training and test data
# wget http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass/news20.scale.bz2
# wget http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass/news20.t.scale.bz2

// Fetch a script to normalize the data
# wget https://raw.githubusercontent.com/maropu/hivemall-spark/master/scripts/misc/conv.awk
# bunzip2 -c news20.scale.bz2 | awk -f conv.awk > news20.train
# bunzip2 -c news20.t.scale.bz2 | awk -f conv.awk > news20.test

// Fetch an initialization script for hivemall-spark
# wget https://raw.githubusercontent.com/maropu/hivemall-spark/master/scripts/ddl/define-dfs.sh

// Invoke a spark-shell with hivemall-spark
# bin/spark-shell --packages maropu:hivemall-spark:0.0.6 --master=local-cluster[2,1,1024] --conf spark.executor.memory=1024

scala> :load define-dfs.sh

scala> :paste

// Load the training data as a RDD
val trainRdd = sc.textFile("news20.train")
  .map(HmLabeledPoint.parse)

// Create the DataFrame that has exactly 2 partitions and
// amplify the data by 3 times.
val trainDf = sqlContext.createDataFrame(trainRdd)
  .coalesce(2).part_amplify(3)

// Load the test data as a RDD
val testRdd = sc.textFile("news20.test")
  .map(HmLabeledPoint.parse)

// Transform into a DataFrame and transform features
// into a Spark Vector type.
val testDf = sqlContext.createDataFrame(testRdd)
  .select(rowid(), $"label".cast(IntegerType).as("target"), $"features")
  .cache

val testDf_exploded = testDf.explode_array($"features")
  .select($"rowid", $"target", extract_feature($"feature"), extract_weight($"feature"))
```

Training (CW)
--------------------
```
// Make a model from the training data
val model = trainDf
  .train_multiclass_cw(add_bias($"features"), $"label".cast(IntegerType))
  .groupby("label", "feature").argmin_kld("weight", "conv")
  .as("label", "feature", "weight")
```

Test
--------------------
```
// Do prediction
val predict = testDf_exploded
  .join(model, testDf_exploded("feature") === model("feature"), "LEFT_OUTER")
  .select($"rowid", $"label".cast(StringType).as("label"), ($"weight" * $"value").as("value"))
  .groupby("rowid", "label").sum("value")
  .groupby("rowid").maxrow("SUM(value)", "label")
  .as("rowid", "r")
  .select($"rowid", $"r.col0".as("score"), $"r.col1".as("predict"))
  .cache
```

Evaluation
--------------------
```
val joinPredicate = (testDf("rowid") === predict("rowid")).and(testDf("target") === predict("predict"))
(testDf.join(predict, joinPredicate, "INNER").count + 0.0) / testDf.count
```
