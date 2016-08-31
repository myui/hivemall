This tutorial uses [Kaggle Titanic binary classification](https://github.com/myui/hivemall/wiki/Kaggle-Titanic-binary-classification-using-Random-Forest) as a reference.

Data preparation
--------------------
```
// Fetch training and test data in Kaggle(https://www.kaggle.com/c/titanic/data), train.csv and test.csv

// Fetch an initialization script for hivemall-spark
# wget https://raw.githubusercontent.com/maropu/hivemall-spark/master/scripts/ddl/define-dfs.sh

// Invoke a spark-shell with spark-csv and hivemall-spark
# bin/spark-shell --packages com.databricks:spark-csv_2.10:1.4.0,maropu:hivemall-spark:0.0.6

scala> :load define-dfs.sh

scala> :paste

// Load the training data as a DataFrame
val trainCsvDf = sqlContext
  .read
  .format("com.databricks.spark.csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("train.csv")
  .cache // Cached for second use

val trainQuantifiedDf = trainCsvDf
  .quantify(true.as("output") +: trainCsvDf.cols: _*)
  // Rename output columns for readability
  .as("passengerid", "survived", "pclass", "name", "sex", "age", "sibsp", "parch", "ticket", "fare", "cabin", "embarked")
  .sort($"passengerid".asc)

val trainDf = trainQuantifiedDf
  .select(
      $"passengerid",
      array(trainQuantifiedDf.cols.drop(2): _*).as("features"),
      $"survived"
    )

// Load the test data as a DataFrame
val testCsvDf = sqlContext
  .read
  .format("com.databricks.spark.csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("test.csv")

val testQuantifiedDf = testCsvDf
  .select(Seq(1.as("train_first"), true.as("output"), $"PassengerId") ++ testCsvDf.cols.drop(1): _*)
  .unionAll(
      trainCsvDf.select(Seq(0.as("train_first"), false.as("output"), $"PassengerId") ++ trainCsvDf.cols.drop(2): _*)
    )
  .sort($"train_first".asc, $"PassengerId".asc)
  .quantify($"output" +: testCsvDf.cols: _*)
  // Rename output columns for readability
  .as("passengerid", "pclass", "name", "sex", "age", "sibsp", "parch", "ticket", "fare", "cabin", "embarked")

val testDf = testQuantifiedDf
  .select($"passengerid", array(testQuantifiedDf.cols.drop(1): _*).as("features"))
```

Training
--------------------
```
// Make a model from the training data
val model = trainDf
  .coalesce(4)
  .train_randomforest_classifier($"features", $"survived", "-trees 400")
```

Test
--------------------
```
// Do prediction
model
  .coalesce(4)
  .join(testDf)
  .select(
      testDf("passengerid"),
      tree_predict(model("model_id"), model("model_type"), model("pred_model"), testDf("features"), true).as("predicted")
    )
  .groupby($"passengerid").rf_ensemble("predicted")
  .as("passengerid", "predicted")
  .select($"passengerid", $"predicted.label")
  .show
```
