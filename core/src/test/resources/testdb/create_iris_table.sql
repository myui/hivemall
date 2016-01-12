USE ${hiveconf:hivemall.schema};

CREATE EXTERNAL TABLE iris_subset (`time` int, rowid int, target int, features array<string>)
  ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    COLLECTION ITEMS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION '${hiveconf:LOCAL.HDFS.DIR}/irisdb/';