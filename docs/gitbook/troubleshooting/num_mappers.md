The default _hive.input.format_ is set to _org.apache.hadoop.hive.ql.io.CombineHiveInputFormat_.
This configuration could give less number of mappers than the split size (i.e., # blocks in HDFS) of the input table.

Try setting _org.apache.hadoop.hive.ql.io.HiveInputFormat_ for _hive.input.format_.
```
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
```

Note Apache Tez uses _org.apache.hadoop.hive.ql.io.HiveInputFormat_ by the default.
```
set hive.tez.input.format;
``` 
> hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat

***

You can then control the maximum number of mappers via setting:
```
set mapreduce.job.maps=128;
```