Map-side join on Tez causes [ClassCastException](http://markmail.org/message/7cwbgupnhah6ggkv) when a serialized table contains array column(s).

[Workaround] Try setting _hive.mapjoin.optimized.hashtable_ off as follows:
```sql
set hive.mapjoin.optimized.hashtable=false;
```

Caution: Fixed in Hive 1.3.0. Refer [HIVE_11051](https://issues.apache.org/jira/browse/HIVE-11051) for the detail.