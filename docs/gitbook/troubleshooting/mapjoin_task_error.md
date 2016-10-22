From Hive 0.11.0, **hive.auto.convert.join** is [enabled by the default](https://issues.apache.org/jira/browse/HIVE-3297).

When using complex queries using views, the auto conversion sometimes throws SemanticException, cannot serialize object.

Workaround for the exception is to disable **hive.auto.convert.join** before the execution as follows.
```
set hive.auto.convert.join=false;
```