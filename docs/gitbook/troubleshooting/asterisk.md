See [HIVE-4181](https://issues.apache.org/jira/browse/HIVE-4181) that asterisk argument without table alias for UDTF is not working. It has been fixed as part of Hive v0.12 release.

A possible workaround is to use asterisk with a table alias, or to specify names of arguments explicitly.