Prerequisites
============

* Hive v0.12 or later
* Java 7 or later
* [hivemall-core-xxx-with-dependencies.jar](https://github.com/myui/hivemall/releases)
* [define-all.hive](https://github.com/myui/hivemall/releases)

Installation
============

Add the following two lines to your `$HOME/.hiverc` file.

```
add jar /home/myui/tmp/hivemall-core-xxx-with-dependencies.jar;
source /home/myui/tmp/define-all.hive;
```

This automatically loads all Hivemall functions every time you start a Hive session. Alternatively, you can run the following command each time.

```
$ hive
add jar /tmp/hivemall-core-xxx-with-dependencies.jar;
source /tmp/define-all.hive;
```