Hivemall supports [Feature Hashing](https://github.com/myui/hivemall/wiki/Feature-hashing) (a.k.a. hashing trick) through `feature_hashing` and `mhash` functions. 
Find the differences in the following examples.

_Note: `feature_hashing` UDF is supported since Hivemall `v0.4.2-rc.1`._

## `feature_hashing` function

`feature_hashing` applies MurmuerHash3 hashing to features. 

```sql
select feature_hashing('aaa');
> 4063537

select feature_hashing('aaa','-features 3');
> 2

select feature_hashing(array('aaa','bbb'));
> ["4063537","8459207"]

select feature_hashing(array('aaa','bbb'),'-features 10');
> ["7","1"]

select feature_hashing(array('aaa:1.0','aaa','bbb:2.0'));
> ["4063537:1.0","4063537","8459207:2.0"]

select feature_hashing(array(1,2,3));
> ["11293631","3322224","4331412"]

select feature_hashing(array('1','2','3'));
> ["11293631","3322224","4331412"]

select feature_hashing(array('1:0.1','2:0.2','3:0.3'));
> ["11293631:0.1","3322224:0.2","4331412:0.3"]

select feature_hashing(features), features from training_fm limit 2;

> ["1803454","6630176"]   ["userid#5689","movieid#3072"]
> ["1828616","6238429"]   ["userid#4505","movieid#2331"]

select feature_hashing(array("userid#4505:3.3","movieid#2331:4.999", "movieid#2331"));

> ["1828616:3.3","6238429:4.999","6238429"]
```

_Note: The hash value is starting from 1 and 0 is system reserved for a bias clause. The default number of features are 16777217 (2^24). You can control the number of features by `-num_features` (or `-features`) option._

```sql
select feature_hashing(null,'-help');

usage: feature_hashing(array<string> features [, const string options]) -
       returns a hashed feature vector in array<string> [-features <arg>]
       [-help]
 -features,--num_features <arg>   The number of features [default:
                                  16777217 (2^24)]
 -help                            Show function help
```

## `mhash` function

```sql
describe function extended mhash;
> mhash(string word) returns a murmurhash3 INT value starting from 1
```

```sql

select mhash('aaa');
> 4063537
```

_Note: The default number of features are `16777216 (2^24)`._
```sql
set hivevar:num_features=16777216;

select mhash('aaa',${num_features});
>4063537
```

_Note: `mhash` returns a `+1'd` murmerhash3 value starting from 1. Never returns 0 (It's a system reserved number)._
```sql
set hivevar:num_features=1;

select mhash('aaa',${num_features});
> 1
```

_Note: `mhash` does not considers feature values._
```sql
select mhash('aaa:2.0');
> 2746618
```

_Note: `mhash` always returns a scalar INT value._
```sql
select mhash(array('aaa','bbb'));
> 9566153
```

_Note: `mhash` value of an array is element order-sentitive._
```sql
select mhash(array('bbb','aaa'));
> 3874068
```