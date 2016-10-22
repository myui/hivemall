`quantified_features` is useful for transforming values of non-number columns to indexed numbers.

*Note: The feature is supported Hivemall v0.4 or later.*

```sql
desc train;

id                      int                                         
age                     int                                         
job                     string                                      
marital                 string                                      
education               string                                      
default                 string                                      
balance                 int                                         
housing                 string                                      
loan                    string                                      
contact                 string                                      
day                     int                                         
month                   string                                      
duration                int                                         
campaign                int                                         
pdays                   int                                         
previous                int                                         
poutcome                string                                      
y                       int
```

```sql
select * from train limit 10;

1       39      blue-collar     married secondary       no      1756    yes     no      cellular        3       apr     939     1       -1      0       unknown 1
2       51      entrepreneur    married primary no      1443    no      no      cellular        18      feb     172     10      -1      0       unknown 1
3       36      management      single  tertiary        no      436     no      no      cellular        13      apr     567     1       595     2       failure 1
4       63      retired married secondary       no      474     no      no      cellular        25      jan     423     1       -1      0       unknown 1
5       31      management      single  tertiary        no      354     no      no      cellular        30      apr     502     1       9       2       success 1
6       29      blue-collar     single  secondary       no      260     yes     no      unknown 2       jun     707     14      -1      0       unknown 1
7       37      services        married secondary       no      52      yes     no      cellular        6       sep     908     1       185     9       success 1
8       32      technician      single  secondary       no      230     yes     no      cellular        18      may     442     1       266     8       failure 1
9       31      admin.  single  secondary       no      0       yes     no      cellular        7       may     895     2       295     2       failure 1
10      32      self-employed   single  tertiary        no      1815    no      no      telephone       10      aug     235     1       102     2       failure 1
```

```sql
set hivevar:output_row=true;

select quantify(${output_row}, *) 
from (
  select * from train
  order by id asc -- force quantify() runs on a single reducer
) t
limit 10;

1       39      0       0       0       0       1756    0       0       0       3       0       939     1       -1      0       0       1
2       51      1       0       1       0       1443    1       0       0       18      1       172     10      -1      0       0       1
3       36      2       1       2       0       436     1       0       0       13      0       567     1       595     2       1       1
4       63      3       0       0       0       474     1       0       0       25      2       423     1       -1      0       0       1
5       31      2       1       2       0       354     1       0       0       30      0       502     1       9       2       2       1
6       29      0       1       0       0       260     0       0       1       2       3       707     14      -1      0       0       1
7       37      4       0       0       0       52      0       0       0       6       4       908     1       185     9       2       1
8       32      5       1       0       0       230     0       0       0       18      5       442     1       266     8       1       1
9       31      6       1       0       0       0       0       0       0       7       5       895     2       295     2       1       1
10      32      7       1       2       0       1815    1       0       2       10      6       235     1       102     2       1       1
```

```sql
select 
  quantify(
    ${output_row}, id, age, job, marital, education, default, balance, housing, loan, contact, day, month, duration, campaign, cast(pdays as string), previous, poutcome, y
  ) as (id, age, job, marital, education, default, balance, housing, loan, contact, day, month, duration, campaign, pdays, previous, poutcome, y)
from (
  select * from train
  order by id asc
) t
limit 10;

1       39      0       0       0       0       1756    0       0       0       3       0       939     1       0       0       0       1
2       51      1       0       1       0       1443    1       0       0       18      1       172     10      0       0       0       1
3       36      2       1       2       0       436     1       0       0       13      0       567     1       1       2       1       1
4       63      3       0       0       0       474     1       0       0       25      2       423     1       0       0       0       1
5       31      2       1       2       0       354     1       0       0       30      0       502     1       2       2       2       1
6       29      0       1       0       0       260     0       0       1       2       3       707     14      0       0       0       1
7       37      4       0       0       0       52      0       0       0       6       4       908     1       3       9       2       1
8       32      5       1       0       0       230     0       0       0       18      5       442     1       4       8       1       1
9       31      6       1       0       0       0       0       0       0       7       5       895     2       5       2       1       1
10      32      7       1       2       0       1815    1       0       2       10      6       235     1       6       2       1       1
```

```sql
select 
  quantified_features(
    ${output_row}, id, age, job, marital, education, default, balance, housing, loan, contact, day, month, duration, campaign, cast(pdays as string), previous, poutcome, y
  ) as features
from (
  select * from train
  order by id asc
) t
limit 10;

[1.0,39.0,0.0,0.0,0.0,0.0,1756.0,0.0,0.0,0.0,3.0,0.0,939.0,1.0,0.0,0.0,0.0,1.0]
[2.0,51.0,1.0,0.0,1.0,0.0,1443.0,1.0,0.0,0.0,18.0,1.0,172.0,10.0,0.0,0.0,0.0,1.0]
[3.0,36.0,2.0,1.0,2.0,0.0,436.0,1.0,0.0,0.0,13.0,0.0,567.0,1.0,1.0,2.0,1.0,1.0]
[4.0,63.0,3.0,0.0,0.0,0.0,474.0,1.0,0.0,0.0,25.0,2.0,423.0,1.0,0.0,0.0,0.0,1.0]
[5.0,31.0,2.0,1.0,2.0,0.0,354.0,1.0,0.0,0.0,30.0,0.0,502.0,1.0,2.0,2.0,2.0,1.0]
[6.0,29.0,0.0,1.0,0.0,0.0,260.0,0.0,0.0,1.0,2.0,3.0,707.0,14.0,0.0,0.0,0.0,1.0]
[7.0,37.0,4.0,0.0,0.0,0.0,52.0,0.0,0.0,0.0,6.0,4.0,908.0,1.0,3.0,9.0,2.0,1.0]
[8.0,32.0,5.0,1.0,0.0,0.0,230.0,0.0,0.0,0.0,18.0,5.0,442.0,1.0,4.0,8.0,1.0,1.0]
[9.0,31.0,6.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,7.0,5.0,895.0,2.0,5.0,2.0,1.0,1.0]
[10.0,32.0,7.0,1.0,2.0,0.0,1815.0,1.0,0.0,2.0,10.0,6.0,235.0,1.0,6.0,2.0,1.0,1.0]
```

## Quantify test dataset 

```sql
select * from test limit 10;

1       30      management      single  tertiary        no      1028    no      no      cellular        4       feb     1294    2       -1      0       unknown
2       39      self-employed   single  tertiary        no      426     no      no      unknown 18      jun     1029    1       -1      0       unknown
3       38      technician      single  tertiary        no      -572    yes     yes     unknown 5       jun     26      24      -1      0       unknown
4       34      technician      single  secondary       no      -476    yes     no      unknown 27      may     92      4       -1      0       unknown
5       37      entrepreneur    married primary no      62      no      no      cellular        31      jul     404     2       -1      0       unknown
6       43      services        married primary no      574     yes     no      cellular        8       may     140     1       -1      0       unknown
7       54      technician      married secondary       no      324     yes     no      telephone       13      may     51      1       -1      0       unknown
8       41      blue-collar     married secondary       no      121     yes     no      cellular        13      may     16      6       176     5       other
9       52      housemaid       married primary no      1466    no      yes     cellular        20      nov     150     1       -1      0       unknown
10      32      management      married secondary       no      6217    yes     yes     cellular        18      nov     486     2       181     2       failure
```

```sql
select
  id, 
  array(age, job, marital, education, default, balance, housing, loan, contact, day, month, duration, campaign, pdays, previous, poutcome) as features
from (
  select 
    quantify(
      output_row, id, age, job, marital, education, default, balance, housing, loan, contact, day, month, duration, campaign, if(pdays==-1,0,pdays), previous, poutcome
    ) as (id, age, job, marital, education, default, balance, housing, loan, contact, day, month, duration, campaign, pdays, previous, poutcome)
  from (
    select * from (
      select
        1 as train_first, false as output_row, id, age, job, marital, education, default, balance, housing, loan, contact, day, month, duration, campaign, pdays, previous, poutcome
      from
        train
      union all
      select
        2 as train_first, true as output_row, id, age, job, marital, education, default, balance, housing, loan, contact, day, month, duration, campaign, pdays, previous, poutcome
      from
        test
    ) t0
    order by train_first, id asc
  ) t1
) t2
limit 10;

1       [30,2,1,2,0,1028,1,0,0,4,1,1294,2,0,0,0]
2       [39,7,1,2,0,426,1,0,1,18,3,1029,1,0,0,0]
3       [38,5,1,2,0,-572,0,1,1,5,3,26,24,0,0,0]
4       [34,5,1,0,0,-476,0,0,1,27,5,92,4,0,0,0]
5       [37,1,0,1,0,62,1,0,0,31,8,404,2,0,0,0]
6       [43,4,0,1,0,574,0,0,0,8,5,140,1,0,0,0]
7       [54,5,0,0,0,324,0,0,2,13,5,51,1,0,0,0]
8       [41,0,0,0,0,121,0,0,0,13,5,16,6,176,5,3]
9       [52,8,0,1,0,1466,1,1,0,20,9,150,1,0,0,0]
10      [32,2,0,0,0,6217,0,1,0,18,9,486,2,181,2,1]
```