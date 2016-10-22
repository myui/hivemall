This page explains how to apply [Mini-Batch Gradient Descent](https://class.coursera.org/ml-003/lecture/106) for the training of logistic regression explained in [this example](https://github.com/myui/hivemall/wiki/a9a-binary-classification-(logistic-regression)). 

See [this page](https://github.com/myui/hivemall/wiki/a9a-binary-classification-(logistic-regression)) first. This content depends on it.

# Training

Replace `a9a_model1` of [this example](https://github.com/myui/hivemall/wiki/a9a-binary-classification-(logistic-regression)).

```sql
set hivevar:total_steps=32561;
set hivevar:mini_batch_size=10;

create table a9a_model1 
as
select 
 cast(feature as int) as feature,
 avg(weight) as weight
from 
 (select 
     logress(addBias(features),label,"-total_steps ${total_steps} -mini_batch ${mini_batch_size}") as (feature,weight)
  from 
     a9atrain
 ) t 
group by feature;
```

# Evaluation

```sql
select count(1) / ${num_test_instances} from a9a_submit1 
where actual == predicted;
```


| Stochastic Gradient Descent | Minibatch Gradient Descent |
| ------------- | ------------- |
| 0.8430071862907684 | 0.8463239358761747 |