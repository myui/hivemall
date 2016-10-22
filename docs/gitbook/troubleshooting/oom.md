# OOM in mappers

In a certain setting, the default input split size is too large for Hivemall. Due to that, OutOfMemoryError cloud happen on mappers in the middle of training.

Then, revise your a Hadoop setting (**mapred.child.java.opts**/**mapred.map.child.java.opts**) first to use a larger value as possible.

If an OOM error still caused after that, set smaller **mapred.max.split.size** value before training.
```
SET mapred.max.split.size=67108864;
```
Then, the number of training examples used for each trainer is reduced (as the number of mappers increases) and the trained model would fit in the memory.

# OOM in shuffle/merge

If OOM caused during the merge step, try setting a larger **mapred.reduce.tasks** value before training and revise [shuffle/reduce parameters](http://hadoop.apache.org/docs/r1.0.4/mapred_tutorial.html#Shuffle%2FReduce+Parameters).
```
SET mapred.reduce.tasks=64;
```

If your OOM happened by using amplify(), try using rand_amplify() instead.