```sql
CREATE TABLE xxx
AS
SELECT 
  regexp_replace(reflect('java.util.UUID','randomUUID'), '-', '') as rowid,
  *
FROM
  ..;
```

Another option to generate rowid is to use row_number(). 
However, the query execution would become too slow for large dataset because the rowid generation is executed on a single reducer.
```sql
CREATE TABLE xxx
AS
select 
  row_number() over () as rowid, 
  * 
from a9atest;
```

***
# Rowid generator provided in Hivemall v0.2 or later
You can use [rowid() function](https://github.com/myui/hivemall/blob/master/src/main/java/hivemall/tools/mapred/RowIdUDF.java) to generate an unique rowid in Hivemall v0.2 or later.
```sql
select
  rowid() as rowid, -- returns ${task_id}-${sequence_number}
  *
from 
  xxx
```