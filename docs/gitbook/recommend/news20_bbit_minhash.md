# Function Signature of bbit_minhash

```
Text bbit_minhash(array<int|string> features)
Text bbit_minhash(array<int|string> features, int numHashes=128)
Text bbit_minhash(array<int|string> features, boolean discardWeight=false)
Text bbit_minhash(array<int|string> features, int numHashes=128, boolean discardWeight=false)
```

# Create a signature for each article

```sql
create table new20mc_with_signature
as
select
  rowid, 
  bbit_minhash(features, false) as signature
from
  news20mc_train;
```

# kNN brute-force search using b-Bit minhash
```sql
set hivevar:topn=10;

select
  t1.rowid, 
  jaccard_similarity(t1.signature, q1.signature,128) as similarity
--  , popcnt(t1.signature, q1.signature) as popcnt
from
  new20mc_with_signature t1 
  CROSS JOIN 
  (select bbit_minhash(features,128,false) as signature from news20mc_test where rowid = 1) q1
order by
  similarity DESC
limit ${topn};
```

|rowid  | similarity | popcnt |
|:------|------------|-------:|
| 11952 | 0.390625   | 41 |
| 10748 | 0.359375   | 41 |
| 12902 | 0.34375    | 45 |
| 3087  | 0.328125   | 48 |
| 3     | 0.328125   | 37 |
| 11493 | 0.328125   | 38 |
| 3839  | 0.328125   | 41 |
| 12669 | 0.328125   | 37 |
| 13604 | 0.3125     | 41 |
| 6333  | 0.3125     | 39 |