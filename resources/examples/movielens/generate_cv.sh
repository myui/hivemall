#!/bin/bash

nfolds=10

evalsql=""
cat /dev/null > generate_cv.sql

echo "-- ${nfolds}-folds cross validation" >> generate_cv.sql
for i in `seq 1 ${nfolds}`;
do
  cat >> generate_cv.sql <<EOF

create or replace view training${i}
as
select 
  userid, movieid, rating
from 
  ratings_groupded
where
  gid != ${i};

create or replace view testing${i}
as
select 
  userid,
  movieid,
  rating
from 
  ratings_groupded
where
  gid = ${i};

create or replace view model${i}
as
select
  idx, 
  array_avg(u_rank) as Pu, 
  array_avg(m_rank) as Qi, 
  avg(u_bias) as Bu, 
  avg(m_bias) as Bi
from (
  select 
    train_mf_sgd(userid, movieid, rating, "-factor \${factor} -mu \${mu} -iterations \${iters} -cv_rate \${cv_rate} -lambda \${lambda} -eta \${eta}") as (idx, u_rank, m_rank, u_bias, m_bias)
  from 
    training${i}
) t
group by idx;
EOF

  if [ ${i} -ne 1 ]; then
    evalsql=${evalsql}$'\nUNION ALL\n'
  fi

  evalsql=`cat <<EOF
${evalsql}select
  t2.actual,
  mf_predict(t2.Pu, p2.Qi, t2.Bu, p2.Bi, \\${mu}) as predicted
from (
  select
    t1.userid, 
    t1.movieid,
    t1.rating as actual,
    p1.Pu,
    p1.Bu
  from
    testing${i} t1 LEFT OUTER JOIN model${i} p1
    ON (t1.userid = p1.idx) 
) t2 
LEFT OUTER JOIN model${i} p2
ON (t2.movieid = p2.idx)
EOF`
done

echo -e "\n-- evaluation " >> generate_cv.sql

cat >> generate_cv.sql <<EOF
select
  mae(predicted, actual) as mae,
  rmse(predicted, actual) as rmse
from (
  ${evalsql}
) t;
EOF
