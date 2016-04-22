create temporary macro max2(x DOUBLE, y DOUBLE) if(x>y,x,y);
create temporary macro min2(x DOUBLE, y DOUBLE) if(x<y,x,y);
create temporary macro rand_gid(k INT) floor(rand()*k);
create temporary macro rand_gid2(k INT, seed INT) floor(rand(seed)*k);
create temporary macro idf(df_t DOUBLE, n_docs DOUBLE) log(10, n_docs / max2(1,df_t)) + 1.0;
create temporary macro tfidf(tf FLOAT, df_t DOUBLE, n_docs DOUBLE) tf * (log(10, n_docs / max2(1,df_t)) + 1.0);
