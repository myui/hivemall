--
-- Hivemall: Hive scalable Machine Learning Library
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

create temporary macro max2(x DOUBLE, y DOUBLE) if(x>y,x,y);
create temporary macro min2(x DOUBLE, y DOUBLE) if(x<y,x,y);
create temporary macro rand_gid(k INT) floor(rand()*k);
create temporary macro rand_gid2(k INT, seed INT) floor(rand(seed)*k);
create temporary macro idf(df_t DOUBLE, n_docs DOUBLE) log(10, n_docs / max2(1,df_t)) + 1.0;
create temporary macro tfidf(tf FLOAT, df_t DOUBLE, n_docs DOUBLE) tf * (log(10, n_docs / max2(1,df_t)) + 1.0);
