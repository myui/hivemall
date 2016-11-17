<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->
        
This page describes a list of useful Hivemall generic functions.

<!-- toc -->

# Array functions

## Array UDFs

- `array_concat(array<ANY> x1, array<ANY> x2, ..)` - Returns a concatenated array

    ```sql
    select array_concat(array(1),array(2,3));
    > [1,2,3]
    ```

- `array_intersect(array<ANY> x1, array<ANY> x2, ..)` - Returns an intersect of given arrays

    ```sql
    select array_intersect(array(1,3,4),array(2,3,4),array(3,5));
    > [3]
    ```

- `array_remove(array<int|text> original, int|text|array<int> target)` - Returns an array that the target is removed from the original array

    ```sql
    select array_remove(array(1,null,3),array(null));
    > [3]
    
    select array_remove(array("aaa","bbb"),"bbb");
    > ["aaa"]
    ```

- `sort_and_uniq_array(array<int>)` - Takes an array of type INT and returns a sorted array in a natural order with duplicate elements eliminated

    ```sql
    select sort_and_uniq_array(array(3,1,1,-2,10));
    > [-2,1,3,10]
    ```

- `subarray_endwith(array<int|text> original, int|text key)` - Returns an array that ends with the specified key
    
    ```sql
    select subarray_endwith(array(1,2,3,4), 3);
    > [1,2,3]
    ```

- `subarray_startwith(array<int|text> original, int|text key)` - Returns an array that starts with the specified key

    ```sql
    select subarray_startwith(array(1,2,3,4), 2);
    > [2,3,4]
    ```

- `subarray(array<int> orignal, int fromIndex, int toIndex)` - Returns a slice of the original array between the inclusive `fromIndex` and the exclusive `toIndex`

    ```sql
    select subarray(array(1,2,3,4,5,6), 2,4);
    > [3,4]
    ```

## Array UDAFs

- `array_avg(array<NUMBER>)` - Returns an array<double> in which each element is the mean of a set of numbers

- `array_sum(array<NUMBER>)` - Returns an array<double> in which each element is summed up

# Bitset functions

## Bitset UDF

- `to_bits(int[] indexes)` - Returns an bitset representation if the given indexes in long[]

    ```sql
    select to_bits(array(1,2,3,128));
    >[14,-9223372036854775808]
    ```

- `unbits(long[] bitset)` - Returns an long array of the give bitset representation

    ```sql
    select unbits(to_bits(array(1,4,2,3)));
    > [1,2,3,4]
    ```

- `bits_or(array<long> b1, array<long> b2, ..)` - Returns a logical OR given bitsets

    ```sql
    select unbits(bits_or(to_bits(array(1,4)),to_bits(array(2,3))));
    > [1,2,3,4]
    ```

## Bitset UDAF

- `bits_collect(int|long x)` - Returns a bitset in array<long>

# Compression functions

- `deflate(TEXT data [, const int compressionLevel])` - Returns a compressed BINARY object by using Deflater.
The compression level must be in range [-1,9]

    ```sql
    select base91(deflate('aaaaaaaaaaaaaaaabbbbccc'));
    > AA+=kaIM|WTt!+wbGAA
    ```

- `inflate(BINARY compressedData)` - Returns a decompressed STRING by using Inflater

    ```sql
    select inflate(unbase91(base91(deflate('aaaaaaaaaaaaaaaabbbbccc'))));
    > aaaaaaaaaaaaaaaabbbbccc
    ```

# Map functions

## Map UDFs

- `map_get_sum(map<int,float> src, array<int> keys)` - Returns sum of values that are retrieved by keys

- `map_tail_n(map SRC, int N)` - Returns the last N elements from a sorted array of SRC

## MAP UDAFs

- `to_map(key, value)` - Convert two aggregated columns into a key-value map

- `to_ordered_map(key, value [, const boolean reverseOrder=false])` - Convert two aggregated columns into an ordered key-value map


# MapReduce functions

- `rowid()` - Returns a generated row id of a form {TASK_ID}-{SEQUENCE_NUMBER}

- `taskid()` - Returns the value of mapred.task.partition

# Math functions

- `sigmoid(x)` - Returns `1.0 / (1.0 + exp(-x))`

# Text processing functions

- `base91(binary)` - Convert the argument from binary to a BASE91 string

    ```sql
    select base91(deflate('aaaaaaaaaaaaaaaabbbbccc'));
    > AA+=kaIM|WTt!+wbGAA
    ```

- `unbase91(string)` - Convert a BASE91 string to a binary

    ```sql
    select inflate(unbase91(base91(deflate('aaaaaaaaaaaaaaaabbbbccc'))));
    > aaaaaaaaaaaaaaaabbbbccc
    ```

- `normalize_unicode(string str [, string form])` - Transforms `str` with the specified normalization form. The `form` takes one of NFC (default), NFD, NFKC, or NFKD

    ```sql
    select normalize_unicode('ﾊﾝｶｸｶﾅ','NFKC');
    > ハンカクカナ
    
    select normalize_unicode('㈱㌧㌦Ⅲ','NFKC');
    > (株)トンドルIII
    ```

- `split_words(string query [, string regex])` - Returns an array<text> containing splitted strings

- `is_stopword(string word)` - Returns whether English stopword or not

- `tokenize(string englishText [, boolean toLowerCase])` - Returns words in array<string>

- `tokenize_ja(String line [, const string mode = "normal", const list<string> stopWords, const list<string> stopTags])` - returns tokenized strings in array<string>. Refer [this article](../misc/tokenizer.html) for detail.

    ```sql
    select tokenize_ja("kuromojiを使った分かち書きのテストです。第二引数にはnormal/search/extendedを指定できます。デフォルトではnormalモードです。");
    
    > ["kuromoji","使う","分かち書き","テスト","第","二","引数","normal","search","extended","指定","デフォルト","normal"," モード"]
    ```

# Other functions

- `convert_label(const int|const float)` - Convert from -1|1 to 0.0f|1.0f, or from 0.0f|1.0f to -1|1

- `each_top_k(int K, Object group, double cmpKey, *)` - Returns top-K values (or tail-K values when k is less than 0). Refer [this article](../misc/topk.html) for detail.

- `generate_series(const int|bigint start, const int|bigint end)` - Generate a series of values, from start to end

    ```sql
    select generate_series(1,9);
    
    1
    2
    3
    4
    5
    6
    7
    8
    9
    ```

    A similar function to PostgreSQL's `generate_serics`.
    http://www.postgresql.org/docs/current/static/functions-srf.html

- `x_rank(KEY)` - Generates a pseudo sequence number starting from 1 for each key
