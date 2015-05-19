/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hivemall.ftvec.hashing;

import static hivemall.utils.hadoop.WritableUtils.val;
import hivemall.utils.hashing.MurmurHash3;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.io.IntWritable;

public class MurmurHash3UDF extends UDF {

    public IntWritable evaluate(String word) throws UDFArgumentException {
        return evaluate(word, MurmurHash3.DEFAULT_NUM_FEATURES);
    }

    public IntWritable evaluate(String word, boolean rawValue) throws UDFArgumentException {
        if(rawValue) {
            if(word == null) {
                return null;
            }
            return val(MurmurHash3.murmurhash3_x86_32(word, 0, word.length(), 0x9747b28c));
        } else {
            return evaluate(word, MurmurHash3.DEFAULT_NUM_FEATURES);
        }
    }

    public IntWritable evaluate(String word, int numFeatures) throws UDFArgumentException {
        if(word == null) {
            return null;
        }
        int r = MurmurHash3.murmurhash3_x86_32(word, 0, word.length(), 0x9747b28c) % numFeatures;
        if(r < 0) {
            r += numFeatures;
        }
        return val(r + 1);
    }

    public IntWritable evaluate(List<String> words) throws UDFArgumentException {
        return evaluate(words, MurmurHash3.DEFAULT_NUM_FEATURES);
    }

    public IntWritable evaluate(List<String> words, int numFeatures) throws UDFArgumentException {
        if(words == null) {
            return null;
        }
        final int size = words.size();
        if(size == 0) {
            return val(1);
        }
        final StringBuilder b = new StringBuilder();
        b.append(words.get(0));
        for(int i = 1; i < size; i++) {
            b.append('\t');
            String v = words.get(i);
            b.append(v);
        }
        String s = b.toString();
        return evaluate(s, numFeatures);
    }

}
