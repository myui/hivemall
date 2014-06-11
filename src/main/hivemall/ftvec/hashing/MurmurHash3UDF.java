/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.ftvec.hashing;

import hivemall.utils.hashing.MurmurHash3;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

public class MurmurHash3UDF extends UDF {

    public int evaluate(String word) throws UDFArgumentException {
        return evaluate(word, MurmurHash3.DEFAULT_NUM_FEATURES);
    }

    public int evaluate(String word, boolean rawValue) throws UDFArgumentException {
        if(rawValue) {
            if(word == null) {
                throw new UDFArgumentException("argument must not be null");
            }
            return MurmurHash3.murmurhash3_x86_32(word, 0, word.length(), 0x9747b28c);
        } else {
            return evaluate(word, MurmurHash3.DEFAULT_NUM_FEATURES);
        }
    }

    public int evaluate(String word, int numFeatures) throws UDFArgumentException {
        if(word == null) {
            throw new UDFArgumentException("argument must not be null");
        }
        int r = MurmurHash3.murmurhash3_x86_32(word, 0, word.length(), 0x9747b28c) % numFeatures;
        if(r < 0) {
            r += numFeatures;
        }
        return r;
    }

    public int evaluate(List<String> words) throws UDFArgumentException {
        return evaluate(words, MurmurHash3.DEFAULT_NUM_FEATURES);
    }

    public int evaluate(List<String> words, int numFeatures) throws UDFArgumentException {
        if(words == null) {
            throw new UDFArgumentException("argument must not be null");
        }
        final int size = words.size();
        if(size == 0) {
            return 0;
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
