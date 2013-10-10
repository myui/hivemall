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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Sha1UDF extends UDF {

    public static final int DEFAULT_NUM_FEATURES = 16777216;

    private final MessageDigest sha256;

    public Sha1UDF() {
        super();
        try {
            sha256 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public int evaluate(String word) {
        return evaluate(word, DEFAULT_NUM_FEATURES);
    }

    public int evaluate(String word, boolean rawValue) {
        if(rawValue) {
            return sha1(word);
        } else {
            return evaluate(word, DEFAULT_NUM_FEATURES);
        }
    }

    public int evaluate(String word, int numFeatures) {
        int r = sha1(word) % numFeatures;
        if(r < 0) {
            r += numFeatures;
        }
        return r;
    }

    public int evaluate(String... words) {
        if(words.length == 0) {
            return 0;
        }
        final StringBuilder b = new StringBuilder();
        b.append(words[0]);
        for(int i = 1; i < words.length; i++) {
            b.append('\t');
            b.append(words[i]);
        }
        String s = b.toString();
        return evaluate(s);
    }

    public int evaluate(String[] words, int numFeatures) {
        if(words.length == 0) {
            return 0;
        }
        final StringBuilder b = new StringBuilder();
        b.append(words[0]);
        for(int i = 1; i < words.length; i++) {
            b.append('\t');
            b.append(words[i]);
        }
        String s = b.toString();
        return evaluate(s, numFeatures);
    }

    private int sha1(final String word) {
        final MessageDigest md;
        try {
            md = (MessageDigest) sha256.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
        byte[] in = getBytes(word);
        byte[] di = md.digest(in);
        return getInt(di, 0);
    }

    private static byte[] getBytes(final String s) {
        final int len = s.length();
        final byte[] b = new byte[len * 2];
        for(int i = 0; i < len; i++) {
            putChar(b, i << 1, s.charAt(i));
        }
        return b;
    }

    private static void putChar(final byte[] b, final int off, final char val) {
        b[off + 1] = (byte) (val >>> 0);
        b[off] = (byte) (val >>> 8);
    }

    private static int getInt(final byte[] b, final int off) {
        return ((b[3] & 0xFF) << 24) | ((b[2] & 0xFF) << 16) | ((b[1] & 0xFF) << 8) | (b[0] & 0xFF);
    }
}
