/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.nlp.tokenizer;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.io.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ja.JapaneseAnalyzer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer.Mode;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;

@Description(
        name = "tokenize_ja",
        value = "_FUNC_(String line [, const string mode = \"normal\", const list<string> stopWords, const list<string> stopTags])"
                + " - returns tokenized strings in array<string>")
@UDFType(deterministic = true, stateful = false)
public final class KuromojiUDF extends GenericUDF {

    private Mode _mode;
    private String[] _stopWordsArray;
    private Set<String> _stoptags;

    // workaround to avoid org.apache.hive.com.esotericsoftware.kryo.KryoException: java.util.ConcurrentModificationException
    private transient JapaneseAnalyzer _analyzer;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        final int arglen = arguments.length;
        if (arglen < 1 || arglen > 4) {
            throw new UDFArgumentException("Invalid number of arguments for `tokenize_ja`: "
                    + arglen);
        }

        this._mode = (arglen >= 2) ? tokenizationMode(arguments[1]) : Mode.NORMAL;
        this._stopWordsArray = (arglen >= 3) ? HiveUtils.getConstStringArray(arguments[2]) : null;
        this._stoptags = (arglen >= 4) ? stopTags(arguments[3])
                : JapaneseAnalyzer.getDefaultStopTags();
        this._analyzer = null;

        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    @Override
    public List<Text> evaluate(DeferredObject[] arguments) throws HiveException {
        JapaneseAnalyzer analyzer = _analyzer;
        if (analyzer == null) {
            CharArraySet stopwords = stopWords(_stopWordsArray);
            analyzer = new JapaneseAnalyzer(null, _mode, stopwords, _stoptags);
            this._analyzer = analyzer;
        }

        Object arg0 = arguments[0].get();
        if (arg0 == null) {
            return null;
        }
        String line = arg0.toString();

        final List<Text> results = new ArrayList<Text>(32);
        TokenStream stream = null;
        try {
            stream = analyzer.tokenStream("", line);
            if (stream != null) {
                analyzeTokens(stream, results);
            }
        } catch (IOException e) {
            IOUtils.closeQuietly(analyzer);
            throw new HiveException(e);
        } finally {
            IOUtils.closeQuietly(stream);
        }
        return results;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(_analyzer);
    }

    @Nonnull
    private static Mode tokenizationMode(@Nonnull ObjectInspector oi) throws UDFArgumentException {
        final String arg = HiveUtils.getConstString(oi);
        if (arg == null) {
            return Mode.NORMAL;
        }
        final Mode mode;
        if ("NORMAL".equalsIgnoreCase(arg)) {
            mode = Mode.NORMAL;
        } else if ("SEARCH".equalsIgnoreCase(arg)) {
            mode = Mode.SEARCH;
        } else if ("EXTENDED".equalsIgnoreCase(arg)) {
            mode = Mode.EXTENDED;
        } else if ("DEFAULT".equalsIgnoreCase(arg)) {
            mode = JapaneseTokenizer.DEFAULT_MODE;
        } else {
            throw new UDFArgumentException(
                "Expected NORMAL|SEARCH|EXTENDED|DEFAULT but got an unexpected mode: " + arg);
        }
        return mode;
    }

    @Nonnull
    private static CharArraySet stopWords(@Nonnull final String[] array)
            throws UDFArgumentException {
        if (array == null) {
            return JapaneseAnalyzer.getDefaultStopSet();
        }
        if (array.length == 0) {
            return CharArraySet.EMPTY_SET;
        }
        CharArraySet results = new CharArraySet(Arrays.asList(array), /* ignoreCase */true);
        return results;
    }

    @Nonnull
    private static Set<String> stopTags(@Nonnull final ObjectInspector oi)
            throws UDFArgumentException {
        final String[] array = HiveUtils.getConstStringArray(oi);
        if (array == null) {
            return JapaneseAnalyzer.getDefaultStopTags();
        }
        final int length = array.length;
        if (length == 0) {
            return Collections.emptySet();
        }
        final Set<String> results = new HashSet<String>(length);
        for (int i = 0; i < length; i++) {
            String s = array[i];
            if (s != null) {
                results.add(s);
            }
        }
        return results;
    }

    private static void analyzeTokens(@Nonnull TokenStream stream, @Nonnull List<Text> results)
            throws IOException {
        // instantiate an attribute placeholder once
        CharTermAttribute termAttr = stream.getAttribute(CharTermAttribute.class);
        stream.reset();

        while (stream.incrementToken()) {
            String term = termAttr.toString();
            results.add(new Text(term));
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "tokenize_ja(" + Arrays.toString(children) + ')';
    }

}
