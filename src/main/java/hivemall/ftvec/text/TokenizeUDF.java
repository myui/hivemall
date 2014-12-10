/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
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
package hivemall.ftvec.text;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

@Description(name = "tokenize", value = "_FUNC_(input string) - Returns words in array<string>")
@UDFType(deterministic = true, stateful = false)
public final class TokenizeUDF extends UDF {
    private static final String DELIM = " .,?!:;()<>[]\b\t\n\f\r\"\'\\";

    public List<Text> evaluate(Text input) {
        return evaluate(input, false);
    }

    public List<Text> evaluate(Text input, boolean toLowerCase) {
        final List<Text> tokens = new ArrayList<Text>();
        final StringTokenizer tokenizer = new StringTokenizer(input.toString(), DELIM);
        while(tokenizer.hasMoreElements()) {
            String word = tokenizer.nextToken();
            if(toLowerCase) {
                word = word.toLowerCase();
            }
            tokens.add(new Text(word));
        }
        return tokens;
    }

}
