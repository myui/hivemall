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
