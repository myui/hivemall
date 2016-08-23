package hivemall.model.lazy;

import com.klarna.hiverunner.CommandShellEmulation;
import com.klarna.hiverunner.sql.StatementsSplitter;
import hivemall.model.HQ;
import hivemall.model.RawHQ;
import hivemall.utils.IO;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LazyMatchingResource {
    private String fileName;
    private Charset charset;


    public LazyMatchingResource(String fileName, Charset charset) {
        this.fileName = fileName;
        this.charset = charset;
    }


    public List<RawHQ> toStrict(String caseDir) {
        String query = IO.getFromResourcePath(caseDir + fileName, charset);
        String formatted = CommandShellEmulation.HIVE_CLI.transformScript(query);
        List<String> split = StatementsSplitter.splitStatements(formatted);
        List<RawHQ> results = new ArrayList<RawHQ>();
        for (String q : split) {
            results.add(HQ.fromStatement(q));
        }
        return results;
    }

    public String[] getAnswers(String answerDir) {
        return IO.getFromResourcePath(answerDir + fileName).split(IO.QD);
    }
}
