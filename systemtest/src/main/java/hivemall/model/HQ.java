package hivemall.model;

import com.google.common.io.Resources;
import com.klarna.hiverunner.CommandShellEmulation;
import com.klarna.hiverunner.sql.StatementsSplitter;
import hivemall.model.lazy.LazyMatchingResource;
import hivemall.utils.lang.Preconditions;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

abstract public class HQ {
    public static List<RawHQ> fromStatements(String hq) {
        String formatted = CommandShellEmulation.HIVE_CLI.transformScript(hq);
        List<String> split = StatementsSplitter.splitStatements(formatted);
        List<RawHQ> results = new ArrayList<RawHQ>();
        for (String q : split) {
            results.add(new RawHQ(q));
        }
        return results;
    }

    public static RawHQ fromStatement(String hq) {
        String formatted = CommandShellEmulation.HIVE_CLI.transformScript(hq);
        List<String> split = StatementsSplitter.splitStatements(formatted);

        Preconditions.checkArgument(
            1 == split.size(),
            "Detected %d queries, should be exactly one. Use `HQ.fromStatements` for multi queries.",
            split.size());

        return new RawHQ(split.get(0));
    }

    public static LazyMatchingResource autoMatchingByFileName(String fileName, Charset charset) {
        return new LazyMatchingResource(fileName, charset);
    }

    public static LazyMatchingResource autoMatchingByFileName(String fileName) {
        return autoMatchingByFileName(fileName, Charset.defaultCharset());
    }

    public static List<RawHQ> fromResourcePath(String resourcePath, Charset charset) {
        return autoMatchingByFileName(resourcePath, charset).toStrict("");
    }

    public static List<RawHQ> fromResourcePath(String resourcePath) {
        return fromResourcePath(resourcePath, Charset.defaultCharset());
    }

    public static TableListHQ tableList() {
        return new TableListHQ();
    }

    public static CreateTableHQ createTable(String tableName, LinkedHashMap<String, String> header) {
        return new CreateTableHQ(tableName, header);
    }

    public static DropTableHQ dropTable(String tableName) {
        return new DropTableHQ(tableName);
    }

    public static InsertHQ insert(String tableName, List<String> header, List<Object[]> data) {
        return new InsertHQ(tableName, header, data);
    }

    public static UploadFileToExistingHQ uploadByFullPathToExisting(String tableName,
            String fullPath) {
        return new UploadFileToExistingHQ(tableName, new File(fullPath));
    }

    public static UploadFileToExistingHQ uploadByResourcePathToExisting(String tableName,
            String resourcePath) {
        return uploadByFullPathToExisting(tableName, Resources.getResource(resourcePath).getPath());
    }

    public static UploadFileAsNewTableHQ uploadByFullPathAsNewTable(String tableName,
            String fullPath, LinkedHashMap<String, String> header) {
        return new UploadFileAsNewTableHQ(tableName, new File(fullPath), header);
    }

    public static UploadFileAsNewTableHQ uploadByResourcePathAsNewTable(String tableName,
            String resourcePath, LinkedHashMap<String, String> header) {
        return uploadByFullPathAsNewTable(tableName, Resources.getResource(resourcePath).getPath(),
            header);
    }
}
