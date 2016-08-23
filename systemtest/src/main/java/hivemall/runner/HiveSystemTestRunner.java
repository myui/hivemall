package hivemall.runner;

import com.klarna.hiverunner.Extractor;
import com.klarna.hiverunner.HiveServerContainer;
import com.klarna.hiverunner.HiveServerContext;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.builder.HiveShellBuilder;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import hivemall.model.RawHQ;
import hivemall.model.UploadFileToExistingHQ;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HiveSystemTestRunner extends SystemTestRunner {
    private HiveServerContainer container;
    private TemporaryFolder tmpFolder;
    private HiveShell hShell;


    public HiveSystemTestRunner(SystemTestCommonInfo ci) {
        super(ci);
    }


    @Override
    protected void initRunner() {
        try {
            tmpFolder = new TemporaryFolder() {
                {
                    create();
                    getRoot().setWritable(true, false);
                }
            };
            final HiveRunnerConfig config = new HiveRunnerConfig() {
                {
                    setHiveExecutionEngine("mr");
                }
            };
            HiveServerContext ctx = Extractor.getStandaloneHiveServerContext(tmpFolder, config);
            container = Extractor.getHiveServerContainer(ctx);
            HiveShellBuilder builder = new HiveShellBuilder() {
                {
                    putAllProperties(new HashMap<String, String>() {
                        {
                            put("LOCAL.HDFS.DIR", "${hadoop.tmp.dir}");
                        }
                    });
                    setCommandShellEmulation(config.getCommandShellEmulation());
                    setHiveServerContainer(container);
                }
            };

            hShell = builder.buildShell();
            hShell.start();
        } catch (IOException ex) {
            throw new RuntimeException("Failed to init HiveRunner. " + ex.getMessage());
        }
    }

    @Override
    protected void finRunner() {
        if (container != null)
            container.tearDown();
        if (tmpFolder != null)
            tmpFolder.delete();
    }

    @Override
    protected List<String> exec(RawHQ hq) {
        logger.info("executing: `" + hq.get() + "`");

        return hShell.executeQuery(hq.get());
    }

    @Override
    List<String> uploadFileToExisting(final UploadFileToExistingHQ hq) throws Exception {
        logger.info("executing: insert " + hq.file.getPath() + " into " + hq.tableName + " on "
                + dbName);

        switch (hq.format) {
            case CSV:
                hShell.insertInto(dbName, hq.tableName)
                      .addRowsFromDelimited(hq.file, ",", null)
                      .commit();
                break;
            case TSV:
                hShell.insertInto(dbName, hq.tableName).addRowsFromTsv(hq.file).commit();
                break;
            case MSGPACK:
            case UNKNOWN:
                throw new Exception("Input csv or tsv");
        }

        return new ArrayList<String>() {
            {
                add("uploaded " + hq.file.getName() + " into " + hq.tableName);
            }
        };
    }
}
