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
package hivemall.systemtest.runner;

import com.klarna.hiverunner.CommandShellEmulation;
import com.klarna.hiverunner.Extractor;
import com.klarna.hiverunner.HiveServerContainer;
import com.klarna.hiverunner.HiveServerContext;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.builder.HiveShellBuilder;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import hivemall.systemtest.model.RawHQ;
import hivemall.systemtest.model.UploadFileToExistingHQ;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class HiveSystemTestRunner extends SystemTestRunner {
    private HiveServerContainer container;
    private TemporaryFolder tmpFolder;
    private HiveShell hShell;

    public HiveSystemTestRunner(final SystemTestCommonInfo ci, final String propertiesFile) {
        super(ci, propertiesFile);
    }

    public HiveSystemTestRunner(final SystemTestCommonInfo ci) {
        super(ci, "hiverunner.properties");
    }

    @Override
    void initRunner() {
        try {
            tmpFolder = new TemporaryFolder() {
                {
                    create();
                    getRoot().setWritable(true, false);
                }
            };
            final HiveRunnerConfig config = new HiveRunnerConfig() {
                {
                    // required
                    setHiveExecutionEngine(props.getProperty("hive.execution.engine", "mr"));

                    // optional
                    if (props.containsKey("enableTimeout")) {
                        setTimeoutEnabled(Boolean.valueOf(props.getProperty("enableTimeout")));
                    }
                    if (props.containsKey("timeoutRetryLimit")) {
                        setTimeoutRetries(Integer.valueOf(props.getProperty("timeoutRetryLimit")));
                    }
                    if (props.containsKey("timeoutSeconds")) {
                        setTimeoutSeconds(Integer.valueOf(props.getProperty("timeoutSeconds")));
                    }
                    if (props.containsKey("commandShellEmulation")) {
                        setCommandShellEmulation(CommandShellEmulation.valueOf(props.getProperty("commandShellEmulation")));
                    }
                }
            };
            final HiveServerContext ctx = Extractor.getStandaloneHiveServerContext(tmpFolder,
                config);
            container = Extractor.getHiveServerContainer(ctx);
            @SuppressWarnings("serial")
            final HiveShellBuilder builder = new HiveShellBuilder() {
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
        if (container != null) {
            container.tearDown();
        }
        if (tmpFolder != null) {
            tmpFolder.delete();
        }
    }

    @Override
    protected List<String> exec(@Nonnull final RawHQ hq) {
        logger.info("executing: `" + hq.query + "`");

        return hShell.executeQuery(hq.query);
    }

    @Override
    List<String> uploadFileToExisting(@Nonnull final UploadFileToExistingHQ hq) throws Exception {
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

        return Collections.singletonList("uploaded " + hq.file.getName() + " into " + hq.tableName);
    }
}
