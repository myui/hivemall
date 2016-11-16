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

import com.google.common.base.Function;
import com.treasuredata.client.ExponentialBackOff;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDBulkImportSession;
import com.treasuredata.client.model.TDColumn;
import com.treasuredata.client.model.TDColumnType;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;
import com.treasuredata.client.model.TDTable;
import hivemall.systemtest.MsgpackConverter;
import hivemall.systemtest.exception.QueryExecutionException;
import hivemall.systemtest.model.CreateTableHQ;
import hivemall.systemtest.model.DropTableHQ;
import hivemall.systemtest.model.HQ;
import hivemall.systemtest.model.RawHQ;
import hivemall.systemtest.model.UploadFileAsNewTableHQ;
import hivemall.systemtest.model.UploadFileToExistingHQ;
import hivemall.systemtest.utils.IO;
import org.apache.commons.csv.CSVFormat;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TDSystemTestRunner extends SystemTestRunner {
    private TDClient client;

    private int execFinishRetryLimit = 7;
    private int fileUploadPerformRetryLimit = 7;
    private int fileUploadCommitBackOff = 5;
    private int fileUploadCommitRetryLimit = 7;

    public TDSystemTestRunner(final SystemTestCommonInfo ci, final String propertiesFile) {
        super(ci, propertiesFile);
    }

    public TDSystemTestRunner(final SystemTestCommonInfo ci) {
        super(ci, "td.properties");
    }

    @Override
    protected void initRunner() {
        // optional
        if (props.containsKey("execFinishRetryLimit")) {
            execFinishRetryLimit = Integer.valueOf(props.getProperty("execFinishRetryLimit"));
        }
        if (props.containsKey("fileUploadPerformRetryLimit")) {
            fileUploadPerformRetryLimit = Integer.valueOf(props.getProperty("fileUploadPerformRetryLimit"));
        }
        if (props.containsKey("fileUploadCommitBackOff")) {
            fileUploadCommitBackOff = Integer.valueOf(props.getProperty("fileUploadCommitBackOff"));
        }
        if (props.containsKey("fileUploadCommitRetryLimit")) {
            fileUploadCommitRetryLimit = Integer.valueOf(props.getProperty("fileUploadCommitRetryLimit"));
        }

        final Properties TDPorps = System.getProperties();
        for (Map.Entry<Object, Object> e : props.entrySet()) {
            if (e.getKey().toString().startsWith("td.client.")) {
                TDPorps.setProperty(e.getKey().toString(), e.getValue().toString());
            }
        }
        System.setProperties(TDPorps);

        client = System.getProperties().size() == TDPorps.size() ? TDClient.newClient() // use $HOME/.td/td.conf
                : TDClient.newBuilder(false).build(); // use *.properties
    }

    @Override
    protected void finRunner() {
        if (client != null) {
            client.close();
        }
    }

    @Override
    protected List<String> exec(@Nonnull final RawHQ hq) throws Exception {
        logger.info("executing: `" + hq.query + "`");

        final TDJobRequest req = TDJobRequest.newHiveQuery(dbName, hq.query);
        final String id = client.submit(req);

        final ExponentialBackOff backOff = new ExponentialBackOff();
        TDJobSummary job = client.jobStatus(id);
        int nRetries = 0;
        while (!job.getStatus().isFinished()) {
            if (nRetries > execFinishRetryLimit) {
                throw new Exception("Exceed standard of finish check retry repetition: "
                        + execFinishRetryLimit);
            }

            Thread.sleep(backOff.nextWaitTimeMillis());
            job = client.jobStatus(id);

            nRetries++;
        }

        return client.jobResult(id, TDResultFormat.TSV, new Function<InputStream, List<String>>() {
            @Override
            public List<String> apply(InputStream input) {
                final List<String> results = new ArrayList<String>();
                BufferedReader reader = null;
                try {
                    try {
                        reader = new BufferedReader(new InputStreamReader(input));
                        String line;
                        while ((line = reader.readLine()) != null) {
                            results.addAll(Arrays.asList(line.split(IO.RD)));
                        }
                    } finally {
                        if (reader != null) {
                            reader.close();
                        }
                    }
                } catch (IOException ex) {
                    throw new QueryExecutionException("Failed to read results from TD. "
                            + ex.getMessage());
                }
                return results;
            }
        });
    }

    @Override
    protected List<String> createDB(@Nonnull final String dbName) throws Exception {
        logger.info("executing: create database if not exists " + dbName);

        client.createDatabaseIfNotExists(dbName);
        return Collections.singletonList("created " + dbName);
    }

    @Override
    protected List<String> dropDB(@Nonnull final String dbName) throws Exception {
        logger.info("executing: drop database if exists " + dbName);

        client.deleteDatabaseIfExists(dbName);
        return Collections.singletonList("dropped " + dbName);
    }

    @Override
    protected List<String> use(@Nonnull final String dbName) throws Exception {
        return Collections.singletonList("No need to execute `USE` statement on TD, so skipped `USE "
                + dbName + "`");
    }

    @Override
    protected List<String> tableList() throws Exception {
        logger.info("executing: show tables on " + dbName);

        final List<TDTable> tables = client.listTables(dbName);
        final List<String> result = new ArrayList<String>();
        for (TDTable t : tables) {
            result.add(t.getName());
        }
        return result;
    }

    @Override
    protected List<String> createTable(@Nonnull final CreateTableHQ hq) throws Exception {
        logger.info("executing: create table " + hq.tableName + " if not exists on " + dbName);

        final List<TDColumn> columns = new ArrayList<TDColumn>();
        for (Map.Entry<String, String> e : hq.header.entrySet()) {
            columns.add(new TDColumn(e.getKey(), TDColumnType.parseColumnType(e.getValue())));
        }
        client.createTableIfNotExists(dbName, hq.tableName);
        client.updateTableSchema(dbName, hq.tableName, columns);
        return Collections.singletonList("created " + hq.tableName + " on " + dbName);
    }

    @Override
    protected List<String> dropTable(@Nonnull final DropTableHQ hq) throws Exception {
        logger.info("executing: drop table " + hq.tableName + " if exists on " + dbName);

        client.deleteTableIfExists(dbName, hq.tableName);
        return Collections.singletonList("dropped " + hq.tableName);
    }

    @Override
    protected List<String> uploadFileAsNewTable(@Nonnull final UploadFileAsNewTableHQ hq)
            throws Exception {
        logger.info("executing: create " + hq.tableName + " based on " + hq.file.getPath()
                + " if not exists on " + dbName);

        createTable(HQ.createTable(hq.tableName, hq.header));

        final String sessionName = "session-" + String.valueOf(System.currentTimeMillis());
        final String partName = "part-of-" + String.valueOf(sessionName);
        client.createBulkImportSession(sessionName, dbName, hq.tableName);

        try {
            // upload file as msgpack
            switch (hq.format) {
                case MSGPACK:
                    client.uploadBulkImportPart(sessionName, partName, hq.file);
                    break;
                case CSV: {
                    final File to = File.createTempFile(hq.file.getName(), ".msgpack.gz");
                    to.deleteOnExit();

                    client.uploadBulkImportPart(sessionName, partName,
                        new MsgpackConverter(hq.file, new ArrayList<String>(hq.header.keySet()),
                            CSVFormat.DEFAULT).asFile(to));
                    break;
                }
                case TSV: {
                    final File to = File.createTempFile(hq.file.getName(), ".msgpack.gz");
                    to.deleteOnExit();

                    client.uploadBulkImportPart(sessionName, partName,
                        new MsgpackConverter(hq.file, new ArrayList<String>(hq.header.keySet()),
                            CSVFormat.TDF).asFile(to));
                    break;
                }
                case UNKNOWN:
                    throw new Exception("Input msgpack.gz, csv or tsv");
            }

            client.freezeBulkImportSession(sessionName);
            client.performBulkImportSession(sessionName);
            final ExponentialBackOff backOff = new ExponentialBackOff();
            TDBulkImportSession session = client.getBulkImportSession(sessionName);
            int performNRetries = 0;
            while (session.getStatus() == TDBulkImportSession.ImportStatus.PERFORMING) {
                if (performNRetries > fileUploadPerformRetryLimit) {
                    throw new Exception("Exceed standard of perform check retry repetition: "
                            + fileUploadPerformRetryLimit);
                }

                logger.debug("Waiting bulk import completion");
                Thread.sleep(backOff.nextWaitTimeMillis());
                session = client.getBulkImportSession(sessionName);

                performNRetries++;
            }

            client.commitBulkImportSession(sessionName);
            session = client.getBulkImportSession(sessionName);
            int commitNRetries = 0;
            while (session.getStatus() != TDBulkImportSession.ImportStatus.COMMITTED) {
                if (commitNRetries > fileUploadCommitRetryLimit) {
                    throw new Exception("Exceed standard of commit check retry repetition: "
                            + fileUploadCommitRetryLimit);
                }

                logger.info("Waiting bulk import perform step completion");
                Thread.sleep(TimeUnit.SECONDS.toMillis(fileUploadCommitBackOff));
                session = client.getBulkImportSession(sessionName);

                commitNRetries++;
            }
        } finally {
            client.deleteBulkImportSession(sessionName);
        }

        return Collections.singletonList("uploaded " + hq.file.getName() + " into " + hq.tableName);
    }

    @Override
    protected List<String> uploadFileToExisting(@Nonnull final UploadFileToExistingHQ hq)
            throws Exception {
        logger.info("executing: insert " + hq.file.getPath() + " into " + hq.tableName + " on "
                + dbName);

        final String sessionName = "session-" + String.valueOf(System.currentTimeMillis());
        final String partName = "part-of-" + String.valueOf(sessionName);
        client.createBulkImportSession(sessionName, dbName, hq.tableName);

        try {
            // upload file as msgpack
            switch (hq.format) {
                case MSGPACK:
                    client.uploadBulkImportPart(sessionName, partName, hq.file);
                    break;
                case CSV: {
                    File to = File.createTempFile(hq.file.getName(), ".msgpack.gz");
                    to.deleteOnExit();
                    client.uploadBulkImportPart(sessionName, partName, new MsgpackConverter(
                        hq.file, getHeaderFromTD(hq.tableName), CSVFormat.DEFAULT).asFile(to));
                    break;
                }
                case TSV: {
                    File to = File.createTempFile(hq.file.getName(), ".msgpack.gz");
                    to.deleteOnExit();
                    client.uploadBulkImportPart(sessionName, partName, new MsgpackConverter(
                        hq.file, getHeaderFromTD(hq.tableName), CSVFormat.TDF).asFile(to));
                    break;
                }
                case UNKNOWN:
                    throw new Exception("Input msgpack.gz, csv or tsv");
            }

            client.freezeBulkImportSession(sessionName);
            client.performBulkImportSession(sessionName);
            final ExponentialBackOff backOff = new ExponentialBackOff();
            TDBulkImportSession session = client.getBulkImportSession(sessionName);
            while (session.getStatus() == TDBulkImportSession.ImportStatus.PERFORMING) {
                logger.debug("Waiting bulk import completion");
                Thread.sleep(backOff.nextWaitTimeMillis());
                session = client.getBulkImportSession(sessionName);
            }

            client.commitBulkImportSession(sessionName);
            session = client.getBulkImportSession(sessionName);
            while (session.getStatus() != TDBulkImportSession.ImportStatus.COMMITTED) {
                logger.info("Waiting bulk import perform step completion");
                Thread.sleep(TimeUnit.SECONDS.toMillis(fileUploadCommitBackOff));
                session = client.getBulkImportSession(sessionName);
            }
        } finally {
            client.deleteBulkImportSession(sessionName);
        }

        return Collections.singletonList("uploaded " + hq.file.getName() + " into " + hq.tableName);
    }

    private List<String> getHeaderFromTD(@Nonnull final String tableName) {
        final List<String> header = new ArrayList<String>();
        for (TDTable t : client.listTables(dbName)) {
            if (t.getName().equals(tableName)) {
                List<TDColumn> cols = t.getColumns();
                for (TDColumn col : cols) {
                    header.add(col.getName());
                }
                break;
            }
        }
        return header;
    }
}
