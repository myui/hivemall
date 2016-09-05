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
import hivemall.systemtest.ConvertToMsgpack;
import hivemall.systemtest.model.CreateTableHQ;
import hivemall.systemtest.model.DropTableHQ;
import hivemall.systemtest.model.HQ;
import hivemall.systemtest.model.InsertHQ;
import hivemall.systemtest.model.RawHQ;
import hivemall.systemtest.model.UploadFileAsNewTableHQ;
import hivemall.systemtest.model.UploadFileToExistingHQ;
import hivemall.systemtest.utils.IO;
import org.apache.commons.csv.CSVFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TDSystemTestRunner extends SystemTestRunner {
    private TDClient client;


    public TDSystemTestRunner(SystemTestCommonInfo ci) {
        super(ci);
    }


    @Override
    String formatDBName(String name) {
        return name.toLowerCase();
    }

    @Override
    protected void initRunner() {
        client = TDClient.newClient();
    }

    @Override
    protected void finRunner() {
        if (client != null)
            client.close();
    }

    @Override
    protected List<String> exec(RawHQ hq) throws Exception {
        logger.info("executing: `" + hq.get() + "`");

        TDJobRequest req = TDJobRequest.newHiveQuery(dbName, hq.get());
        String id = client.submit(req);

        ExponentialBackOff backOff = new ExponentialBackOff();
        TDJobSummary job = client.jobStatus(id);
        while (!job.getStatus().isFinished()) {
            Thread.sleep(backOff.nextWaitTimeMillis());
            job = client.jobStatus(id);
        }

        return client.jobResult(id, TDResultFormat.TSV, new Function<InputStream, List<String>>() {
            @Override
            public List<String> apply(InputStream input) {
                List<String> results = new ArrayList<String>();
                BufferedReader reader = null;
                try {
                    try {
                        reader = new BufferedReader(new InputStreamReader(input));
                        String line;
                        while ((line = reader.readLine()) != null) {
                            results.addAll(Arrays.asList(line.split(IO.RD)));
                        }
                    } finally {
                        if (reader != null)
                            reader.close();
                    }
                } catch (IOException ex) {
                    throw new RuntimeException("Failed to read results from TD. " + ex.getMessage());
                }
                return results;
            }
        });
    }

    @Override
    protected List<String> createDB(final String dbName) throws Exception {
        logger.info("executing: create database if not exists " + dbName);

        client.createDatabaseIfNotExists(dbName);
        return new ArrayList<String>() {
            {
                add("created " + dbName);
            }
        };
    }

    @Override
    protected List<String> dropDB(final String dbName) throws Exception {
        logger.info("executing: drop database if exists " + dbName);

        client.deleteDatabaseIfExists(dbName);
        return new ArrayList<String>() {
            {
                add("dropped " + dbName);
            }
        };
    }

    @Override
    protected List<String> use(final String dbName) throws Exception {
        return new ArrayList<String>() {
            {
                add("No need to execute `USE` statement on TD, so skipped `USE " + dbName + "`");
            }
        };
    }

    @Override
    protected List<String> tableList() throws Exception {
        logger.info("executing: show tables on " + dbName);

        List<TDTable> tables = client.listTables(dbName);
        List<String> result = new ArrayList<String>();
        for (TDTable t : tables) {
            result.add(t.getName());
        }
        return result;
    }

    @Override
    protected List<String> createTable(final CreateTableHQ hq) throws Exception {
        logger.info("executing: create table " + hq.tableName + " if not exists on " + dbName);

        List<TDColumn> columns = new ArrayList<TDColumn>();
        for (Map.Entry<String, String> e : hq.header.entrySet()) {
            columns.add(new TDColumn(e.getKey(), TDColumnType.parseColumnType(e.getValue())));
        }
        client.createTableIfNotExists(dbName, hq.tableName);
        client.updateTableSchema(dbName, hq.tableName, columns);
        return new ArrayList<String>() {
            {
                add("created " + hq.tableName + " on " + dbName);
            }
        };
    }

    @Override
    protected List<String> dropTable(final DropTableHQ hq) throws Exception {
        logger.info("executing: drop table " + hq.tableName + " if exists on " + dbName);

        client.deleteTableIfExists(dbName, hq.tableName);
        return new ArrayList<String>() {
            {
                add("dropped " + hq.tableName);
            }
        };
    }

    @Override
    // for Hive 0.13.0 or lower
    // if on 0.14.0 or later, doesn't need following
    protected List<String> insert(InsertHQ hq) throws Exception {
        logger.info("executing: insert into " + hq.tableName + " on " + dbName);

        // construct statement based on WITH clause
        StringBuilder sb = new StringBuilder();
        sb.append("WITH t AS (");
        for (Object[] row : hq.data) {
            sb.append("SELECT ");
            for (int i = 0; i < hq.header.size(); i++) {
                sb.append(InsertHQ.serialize(row[i]));
                sb.append(" ");
                sb.append(hq.header.get(i));
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(" UNION ALL ");
        }
        sb.delete(sb.length() - 11, sb.length());
        sb.append(") INSERT INTO TABLE ");
        sb.append(hq.tableName);
        sb.append(" SELECT * FROM t");

        return exec(HQ.fromStatement(sb.toString()));
    }

    @Override
    protected List<String> uploadFileAsNewTable(final UploadFileAsNewTableHQ hq) throws Exception {
        logger.info("executing: create " + hq.tableName + " based on " + hq.file.getPath()
                + " if not exists on " + dbName);

        createTable(HQ.createTable(hq.tableName, hq.header));

        String sessionName = "session-" + String.valueOf(System.currentTimeMillis());
        String partName = "part-of-" + String.valueOf(sessionName);
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

                    client.uploadBulkImportPart(sessionName, partName,
                        new ConvertToMsgpack(hq.file, new ArrayList<String>(hq.header.keySet()),
                            CSVFormat.DEFAULT).asFile(to));
                    break;
                }
                case TSV: {
                    File to = File.createTempFile(hq.file.getName(), ".msgpack.gz");
                    to.deleteOnExit();

                    client.uploadBulkImportPart(sessionName, partName,
                        new ConvertToMsgpack(hq.file, new ArrayList<String>(hq.header.keySet()),
                            CSVFormat.TDF).asFile(to));
                    break;
                }
                case UNKNOWN:
                    throw new Exception("Input msgpack.gz, csv or tsv");
            }

            client.freezeBulkImportSession(sessionName);
            client.performBulkImportSession(sessionName);
            ExponentialBackOff backOff = new ExponentialBackOff();
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
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
                session = client.getBulkImportSession(sessionName);
            }
        } finally {
            client.deleteBulkImportSession(sessionName);
        }

        return new ArrayList<String>() {
            {
                add("uploaded " + hq.file.getName() + " into " + hq.tableName);
            }
        };
    }

    @Override
    protected List<String> uploadFileToExisting(final UploadFileToExistingHQ hq) throws Exception {
        logger.info("executing: insert " + hq.file.getPath() + " into " + hq.tableName + " on "
                + dbName);

        String sessionName = "session-" + String.valueOf(System.currentTimeMillis());
        String partName = "part-of-" + String.valueOf(sessionName);
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
                    client.uploadBulkImportPart(sessionName, partName, new ConvertToMsgpack(
                        hq.file, getHeaderFromTD(hq.tableName), CSVFormat.DEFAULT).asFile(to));
                    break;
                }
                case TSV: {
                    File to = File.createTempFile(hq.file.getName(), ".msgpack.gz");
                    to.deleteOnExit();
                    client.uploadBulkImportPart(sessionName, partName, new ConvertToMsgpack(
                        hq.file, getHeaderFromTD(hq.tableName), CSVFormat.TDF).asFile(to));
                    break;
                }
                case UNKNOWN:
                    throw new Exception("Input msgpack.gz, csv or tsv");
            }

            client.freezeBulkImportSession(sessionName);
            client.performBulkImportSession(sessionName);
            ExponentialBackOff backOff = new ExponentialBackOff();
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
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
                session = client.getBulkImportSession(sessionName);
            }
        } finally {
            client.deleteBulkImportSession(sessionName);
        }

        return new ArrayList<String>() {
            {
                add("uploaded " + hq.file.getName() + " into " + hq.tableName);
            }
        };
    }

    private List<String> getHeaderFromTD(String tableName) {
        List<String> header = new ArrayList<String>();
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
