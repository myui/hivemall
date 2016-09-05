package hivemall.systemtest.runner;

import hivemall.systemtest.model.CreateTableHQ;
import hivemall.systemtest.model.DropTableHQ;
import hivemall.systemtest.model.HQ;
import hivemall.systemtest.model.InsertHQ;
import hivemall.systemtest.model.RawHQ;
import hivemall.systemtest.model.StrictHQ;
import hivemall.systemtest.model.TableHQ;
import hivemall.systemtest.model.TableListHQ;
import hivemall.systemtest.model.UploadFileAsNewTableHQ;
import hivemall.systemtest.model.UploadFileHQ;
import hivemall.systemtest.model.UploadFileToExistingHQ;
import hivemall.systemtest.utils.IO;
import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class SystemTestRunner extends ExternalResource {
    static final Logger logger = LoggerFactory.getLogger(SystemTestRunner.class);
    List<StrictHQ> classInitHqs = new ArrayList<StrictHQ>();
    Set<String> immutableTables = new HashSet<String>();
    final String dbName;


    SystemTestRunner(SystemTestCommonInfo ci) {
        this.dbName = formatDBName(ci.dbName);
    }


    @Override
    protected void before() throws Exception {
        initRunner();
        prepareDB(); // initialize database
    }

    @Override
    protected void after() {
        try {
            resetDB(); // clean up database
        } catch (Exception ex) {
            throw new RuntimeException("Failed to clean up temporary database. " + ex.getMessage());
        } finally {
            finRunner();
        }
    }

    abstract void initRunner();

    abstract void finRunner();

    public void initBy(StrictHQ hq) {
        classInitHqs.add(hq);
    }

    public void initBy(List<? extends StrictHQ> hqs) {
        classInitHqs.addAll(hqs);
    }

    // fix to temporary database and user-defined init (should be called per Test class)
    final void prepareDB() throws Exception {
        createDB(dbName);
        use(dbName);
        for (StrictHQ q : classInitHqs) {
            exec(q);

            if (q instanceof CreateTableHQ) {
                // memo table initialized each class as immutable
                immutableTables.add(((CreateTableHQ) q).tableName);
            } else if (q instanceof UploadFileAsNewTableHQ) {
                immutableTables.add(((UploadFileAsNewTableHQ) q).tableName);
            }
        }
    }

    // drop temporary database (should be called per Test class)
    final void resetDB() throws Exception {
        dropDB(dbName);
    }

    String formatDBName(String dbName) {
        return dbName;
    }

    public final boolean isImmutableTable(String tableName) {
        return immutableTables.contains(tableName);
    }

    // execute StrictHQ
    public List<String> exec(StrictHQ hq) throws Exception {
        if (hq instanceof RawHQ)
            return exec((RawHQ) hq);
        else if (hq instanceof TableHQ)
            return exec((TableHQ) hq);
        else if (hq instanceof TableListHQ)
            return tableList();
        else
            throw new RuntimeException("Unexpected query type: " + hq.getClass());
    }

    //// execute RawHQ
    abstract protected List<String> exec(RawHQ hq) throws Exception;

    //// execute TableHQ
    protected List<String> exec(TableHQ hq) throws Exception {
        if (hq instanceof CreateTableHQ)
            return createTable((CreateTableHQ) hq);
        else if (hq instanceof DropTableHQ)
            return dropTable((DropTableHQ) hq);
        else if (hq instanceof InsertHQ)
            return insert((InsertHQ) hq);
        else if (hq instanceof UploadFileHQ)
            return exec((UploadFileHQ) hq);
        else
            throw new RuntimeException("Unexpected query type: " + hq.getClass());
    }

    ////// execute UploadFileHQ
    protected List<String> exec(UploadFileHQ hq) throws Exception {
        if (hq instanceof UploadFileAsNewTableHQ)
            return uploadFileAsNewTable((UploadFileAsNewTableHQ) hq);
        else if (hq instanceof UploadFileToExistingHQ)
            return uploadFileToExisting((UploadFileToExistingHQ) hq);
        else
            throw new RuntimeException("Unexpected query type: " + hq.getClass());
    }

    // matching StrictHQ
    public void matching(StrictHQ hq, String answer) throws Exception {
        List<String> result = exec(hq);

        Assert.assertArrayEquals(answer.split(IO.RD), result.toArray());
    }

    List<String> createDB(String dbName) throws Exception {
        logger.info("executing: create database if not exists" + dbName);

        return exec(HQ.fromStatement("CREATE DATABASE IF NOT EXISTS " + dbName));
    }

    List<String> dropDB(String dbName) throws Exception {
        logger.info("executing: drop database if exists " + dbName);

        return exec(HQ.fromStatement("DROP DATABASE IF EXISTS " + dbName + " CASCADE"));
    }

    List<String> use(String dbName) throws Exception {
        logger.info("executing: use " + dbName);

        return exec(HQ.fromStatement("USE " + dbName));
    }

    List<String> tableList() throws Exception {
        logger.info("executing: show tables on " + dbName);

        return exec(HQ.fromStatement("SHOW TABLES"));
    }

    List<String> createTable(CreateTableHQ hq) throws Exception {
        logger.info("executing: create table " + hq.tableName + " if not exists on " + dbName);

        return exec(HQ.fromStatement("CREATE TABLE IF NOT EXISTS " + hq.tableName
                + hq.getTableDeclaration()));
    }

    List<String> dropTable(DropTableHQ hq) throws Exception {
        logger.info("executing: drop table " + hq.tableName + " if exists on " + dbName);

        return exec(HQ.fromStatement("DROP TABLE IF EXISTS " + hq.tableName));
    }

    List<String> insert(InsertHQ hq) throws Exception {
        logger.info("executing: insert into " + hq.tableName + " on " + dbName);

        return exec(HQ.fromStatement("INSERT INTO TABLE " + hq.tableName + " VALUES "
                + hq.getAsValuesFormat()));
    }

    List<String> uploadFileAsNewTable(UploadFileAsNewTableHQ hq) throws Exception {
        logger.info("executing: create " + hq.tableName + " based on " + hq.file.getPath()
                + " if not exists on " + dbName);

        createTable(HQ.createTable(hq.tableName, hq.header));
        return uploadFileToExisting(HQ.uploadByFullPathToExisting(hq.tableName, hq.file.getPath()));
    }

    abstract List<String> uploadFileToExisting(UploadFileToExistingHQ hq) throws Exception;
}
