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

import com.google.common.io.Resources;
import hivemall.systemtest.exception.QueryExecutionException;
import hivemall.systemtest.model.CreateTableHQ;
import hivemall.systemtest.model.DropTableHQ;
import hivemall.systemtest.model.HQ;
import hivemall.systemtest.model.HQBase;
import hivemall.systemtest.model.InsertHQ;
import hivemall.systemtest.model.RawHQ;
import hivemall.systemtest.model.TableHQ;
import hivemall.systemtest.model.TableListHQ;
import hivemall.systemtest.model.UploadFileAsNewTableHQ;
import hivemall.systemtest.model.UploadFileHQ;
import hivemall.systemtest.model.UploadFileToExistingHQ;
import hivemall.systemtest.utils.IO;
import hivemall.utils.lang.Preconditions;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public abstract class SystemTestRunner extends ExternalResource {
    static final Logger logger = LoggerFactory.getLogger(SystemTestRunner.class);
    @Nonnull
    final List<HQBase> classInitHqs;
    @Nonnull
    final Set<String> immutableTables;
    @Nonnull
    final String dbName;
    @Nonnull
    final Properties props;

    SystemTestRunner(@CheckForNull SystemTestCommonInfo ci, @CheckForNull String propertiesFile) {
        Preconditions.checkNotNull(ci);
        Preconditions.checkNotNull(propertiesFile);

        classInitHqs = new ArrayList<HQBase>();
        immutableTables = new HashSet<String>();
        dbName = ci.dbName;

        final String path = "hivemall/" + propertiesFile;
        try {
            InputStream is = null;
            try {
                props = new Properties();
                is = new FileInputStream(Resources.getResource(path).getPath());
                props.load(is);
            } finally {
                if (is != null) {
                    is.close();
                }
            }
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to load properties from " + path + ". "
                    + ex.getMessage());
        }
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
            throw new QueryExecutionException("Failed to clean up temporary database. "
                    + ex.getMessage());
        } finally {
            finRunner();
        }
    }

    abstract void initRunner();

    abstract void finRunner();

    public void initBy(@Nonnull final HQBase hq) {
        classInitHqs.add(hq);
    }

    public void initBy(@Nonnull final List<? extends HQBase> hqs) {
        classInitHqs.addAll(hqs);
    }

    // fix to temporary database and user-defined init (should be called per Test class)
    void prepareDB() throws Exception {
        createDB(dbName);
        use(dbName);
        for (HQBase q : classInitHqs) {
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
    void resetDB() throws Exception {
        dropDB(dbName);
    }

    public final boolean isImmutableTable(final String tableName) {
        return immutableTables.contains(tableName);
    }

    // execute HQBase
    public List<String> exec(@Nonnull final HQBase hq) throws Exception {
        if (hq instanceof RawHQ) {
            return exec((RawHQ) hq);
        } else if (hq instanceof TableHQ) {
            return exec((TableHQ) hq);
        } else if (hq instanceof TableListHQ) {
            return tableList();
        } else {
            throw new IllegalArgumentException("Unexpected query type: " + hq.getClass());
        }
    }

    //// execute RawHQ
    abstract protected List<String> exec(@Nonnull final RawHQ hq) throws Exception;

    //// execute TableHQ
    List<String> exec(@Nonnull final TableHQ hq) throws Exception {
        if (hq instanceof CreateTableHQ) {
            return createTable((CreateTableHQ) hq);
        } else if (hq instanceof DropTableHQ) {
            return dropTable((DropTableHQ) hq);
        } else if (hq instanceof InsertHQ) {
            return insert((InsertHQ) hq);
        } else if (hq instanceof UploadFileHQ) {
            return exec((UploadFileHQ) hq);
        } else {
            throw new IllegalArgumentException("Unexpected query type: " + hq.getClass());
        }
    }

    ////// execute UploadFileHQ
    List<String> exec(@Nonnull final UploadFileHQ hq) throws Exception {
        if (hq instanceof UploadFileAsNewTableHQ) {
            return uploadFileAsNewTable((UploadFileAsNewTableHQ) hq);
        } else if (hq instanceof UploadFileToExistingHQ) {
            return uploadFileToExisting((UploadFileToExistingHQ) hq);
        } else {
            throw new IllegalArgumentException("Unexpected query type: " + hq.getClass());
        }
    }

    // matching HQBase
    public void matching(@Nonnull final HQBase hq, @CheckForNull final String answer,
            final boolean ordered) throws Exception {
        Preconditions.checkNotNull(answer);

        List<String> result = exec(hq);

        if (ordered) {
            // take order into consideration (like list)
            Assert.assertThat(Arrays.asList(answer.split(IO.RD)),
                Matchers.contains(result.toArray()));
        } else {
            // not take order into consideration (like multiset)
            Assert.assertThat(Arrays.asList(answer.split(IO.RD)),
                Matchers.containsInAnyOrder(result.toArray()));
        }
    }

    // matching HQBase (ordered == false)
    public void matching(@Nonnull final HQBase hq, @CheckForNull final String answer)
            throws Exception {
        matching(hq, answer, false);
    }

    List<String> createDB(@Nonnull final String dbName) throws Exception {
        logger.info("executing: create database if not exists" + dbName);

        return exec(HQ.fromStatement("CREATE DATABASE IF NOT EXISTS " + dbName));
    }

    List<String> dropDB(@Nonnull final String dbName) throws Exception {
        logger.info("executing: drop database if exists " + dbName);

        return exec(HQ.fromStatement("DROP DATABASE IF EXISTS " + dbName + " CASCADE"));
    }

    List<String> use(@Nonnull final String dbName) throws Exception {
        logger.info("executing: use " + dbName);

        return exec(HQ.fromStatement("USE " + dbName));
    }

    List<String> tableList() throws Exception {
        logger.info("executing: show tables on " + dbName);

        return exec(HQ.fromStatement("SHOW TABLES"));
    }

    List<String> createTable(@Nonnull final CreateTableHQ hq) throws Exception {
        logger.info("executing: create table " + hq.tableName + " if not exists on " + dbName);

        return exec(HQ.fromStatement("CREATE TABLE IF NOT EXISTS " + hq.tableName
                + hq.getTableDeclaration()));
    }

    List<String> dropTable(@Nonnull final DropTableHQ hq) throws Exception {
        logger.info("executing: drop table " + hq.tableName + " if exists on " + dbName);

        return exec(HQ.fromStatement("DROP TABLE IF EXISTS " + hq.tableName));
    }

    List<String> insert(@Nonnull final InsertHQ hq) throws Exception {
        logger.info("executing: insert into " + hq.tableName + " on " + dbName);

        // *WORKAROUND*
        // `INSERT INTO TABLE ... VALUES ...`
        //     cannot use array() and map() with `VALUES` on hiverunner(v3.0.0),
        //     cannot insert anything on TD(v20160901)
        // `WITH ... AS (SELECT ...) INSERT INTO TABLE ... SELECT * FROM ...`
        //     can insert anything on hiverunner(v3.0.0)
        //     cannot use map<?> on TD(v20160901)
        final StringBuilder sb = new StringBuilder();
        sb.append("WITH temporary_table_for_with_clause AS (");
        for (Object[] row : hq.data) {
            sb.append("SELECT ");
            for (int i = 0; i < hq.header.size(); i++) {
                sb.append(serialize(row[i]));
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
        sb.append(" SELECT * FROM temporary_table_for_with_clause");

        return exec(HQ.fromStatement(sb.toString()));
    }

    List<String> uploadFileAsNewTable(@Nonnull final UploadFileAsNewTableHQ hq) throws Exception {
        logger.info("executing: create " + hq.tableName + " based on " + hq.file.getPath()
                + " if not exists on " + dbName);

        createTable(HQ.createTable(hq.tableName, hq.header));
        return uploadFileToExisting(HQ.uploadByFullPathToExisting(hq.tableName, hq.file.getPath()));
    }

    abstract List<String> uploadFileToExisting(@Nonnull final UploadFileToExistingHQ hq)
            throws Exception;

    private String serialize(@Nullable final Object val) {
        // NOTE: this method is low-performance, don't use w/ big data
        if (val instanceof String) {
            return "'" + String.valueOf(val) + "'";
        } else if (val instanceof Object[]) {
            final Object[] objs = (Object[]) val;
            final StringBuilder sb = new StringBuilder();
            sb.append("array(");
            for (Object o : objs) {
                sb.append(serialize(o));
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
            return sb.toString();
        } else if (val instanceof List<?>) {
            final List<?> list = (List<?>) val;
            final StringBuilder sb = new StringBuilder();
            sb.append("array(");
            for (Object o : list) {
                sb.append(serialize(o));
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
            return sb.toString();
        } else if (val instanceof Map<?, ?>) {
            final Map<?, ?> map = (Map<?, ?>) val;
            final StringBuilder sb = new StringBuilder();
            sb.append("map(");
            for (Map.Entry<?, ?> e : map.entrySet()) {
                sb.append(serialize(e.getKey()));
                sb.append(",");
                sb.append(serialize(e.getValue()));
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
            return sb.toString();
        } else {
            return String.valueOf(val);
        }
    }
}
