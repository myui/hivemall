/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2016 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hivemall.systemtest.model;

import com.google.common.io.Resources;
import com.klarna.hiverunner.CommandShellEmulation;
import com.klarna.hiverunner.sql.StatementsSplitter;
import hivemall.systemtest.model.lazy.LazyMatchingResource;
import hivemall.utils.lang.Preconditions;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public abstract class HQ {

    @Nonnull
    public static RawHQ fromStatement(String query) {
        Preconditions.checkNotNull(query);

        final String formatted = CommandShellEmulation.HIVE_CLI.transformScript(query);
        final List<String> split = StatementsSplitter.splitStatements(formatted);

        Preconditions.checkArgument(
            1 == split.size(),
            "Detected %s queries, should be exactly one. Use `HQ.fromStatements` for multi queries.",
            split.size());

        return new RawHQ(split.get(0));
    }

    @Nonnull
    public static List<RawHQ> fromStatements(@CheckForNull final String queries) {
        Preconditions.checkNotNull(queries);

        final String formatted = CommandShellEmulation.HIVE_CLI.transformScript(queries);
        final List<String> split = StatementsSplitter.splitStatements(formatted);
        final List<RawHQ> results = new ArrayList<RawHQ>();
        for (String q : split) {
            results.add(new RawHQ(q));
        }
        return results;
    }

    @Nonnull
    public static LazyMatchingResource autoMatchingByFileName(@CheckForNull final String fileName,
            @CheckForNull final Charset charset) {
        Preconditions.checkNotNull(fileName);
        Preconditions.checkNotNull(charset);

        return new LazyMatchingResource(fileName, charset);
    }

    @Nonnull
    public static LazyMatchingResource autoMatchingByFileName(final String fileName) {
        return autoMatchingByFileName(fileName, Charset.defaultCharset());
    }

    @Nonnull
    public static List<RawHQ> fromResourcePath(final String resourcePath, final Charset charset) {
        return autoMatchingByFileName(resourcePath, charset).toStrict("");
    }

    @Nonnull
    public static List<RawHQ> fromResourcePath(final String resourcePath) {
        return fromResourcePath(resourcePath, Charset.defaultCharset());
    }

    @Nonnull
    public static TableListHQ tableList() {
        return new TableListHQ();
    }

    @Nonnull
    public static CreateTableHQ createTable(@CheckForNull final String tableName,
            @CheckForNull final LinkedHashMap<String, String> header) {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(header);

        return new CreateTableHQ(tableName, header);
    }

    @Nonnull
    public static DropTableHQ dropTable(@CheckForNull final String tableName) {
        Preconditions.checkNotNull(tableName);

        return new DropTableHQ(tableName);
    }

    @Nonnull
    public static InsertHQ insert(@CheckForNull final String tableName,
            @CheckForNull final List<String> header, @CheckForNull final List<Object[]> data) {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(header);
        Preconditions.checkNotNull(data);

        return new InsertHQ(tableName, header, data);
    }

    @Nonnull
    public static UploadFileToExistingHQ uploadByFullPathToExisting(
            @CheckForNull final String tableName, @CheckForNull final String fullPath) {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(fullPath);

        return new UploadFileToExistingHQ(tableName, new File(fullPath));
    }

    @Nonnull
    public static UploadFileToExistingHQ uploadByResourcePathToExisting(final String tableName,
            final String resourcePath) {
        return uploadByFullPathToExisting(tableName, Resources.getResource(resourcePath).getPath());
    }

    @Nonnull
    public static UploadFileAsNewTableHQ uploadByFullPathAsNewTable(
            @CheckForNull final String tableName, @CheckForNull final String fullPath,
            @CheckForNull final LinkedHashMap<String, String> header) {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(fullPath);
        Preconditions.checkNotNull(header);

        final File file = new File(fullPath);

        Preconditions.checkArgument(file.exists(), "%s not found", file.getPath());

        return new UploadFileAsNewTableHQ(tableName, file, header);
    }

    @Nonnull
    public static UploadFileAsNewTableHQ uploadByResourcePathAsNewTable(final String tableName,
            final String resourcePath, final LinkedHashMap<String, String> header) {
        return uploadByFullPathAsNewTable(tableName, Resources.getResource(resourcePath).getPath(),
            header);
    }
}
