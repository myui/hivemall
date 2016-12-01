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
package hivemall.systemtest.model;

import hivemall.utils.lang.Preconditions;

import javax.annotation.Nonnull;
import java.io.File;

public abstract class UploadFileHQ extends TableHQ {
    public enum Format {
        MSGPACK, CSV, TSV, UNKNOWN
    }

    @Nonnull
    public final File file;
    @Nonnull
    public final Format format;

    UploadFileHQ(@Nonnull final String tableName, @Nonnull final File file) {
        super(tableName);

        Preconditions.checkArgument(file.exists(), "%s not found", file.getPath());

        this.file = file;
        this.format = guessFormat(file);
    }

    private Format guessFormat(File file) {
        final String fileName = file.getName();
        if (fileName.endsWith(".msgpack.gz")) {
            return Format.MSGPACK;
        } else if (fileName.endsWith(".csv")) {
            return Format.CSV;
        } else if (fileName.endsWith(".tsv")) {
            return Format.TSV;
        } else {
            return Format.UNKNOWN;
        }
    }
}
