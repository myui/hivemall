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
package hivemall.systemtest;

import hivemall.utils.lang.Preconditions;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.value.ValueFactory;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class MsgpackConverter {
    @Nonnull
    private final File file;
    @Nonnull
    private final List<String> header;
    @Nonnull
    private final CSVFormat format;

    public MsgpackConverter(@CheckForNull File file, @CheckForNull List<String> header,
            @CheckForNull CSVFormat format) {
        Preconditions.checkNotNull(file);
        Preconditions.checkNotNull(header);
        Preconditions.checkNotNull(format);
        Preconditions.checkArgument(file.exists(), "%s not found", file.getPath());

        this.file = file;
        this.header = header;
        this.format = format;
    }

    public byte[] asByteArray(final boolean needTimeColumn) throws Exception {
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final MessagePacker packer = MessagePack.newDefaultPacker(new GZIPOutputStream(os));
        final BufferedReader br = new BufferedReader(new FileReader(file));
        try {
            // always skip header, use user-defined or existing table's
            final CSVParser parser = format.withSkipHeaderRecord().parse(br);
            final long time = System.currentTimeMillis() / 1000;
            for (CSVRecord record : parser.getRecords()) {
                final ValueFactory.MapBuilder map = ValueFactory.newMapBuilder();

                // add `time` column if needed && not exists
                if (needTimeColumn && !header.contains("time")) {
                    map.put(ValueFactory.newString("time"), ValueFactory.newInteger(time));
                }

                // pack each value in row
                int i = 0;
                for (String val : record) {
                    map.put(ValueFactory.newString(header.get(i)), ValueFactory.newString(val));
                    i++;
                }
                packer.packValue(map.build());
            }
        } finally {
            br.close();
            packer.close();
        }

        return os.toByteArray();
    }

    public byte[] asByteArray() throws Exception {
        return asByteArray(true);
    }

    public File asFile(@CheckForNull File to, final boolean needTimeColumn) throws Exception {
        Preconditions.checkNotNull(to);
        Preconditions.checkArgument(to.exists(), "%s not found", to.getPath());

        FileOutputStream os = null;
        try {
            os = new FileOutputStream(to);
            os.write(asByteArray(needTimeColumn));
            return to;
        } finally {
            if (os != null) {
                os.close();
            }
        }
    }

    public File asFile(File to) throws Exception {
        return asFile(to, true);
    }
}
