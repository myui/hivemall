package hivemall;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.value.ValueFactory;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class ConvertToMsgpack {
    @Nonnull
    private final File file;
    @Nonnull
    private final List<String> header;
    @Nonnull
    private final CSVFormat format;


    public ConvertToMsgpack(File file, List<String> header, CSVFormat format) {
        this.file = file;
        this.header = header;
        this.format = format;
    }


    public byte[] asByteArray(final boolean needTimeColumn) throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        MessagePacker packer = MessagePack.newDefaultPacker(new GZIPOutputStream(os));
        BufferedReader br = new BufferedReader(new FileReader(file));
        try {
            // always skip header, use user-defined or existing table's
            CSVParser parser = format.withSkipHeaderRecord().parse(br);
            final long time = System.currentTimeMillis() / 1000;
            for (CSVRecord record : parser.getRecords()) {
                ValueFactory.MapBuilder map = ValueFactory.newMapBuilder();

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

    public File asFile(File to, final boolean needTimeColumn) throws Exception {
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
