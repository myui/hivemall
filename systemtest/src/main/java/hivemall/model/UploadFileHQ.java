package hivemall.model;

import hivemall.utils.lang.Preconditions;

import java.io.File;

public abstract class UploadFileHQ extends TableHQ {
    public enum Format {
        MSGPACK, CSV, TSV, UNKNOWN
    }

    public final File file;
    public final Format format;


    UploadFileHQ(String tableName, File file) {
        super(tableName);

        Preconditions.checkArgument(file.exists(), "%s is not found", file.getPath());

        this.file = file;
        this.format = guessFormat(file);
    }


    Format guessFormat(File file) {
        String fileName = file.getName();
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
