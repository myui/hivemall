package hivemall.systemtest.model;

import hivemall.utils.lang.Preconditions;

import java.io.File;
import java.util.LinkedHashMap;

public class UploadFileAsNewTableHQ extends UploadFileHQ {
    public final LinkedHashMap<String, String> header;


    UploadFileAsNewTableHQ(String tableName, File file, LinkedHashMap<String, String> header) {
        super(tableName, file);

        Preconditions.checkArgument(file.exists(), "%s is not found", file.getPath());

        this.header = header;
    }
}
