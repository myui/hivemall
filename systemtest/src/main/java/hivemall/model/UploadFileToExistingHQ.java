package hivemall.model;

import java.io.File;

public class UploadFileToExistingHQ extends UploadFileHQ {
    UploadFileToExistingHQ(String tableName, File file) {
        super(tableName, file);
    }
}
