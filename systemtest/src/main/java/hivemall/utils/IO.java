package hivemall.utils;

import com.google.common.io.Resources;
import hivemall.utils.lang.Preconditions;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class IO {
    public static final String RD = "\t"; // row delimiter
    public static final String QD = "\n"; // query delimiter


    private IO() {}


    public static String getFromFullPath(String fullPath, Charset charset) {
        Preconditions.checkArgument(new File(fullPath).exists(), "%s is not found", fullPath);

        return new String(readAllBytes(fullPath), charset);
    }

    public static String getFromFullPath(String fullPath) {
        return getFromFullPath(fullPath, Charset.defaultCharset());
    }

    public static String getFromResourcePath(String resourcePath, Charset charset) {
        String fullPath = Resources.getResource(resourcePath).getPath();
        return getFromFullPath(fullPath, charset);
    }

    public static String getFromResourcePath(String resourcePath) {
        return getFromResourcePath(resourcePath, Charset.defaultCharset());
    }

    private static byte[] readAllBytes(String filePath) {
        File f = new File(filePath);

        int len = (int) f.length();
        byte[] buf = new byte[len];

        InputStream is = null;
        try {
            try {
                is = new FileInputStream(f);
                is.read(buf);
            } finally {
                if (is != null)
                    is.close();
            }
        } catch (IOException ex) {
            throw new RuntimeException("Failed to read " + filePath + ". " + ex.getMessage());
        }

        return buf;
    }
}
