/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2015
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.utils.io;

import hivemall.utils.lang.NumberUtils;

import java.io.File;

import javax.annotation.Nonnull;

public final class FileUtils {

    private FileUtils() {}

    public static long getFileSize(@Nonnull File file) {
        if(!file.exists()) {
            return -1L;
        }
        long size = 0;
        if(file.isDirectory()) {
            File[] files = file.listFiles();
            if(files != null && files.length > 0) {
                for(File f : files) {
                    size += getFileSize(f);
                }
            }
        } else {
            size += file.length();
        }
        return size;
    }

    public static String prettyFileSize(@Nonnull File file) {
        return NumberUtils.prettySize(getFileSize(file));
    }

}
