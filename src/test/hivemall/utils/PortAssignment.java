/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
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
package hivemall.utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

public class PortAssignment {
    private static int nextPort = 11212;

    static final File TEST_BASE = new File(System.getProperty("build.test.dir", "build"));
    static private RandomAccessFile portNumLockFile;
    static private File portNumFile;

    synchronized public static int getNextPort() {
        TEST_BASE.mkdirs();
        int port;
        for (;;) {
            port = nextPort++;
            FileLock lock = null;
            portNumLockFile = null;
            try {
                try {
                    portNumFile = new File(TEST_BASE, port + ".lock");
                    portNumLockFile = new RandomAccessFile(portNumFile, "rw");
                    try {
                        lock = portNumLockFile.getChannel().tryLock();
                    } catch (OverlappingFileLockException e) {
                        continue;
                    }
                } finally {
                    if (lock != null) {
                        break;
                    }
                    if (portNumLockFile != null) {
                        portNumLockFile.close();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return port;
    }

}
