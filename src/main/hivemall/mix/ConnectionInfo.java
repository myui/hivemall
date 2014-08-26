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
package hivemall.mix;

import hivemall.utils.net.NetUtils;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public final class ConnectionInfo implements Closeable {

    private final Socket socket;
    private final DataInputStream inStream;
    private final DataOutputStream outStream;

    public ConnectionInfo(Socket socket) throws IOException {
        this.socket = socket;
        this.inStream = NetUtils.getDataInputStream(socket);
        this.outStream = NetUtils.getDataOutputStream(socket);
    }

    public DataInputStream getDataInputStream() {
        return inStream;
    }

    public DataOutputStream getDataOutputStream() {
        return outStream;
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    @Override
    public String toString() {
        return socket.toString();
    }

}