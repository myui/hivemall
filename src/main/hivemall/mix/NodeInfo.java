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

import hivemall.utils.io.IOUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

public class NodeInfo {

    private final InetAddress addr;
    private final int port;

    public NodeInfo(InetAddress addr, int port) {
        if(addr == null) {
            throw new IllegalArgumentException("addr is null");
        }
        this.addr = addr;
        this.port = port;
    }

    public NodeInfo(Socket socket) {
        assert (socket != null);
        this.addr = socket.getInetAddress();
        this.port = socket.getPort();
    }

    public NodeInfo(NodeInfo src) {
        assert (src != null);
        this.addr = src.addr;
        this.port = src.port;
    }

    public NodeInfo(ServerSocket socket) {
        assert (socket != null);
        this.addr = socket.getInetAddress();
        this.port = socket.getLocalPort();
    }

    public InetAddress getAddress() {
        return addr;
    }

    public int getPort() {
        return port;
    }

    public SocketAddress getSocketAddress() {
        return new InetSocketAddress(addr, port);
    }

    @Override
    public int hashCode() {
        return addr.hashCode() + port;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }
        if(obj instanceof NodeInfo) {
            NodeInfo other = (NodeInfo) obj;
            return addr.equals(other.addr) && (port == other.port);
        }
        return false;
    }

    @Override
    public String toString() {
        return addr.toString() + ":" + port;
    }

    public void writeTo(DataOutput out) throws IOException {
        writeTo(this, out);
    }

    public static void writeTo(NodeInfo node, DataOutput out) throws IOException {
        if(node == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        byte[] b = node.getAddress().getAddress();
        IOUtils.writeBytes(b, out);
        int port = node.getPort();
        out.writeInt(port);
    }

    public static NodeInfo readFrom(DataInput in) throws IOException {
        if(in.readBoolean() == false) {
            return null;
        }
        byte[] b = IOUtils.readBytes(in);
        InetAddress addr = InetAddress.getByAddress(b);
        int port = in.readInt();
        return new NodeInfo(addr, port);
    }

}
