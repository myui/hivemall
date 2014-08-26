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
package hivemall.utils.net;

import hivemall.utils.io.FastBufferedOutputStream;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

public final class NetUtils {

    private NetUtils() {}

    public static SocketAddress getSocketAddress(String endpointURI) {
        int pos = endpointURI.indexOf(':');
        String host = endpointURI.substring(0, pos);
        String portStr = endpointURI.substring(pos + 1);
        int port = Integer.parseInt(portStr);
        return new InetSocketAddress(host, port);
    }

    public static Socket openSocket(String endpointURI) throws IOException {
        SocketAddress sockAddr = getSocketAddress(endpointURI);
        return openSocket(sockAddr);
    }

    public static Socket openSocket(SocketAddress endpoint) throws IOException {
        Socket socket = new Socket();
        socket.connect(endpoint);
        return socket;
    }

    public static String getLocalHostName() {
        return getLocalHost().getHostName();
    }

    public static InetAddress getLocalHost() {
        return getLocalHost(false);
    }

    public static InetAddress getLocalHost(boolean allowLoopbackAddr) {
        InetAddress localHost = null;
        try {
            InetAddress probeAddr = InetAddress.getLocalHost();
            if(allowLoopbackAddr) {
                localHost = probeAddr;
            }
            if(probeAddr.isLoopbackAddress()/* || probeAddr.isLinkLocalAddress() */) {
                final Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
                nicLoop: while(nics.hasMoreElements()) {
                    NetworkInterface nic = nics.nextElement();
                    if(nic.isLoopback()) {
                        continue;
                    }
                    final Enumeration<InetAddress> nicAddrs = nic.getInetAddresses();
                    while(nicAddrs.hasMoreElements()) {
                        InetAddress nicAddr = nicAddrs.nextElement();
                        if(!nicAddr.isLoopbackAddress()/* && !nicAddr.isLinkLocalAddress() */) {
                            localHost = nicAddr;
                            if(nic.isVirtual()) {
                                continue nicLoop; // try to find IP-address of non-virtual NIC
                            } else {
                                break nicLoop;
                            }
                        }
                    }
                }
            } else {
                localHost = probeAddr;
            }
        } catch (UnknownHostException ue) {
            throw new IllegalStateException(ue);
        } catch (SocketException se) {
            throw new IllegalStateException(se);
        }
        return localHost;
    }

    public static DataInputStream getDataInputStream(Socket socket) throws IOException {
        InputStream is = socket.getInputStream();
        return new DataInputStream(new BufferedInputStream(is));
    }

    public static DataOutputStream getDataOutputStream(Socket socket) throws IOException {
        OutputStream os = socket.getOutputStream();
        return new DataOutputStream(new FastBufferedOutputStream(os));
    }

}
