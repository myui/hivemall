/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.utils.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.NoSuchElementException;

public final class NetUtils {

    private NetUtils() {}

    public static InetSocketAddress getInetSocketAddress(String endpointURI, int defaultPort) {
        final int pos = endpointURI.indexOf(':');
        if (pos == -1) {
            InetAddress addr = getInetAddress(endpointURI);
            return new InetSocketAddress(addr, defaultPort);
        } else {
            String host = endpointURI.substring(0, pos);
            InetAddress addr = getInetAddress(host);
            String portStr = endpointURI.substring(pos + 1);
            int port = Integer.parseInt(portStr);
            return new InetSocketAddress(addr, port);
        }
    }

    public static InetAddress getInetAddress(final String addressOrName) {
        try {
            return InetAddress.getByName(addressOrName);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Cannot find InetAddress: " + addressOrName);
        }
    }

    public static boolean isIPAddress(final String ip) {
        return ip.matches("^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$");
    }

    public static int getAvailablePort() {
        try {
            ServerSocket s = new ServerSocket(0);
            s.setReuseAddress(true);
            s.close();
            return s.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to find an available port", e);
        }
    }

    public static int getAvailablePort(final int basePort) {
        if (basePort == 0) {
            return getAvailablePort();
        }
        if (basePort < 0 || basePort > 65535) {
            throw new IllegalArgumentException("Illegal port number: " + basePort);
        }
        for (int i = basePort; i <= 65535; i++) {
            if (isPortAvailable(i)) {
                return i;
            }
        }
        throw new NoSuchElementException("Could not find available port greater than or equals to "
                + basePort);
    }

    public static boolean isPortAvailable(final int port) {
        ServerSocket s = null;
        try {
            s = new ServerSocket(port);
            s.setReuseAddress(true);
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException e) {
                    ;
                }
            }
        }
    }
}
