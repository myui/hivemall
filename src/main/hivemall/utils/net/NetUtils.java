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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public final class NetUtils {

    private NetUtils() {}

    public static InetSocketAddress getInetSocketAddress(String endpointURI, int defaultPort) {
        final int pos = endpointURI.indexOf(':');
        if(pos == -1) {
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

}
