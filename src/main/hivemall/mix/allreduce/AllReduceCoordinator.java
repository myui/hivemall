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
package hivemall.mix.allreduce;

import hivemall.mix.NodeInfo;
import hivemall.utils.concurrent.ExecutorUtils;
import hivemall.utils.io.FastBufferedOutputStream;
import hivemall.utils.io.IOUtils;
import hivemall.utils.lang.CommandLineUtils;
import hivemall.utils.lang.Primitives;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public final class AllReduceCoordinator implements Runnable {
    private static final int DEFAULT_PORT = 26543;
    private static final int DEFAULT_CHILDS = 2;

    private final ServerSocket serverSocket;
    private final NodeManager nodeManager;

    public AllReduceCoordinator(int port, int maxChildConn) {
        try {
            this.serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new RuntimeException("failed to create a server socket on port: " + port, e);
        }
        this.nodeManager = new NodeManager(maxChildConn);
    }

    public String getEndpointURI() {
        InetAddress addr = serverSocket.getInetAddress();
        int port = serverSocket.getLocalPort();
        return addr.getHostName() + ':' + port;
    }

    public static void main(String[] args) {
        Options opts = new Options();
        opts.addOption("p", "port", true, "port number of the spanning tree builder");
        opts.addOption("cc", "max_child_conn", true, "Max number of child connections per node");
        CommandLine cl = CommandLineUtils.parseOptions(args, opts);
        int port = Primitives.parseInt(cl.getOptionValue("port"), DEFAULT_PORT);
        int children = Primitives.parseInt(cl.getOptionValue("max_child_conn"), DEFAULT_CHILDS);

        AllReduceCoordinator coord = new AllReduceCoordinator(port, children);
        coord.run();

        System.out.println("Started an AllReduce coordinator: " + coord.getEndpointURI());
    }

    @Override
    public void run() {
        final ExecutorService pool = Executors.newCachedThreadPool();
        try {
            while(true) {
                try {
                    Socket socket = serverSocket.accept();
                    Worker worker = new Worker(socket, nodeManager);
                    pool.execute(worker);
                } catch (IOException e) {
                    System.err.println(e);
                    break;
                }
            }
        } finally {
            ExecutorUtils.shutdownAndAwaitTermination(pool, 10);
        }
    }

    public enum Command {
        /** Register a node and configure/return its parent */
        registerNode,
        /** Unregister a node */
        unregisterNode
    }

    private static final class Worker implements Runnable {

        private final NodeManager nodeManager;

        private final DataInputStream in;
        private final DataOutputStream out;

        public Worker(Socket socket, NodeManager nodeManager) throws IOException {
            this.nodeManager = nodeManager;
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            this.out = new DataOutputStream(new FastBufferedOutputStream(socket.getOutputStream()));
        }

        @Override
        public void run() {
            try {
                // 1. read a command
                final Command cmd = IOUtils.readEnum(in, Command.class);
                // 2. read a session name
                final String sessionName = IOUtils.readString(in);
                // 3. read a node info
                final NodeInfo node = NodeInfo.readFrom(in);
                switch(cmd) {
                    case registerNode: {
                        NodeInfo parent = nodeManager.pop_push(sessionName, node);
                        // 4. write a parent node
                        NodeInfo.writeTo(parent, out); // parent is null then node becomes root
                        out.flush();
                        System.out.println(((parent == null) ? "Got no parent "
                                : ("Got parent " + parent)) + " for node: " + node);
                        break;
                    }
                    case unregisterNode: {
                        nodeManager.remove(sessionName, node);
                        System.out.println("Node releaved: " + node);
                        break;
                    }
                    default: {
                        System.err.println("Unknown command '" + cmd + "' from " + node);
                        break;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
