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
package hivemall.mix.server;

import hivemall.io.DenseModel;
import hivemall.io.PredictionModel;
import hivemall.io.SparseModel;
import hivemall.io.WeightValue;
import hivemall.mix.MixMessage.MixEventName;
import hivemall.mix.client.MixClient;
import hivemall.mix.server.MixServer.ServerState;
import hivemall.utils.io.IOUtils;
import hivemall.utils.lang.CommandLineUtils;
import hivemall.utils.net.NetUtils;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.commons.cli.CommandLine;
import org.junit.Test;

public class MixServerTest {

    @Test
    public void testSimpleScenario() throws InterruptedException {
        int port = NetUtils.getAvialablePort(11212);
        CommandLine cl = CommandLineUtils.parseOptions(new String[] { "-port",
                Integer.toString(port), "-sync_threshold", "3" }, MixServer.getOptions());
        MixServer server = new MixServer(cl);
        ExecutorService serverExec = Executors.newSingleThreadExecutor();
        serverExec.submit(server);

        waitForState(server, ServerState.RUNNING);

        PredictionModel model = new DenseModel(16777216, false);
        model.configureClock();
        MixClient client = null;
        try {
            client = new MixClient(MixEventName.average, "testSimpleScenario", "localhost:" + port, false, 2, model);
            model.setUpdateHandler(client);

            final Random rand = new Random(43);
            for(int i = 0; i < 100000; i++) {
                Integer feature = Integer.valueOf(rand.nextInt(100));
                float weight = (float) rand.nextGaussian();
                model.set(feature, new WeightValue(weight));
            }

            Thread.sleep(5 * 1000); // slight delay to wait for async callbacks

            int numMixed = model.getNumMixed();
            //System.out.println("number of mix events: " + numMixed);
            Assert.assertTrue("number of mix events: " + numMixed, numMixed > 0);

            serverExec.shutdown();
        } finally {
            IOUtils.closeQuietly(client);
        }
    }

    @Test
    public void testSSL() throws InterruptedException {
        int port = NetUtils.getAvialablePort(11213);
        CommandLine cl = CommandLineUtils.parseOptions(new String[] { "-port",
                Integer.toString(port), "-sync_threshold", "3", "-ssl" }, MixServer.getOptions());
        MixServer server = new MixServer(cl);
        ExecutorService serverExec = Executors.newSingleThreadExecutor();
        serverExec.submit(server);

        waitForState(server, ServerState.RUNNING);

        PredictionModel model = new DenseModel(16777216, false);
        model.configureClock();
        MixClient client = null;
        try {
            client = new MixClient(MixEventName.average, "testSSL", "localhost:" + port, true, 2, model);
            model.setUpdateHandler(client);

            final Random rand = new Random(43);
            for(int i = 0; i < 100000; i++) {
                Integer feature = Integer.valueOf(rand.nextInt(100));
                float weight = (float) rand.nextGaussian();
                model.set(feature, new WeightValue(weight));
            }

            Thread.sleep(5 * 1000); // slight delay to wait for async callbacks

            int numMixed = model.getNumMixed();
            //System.out.println("number of mix events: " + numMixed);
            Assert.assertTrue("number of mix events: " + numMixed, numMixed > 0);

            serverExec.shutdown();
        } finally {
            IOUtils.closeQuietly(client);
        }
    }

    @Test
    public void testMultipleClients() throws InterruptedException {
        final int port = NetUtils.getAvialablePort(11214);
        CommandLine cl = CommandLineUtils.parseOptions(new String[] { "-port",
                Integer.toString(port), "-sync_threshold", "3" }, MixServer.getOptions());
        MixServer server = new MixServer(cl);
        ExecutorService serverExec = Executors.newSingleThreadExecutor();
        serverExec.submit(server);

        waitForState(server, ServerState.RUNNING);

        final int numClients = 5;
        final ExecutorService clientsExec = Executors.newCachedThreadPool();
        for(int i = 0; i < numClients; i++) {
            clientsExec.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        invokeClient("testMultipleClients", port);
                    } catch (InterruptedException e) {
                        Assert.fail(e.getMessage());
                    }
                }
            });
        }
        clientsExec.awaitTermination(10, TimeUnit.SECONDS);
        clientsExec.shutdown();
        serverExec.shutdown();
    }

    private static void invokeClient(String groupId, int serverPort) throws InterruptedException {
        PredictionModel model = new DenseModel(16777216, false);
        model.configureClock();
        MixClient client = null;
        try {
            client = new MixClient(MixEventName.average, groupId, "localhost:" + serverPort, false, 2, model);
            model.setUpdateHandler(client);

            final Random rand = new Random(43);
            for(int i = 0; i < 100000; i++) {
                Integer feature = Integer.valueOf(rand.nextInt(100));
                float weight = (float) rand.nextGaussian();
                model.set(feature, new WeightValue(weight));
            }

            Thread.sleep(5 * 1000); // slight delay to wait for async callbacks

            int numMixed = model.getNumMixed();
            //System.out.println("number of mix events: " + numMixed);
            Assert.assertTrue("number of mix events: " + numMixed, numMixed > 0);
        } finally {
            IOUtils.closeQuietly(client);
        }

    }

    @Test
    public void test2ClientsZeroOneSparseModel() throws InterruptedException {
        final int port = NetUtils.getAvialablePort(11215);
        CommandLine cl = CommandLineUtils.parseOptions(new String[] { "-port",
                Integer.toString(port), "-sync_threshold", "30" }, MixServer.getOptions());
        MixServer server = new MixServer(cl);
        ExecutorService serverExec = Executors.newSingleThreadExecutor();
        serverExec.submit(server);

        waitForState(server, ServerState.RUNNING);

        final ExecutorService clientsExec = Executors.newCachedThreadPool();
        for(int i = 0; i < 2; i++) {
            clientsExec.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        invokeClient01("test2ClientsZeroOne", port, false);
                    } catch (InterruptedException e) {
                        Assert.fail(e.getMessage());
                    }
                }
            });
        }
        clientsExec.awaitTermination(30, TimeUnit.SECONDS);
        clientsExec.shutdown();
        serverExec.shutdown();
    }

    @Test
    public void test2ClientsZeroOneDenseModel() throws InterruptedException {
        final int port = NetUtils.getAvialablePort(11216);
        CommandLine cl = CommandLineUtils.parseOptions(new String[] { "-port",
                Integer.toString(port), "-sync_threshold", "30" }, MixServer.getOptions());
        MixServer server = new MixServer(cl);
        ExecutorService serverExec = Executors.newSingleThreadExecutor();
        serverExec.submit(server);

        waitForState(server, ServerState.RUNNING);

        final ExecutorService clientsExec = Executors.newCachedThreadPool();
        for(int i = 0; i < 2; i++) {
            clientsExec.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        invokeClient01("test2ClientsZeroOne", port, true);
                    } catch (InterruptedException e) {
                        Assert.fail(e.getMessage());
                    }
                }
            });
        }
        clientsExec.awaitTermination(30, TimeUnit.SECONDS);
        clientsExec.shutdown();
        serverExec.shutdown();
    }

    private static void invokeClient01(String groupId, int serverPort, boolean denseModel)
            throws InterruptedException {
        PredictionModel model = denseModel ? new DenseModel(100, false)
                : new SparseModel(100, false);
        model.configureClock();
        MixClient client = null;
        try {
            client = new MixClient(MixEventName.average, groupId, "localhost:" + serverPort, false, 3, model);
            model.setUpdateHandler(client);

            final Random rand = new Random(43);
            for(int i = 0; i < 1000000; i++) {
                Integer feature = Integer.valueOf(rand.nextInt(100));
                float weight = rand.nextFloat() >= 0.5f ? 1.f : 0.f;
                model.set(feature, new WeightValue(weight));
            }

            Thread.sleep(5 * 1000); // slight delay to wait for async callbacks

            int numMixed = model.getNumMixed();
            //System.out.println("number of mix events: " + numMixed);
            Assert.assertTrue("number of mix events: " + numMixed, numMixed > 0);

            for(int i = 0; i < 100; i++) {
                float w = model.getWeight(i);
                Assert.assertEquals(0.5f, w, 0.1f);
            }
        } finally {
            IOUtils.closeQuietly(client);
        }
    }

    private static void waitForState(MixServer server, ServerState expected)
            throws InterruptedException {
        int retry = 0;
        while(server.getState() != expected && retry < 50) {
            Thread.sleep(100);
            retry++;
        }
        Assert.assertEquals("MixServer state is not correct (timed out)", expected, server.getState());
    }

}
