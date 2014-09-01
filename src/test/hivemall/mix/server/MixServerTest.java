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
import hivemall.io.WeightValue;
import hivemall.mix.MixMessage.MixEventName;
import hivemall.mix.client.MixClient;
import hivemall.utils.lang.CommandLineUtils;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.Assert;

import org.apache.commons.cli.CommandLine;
import org.junit.Test;

public class MixServerTest {

    @Test
    public void testScenario1() throws InterruptedException {
        CommandLine cl = CommandLineUtils.parseOptions(new String[] { "-port", "11212",
                "-sync_threshold", "3" }, MixServer.getOptions());
        MixServer server = new MixServer(cl);
        ExecutorService serverExec = Executors.newSingleThreadExecutor();
        serverExec.submit(server);

        PredictionModel model = new DenseModel(16777216, false);
        model.configureClock();
        MixClient client = new MixClient(MixEventName.average, "scenario1", "localhost:11212", false, 2, model);
        model.setUpdateHandler(client);

        final Random rand = new Random(43);
        for(int i = 0; i < 100000; i++) {
            Integer feature = Integer.valueOf(rand.nextInt(100));
            float weight = (float) rand.nextGaussian();
            model.set(feature, new WeightValue(weight));
        }

        Thread.sleep(10 * 1000);
        int numMixed = model.getNumMixed();
        //System.out.println("number of mix events: " + numMixed);
        Assert.assertTrue("number of mix events: " + numMixed, numMixed > 0);

        serverExec.shutdown();
    }

}
