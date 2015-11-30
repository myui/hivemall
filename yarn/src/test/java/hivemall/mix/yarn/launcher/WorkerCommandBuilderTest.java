/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hivemall.mix.yarn.launcher;

import hivemall.mix.yarn.launcher.WorkerCommandBuilder;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

public class WorkerCommandBuilderTest {

    @Test
    public void testInvokeProcess() {
        WorkerCommandBuilder cmdBuilder = new WorkerCommandBuilder(Launcher.class, "", 512, Arrays.asList("test"), null);
        String output = null;
        int exitCode = -1;
        try {
            List<String> cmd = cmdBuilder.buildCommand();
            final Process process = new ProcessBuilder(cmd).start();
            InputStream input = process.getInputStream();
            output = IOUtils.toString(input);
            exitCode = process.waitFor();
        } catch (Exception e) {
            Assert.fail("Failed to launch a program:" + cmdBuilder);
        }
        Assert.assertEquals("This is a test program! args=[test]", output);
        Assert.assertEquals(0, exitCode);
    }

    public static class Launcher {

        public static void main(String[] args) {
            System.out.print("This is a test program! args=" + Arrays.toString(args));
        }
    }

}
