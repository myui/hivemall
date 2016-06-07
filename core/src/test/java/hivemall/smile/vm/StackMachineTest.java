/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (c) 2010 Haifeng Li
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
package hivemall.smile.vm;

import static org.junit.Assert.assertEquals;
import hivemall.utils.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Assert;
import org.junit.Test;

public class StackMachineTest {
    private static final boolean DEBUG = false;

    @Test
    public void testFindInfinteLoop() throws IOException, ParseException, HiveException,
            VMRuntimeException {
        // Sample of machine code having infinite loop
        ArrayList<String> opScript = new ArrayList<String>();
        opScript.add("push 2.0");
        opScript.add("push 1.0");
        opScript.add("iflt 0");
        opScript.add("push 1");
        opScript.add("call end");
        debugPrint(opScript);
        double[] x = new double[0];
        StackMachine sm = new StackMachine();
        try {
            sm.run(opScript, x);
            Assert.fail("VMRuntimeException is expected");
        } catch (VMRuntimeException ex) {
            assertEquals("There is a infinite loop in the Machine code.", ex.getMessage());
        }
    }

    @Test
    public void testLargeOpcodes() throws IOException, ParseException, HiveException,
            VMRuntimeException {
        URL url = new URL(
            "https://gist.githubusercontent.com/myui/b1a8e588f5750e3b658c/raw/a4074d37400dab2b13a2f43d81f5166188d3461a/vmtest01.txt");
        InputStream is = new BufferedInputStream(url.openStream());
        String opScript = IOUtils.toString(is);

        StackMachine sm = new StackMachine();
        sm.compile(opScript);

        double[] x1 = new double[] {36, 2, 1, 2, 0, 436, 1, 0, 0, 13, 0, 567, 1, 595, 2, 1};
        sm.eval(x1);
        assertEquals(0.d, sm.getResult().doubleValue(), 0d);

        double[] x2 = {31, 2, 1, 2, 0, 354, 1, 0, 0, 30, 0, 502, 1, 9, 2, 2};
        sm.eval(x2);
        assertEquals(1.d, sm.getResult().doubleValue(), 0d);

        double[] x3 = {39, 0, 0, 0, 0, 1756, 0, 0, 0, 3, 0, 939, 1, 0, 0, 0};
        sm.eval(x3);
        assertEquals(0.d, sm.getResult().doubleValue(), 0d);
    }

    private static void debugPrint(Object msg) {
        if (DEBUG) {
            System.out.println(msg);
        }
    }
}
