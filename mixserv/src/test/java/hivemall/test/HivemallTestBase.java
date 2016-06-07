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
package hivemall.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

public abstract class HivemallTestBase {
    private final Log logger = LogFactory.getLog(this.getClass());
    private final String packageName = this.getClass().getPackage().getName();

    @Rule
    public final TestName testName = new TestName();

    @Before
    public void beforeEachTest() {
        logger.info("\n\n===== TEST OUTPUT FOR " + packageName + ": '" + testName.getMethodName()
                + "' =====\n");
    }

    @Before
    public void afterEachTest() {
        logger.info("\n\n===== FINISHED " + packageName + ": '" + testName.getMethodName()
                + "' =====\n");
    }
}
