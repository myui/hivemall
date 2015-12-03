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
package hivemall.mix.yarn;

import org.apache.hadoop.yarn.api.records.ContainerId;

import java.nio.ByteBuffer;
import java.util.Map;

// TODO: Move to the inner class of MixClusterTest
public final class RetryDeadMixServerApplicationMaster extends ApplicationMaster {

    @Override
    NMCallbackHandler createNMCallbackHandler() {
        return new LaunchFailNMCallbackHandler(this);
    }

    private class LaunchFailNMCallbackHandler extends NMCallbackHandler {

        public LaunchFailNMCallbackHandler(ApplicationMaster appMaster) {
            super(appMaster, null);
        }

        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {
            // We assume that AM releases containers because of inactive MIX servers
            appMaster.releaseAssignedContainer(containerId);
        }
    }

    public static void main(String[] args) {
        main(new RetryDeadMixServerApplicationMaster(), args);
    }
}
