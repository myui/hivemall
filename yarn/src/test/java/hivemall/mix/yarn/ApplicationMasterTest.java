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

import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public final class ApplicationMasterTest {

    private static class TestApplicationMaster extends ApplicationMaster {
        private int numThreadLaunched = 0;

        @Override
        protected Runnable createLaunchContainerThread(
                Container container, ContainerLaunchInfo cmdInfo) {
            numThreadLaunched++;
            return new Runnable() {
                @Override
                public void run() {}
            };
        }
    }

    @Test
    public void testApplicationMasterAllocateHandler() throws Exception {
        final int numRequestContainers = 2;

        TestApplicationMaster master = new TestApplicationMaster();
        AMRMClientAsync mockClient = Mockito.mock(AMRMClientAsync.class);

        master.setAmRMClient(mockClient);
        master.setNumContainers(numRequestContainers);
        Mockito.doNothing().when(mockClient)
            .addContainerRequest(Matchers.any(AMRMClient.ContainerRequest.class));

        ApplicationMaster.RMCallbackHandler handler = master.getRMCallbackHandler();

        final List<Container> containers = new ArrayList<Container>(1);
        ContainerId id1 = BuilderUtils.newContainerId(1, 1, 1, 1);
        containers.add(createContainer(id1));

        master.setNumRequestedContainers(numRequestContainers);

        // First allocate a single container, everything should be fine
        handler.onContainersAllocated(containers);
        Assert.assertEquals(1, master.getNumAllocatedContainers());
        Mockito.verifyZeroInteractions(mockClient);
        Assert.assertEquals(1, master.numThreadLaunched);

        // Now send 3 extra containers
        containers.clear();
        ContainerId id2 = BuilderUtils.newContainerId(1, 1, 1, 2);
        containers.add(createContainer(id2));
        ContainerId id3 = BuilderUtils.newContainerId(1, 1, 1, 3);
        containers.add(createContainer(id3));
        ContainerId id4 = BuilderUtils.newContainerId(1, 1, 1, 4);
        containers.add(createContainer(id4));
        handler.onContainersAllocated(containers);
        Assert.assertEquals(2, master.getNumAllocatedContainers());
        Assert.assertEquals(2, master.numThreadLaunched);

        // Make sure we handle retries for failed containers
        final List<ContainerStatus> status = new ArrayList<ContainerStatus>();
        status.add(createContainerStatus(id1, ContainerExitStatus.ABORTED));
        handler.onContainersCompleted(status);
        Assert.assertEquals(1, master.getNumFailedContainers());
        Assert.assertEquals(1, handler.getNumRetries());
        Assert.assertFalse(master.isTerminated());

        // Finish AM when containers killed
        status.clear();
        status.add(createContainerStatus(id2, 143));
        handler.onContainersCompleted(status);
        Assert.assertEquals(2, master.getNumFailedContainers());
        Assert.assertEquals(1, handler.getNumRetries());
        Assert.assertTrue(master.isTerminated());
    }

    private Container createContainer(ContainerId cid) {
        return Container.newInstance(
                cid, NodeId.newInstance("host", 5000), "host:80",
                Resource.newInstance(1024, 1), Priority.newInstance(0), null);
    }

    private ContainerStatus createContainerStatus(ContainerId id, int exitStatus) {
        return ContainerStatus.newInstance(id, ContainerState.COMPLETE, "", exitStatus);
    }
}
