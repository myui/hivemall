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

public class MixYarnEnv {

    // Default port number for resource requests
    public static final int RESOURCE_REQUEST_PORT = 11213;

    // Default port number for report receivers
    public static final int REPORT_RECEIVER_PORT = 11214;

    // Environment key name for preloading custom jar of MIX server implementation
    public static final String MIXSERVER_PRELOAD = "MIXSERVER_PRELOAD";

    // Environment key name pointing to a cluster-wide directory
    public static final String MIXSERVER_RESOURCE_LOCATION = "MIXSERVER_RESOURCE_LOCATION";

    // Environment key name denoting a jar name for containers
    public static final String MIXSERVER_CONTAINER_APP = "MIXSERVER_CONTAINER_APP";

    // Interval between each MIX server's heartbeat to an application master
    public static final long MIXSERVER_HEARTBEAT_INTERVAL = 180L;

    // Separator for MIX servers
    public static final String MIXSERVER_SEPARATOR = ",";

}
