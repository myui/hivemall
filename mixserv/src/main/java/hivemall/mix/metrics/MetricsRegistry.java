/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.mix.metrics;

import java.lang.management.ManagementFactory;

import javax.annotation.Nonnull;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class MetricsRegistry {
    private static final Log logger = LogFactory.getLog(MetricsRegistry.class);

    public static void registerMBeans(@Nonnull MixServerMetrics metrics, int port) {
        final StandardMBean mbean;
        try {
            mbean = new StandardMBean(metrics, MixServerMetricsMBean.class);
        } catch (NotCompliantMBeanException e) {
            logger.error("Unexpected as GridNodeMetricsMBean: " + metrics.getClass().getName(), e);
            return;
        }
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName name = makeMBeanName("hivemall",
            MixServerMetricsMBean.class.getSimpleName(), "port=" + port);
        try {
            server.registerMBean(mbean, name);
            logger.info("Registered MBean: " + name);
        } catch (Exception e) {
            logger.error("Failed registering mbean: " + name, e);
        }
    }

    public static void unregisterMBeans(int port) {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName name = makeMBeanName("hivemall",
            MixServerMetricsMBean.class.getSimpleName(), "port=" + port);
        try {
            server.unregisterMBean(name);
            logger.info("Unregistered MBean: " + name);
        } catch (Exception e) {
            logger.warn("Failed unregistering mbean: " + name);
        }
    }

    private static ObjectName makeMBeanName(@Nonnull final String domain,
            @Nonnull final String type, @Nonnull final String channelName) {
        final String mbeanName = makeMBeanNameString(domain, type, channelName);
        try {
            return new ObjectName(mbeanName);
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException(e);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static String makeMBeanNameString(@Nonnull final String domain,
            @Nonnull final String type, @Nonnull final String channelName) {
        return domain + ":type=" + type + ',' + channelName;
    }
}
