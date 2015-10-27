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
package hivemall.mix.metrics;

import java.lang.management.ManagementFactory;

import javax.annotation.Nonnull;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import hivemall.utils.logging.Logging;

public final class MetricsRegistry {
    private static final Logging logger = new Logging() {};

    public static void registerMBeans(@Nonnull MixServerMetrics metrics, int port) {
        final StandardMBean mbean;
        try {
            mbean = new StandardMBean(metrics, MixServerMetricsMBean.class);
        } catch (NotCompliantMBeanException e) {
            logger.logError("Unexpected as GridNodeMetricsMBean: " + metrics.getClass().getName(), e);
            return;
        }
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName name = makeMBeanName("hivemall", MixServerMetricsMBean.class.getSimpleName(), "port="
                + port);
        try {
            server.registerMBean(mbean, name);
            logger.logInfo("Registered MBean: " + name);
        } catch (Exception e) {
            logger.logError("Failed registering mbean: " + name, e);
        }
    }

    public static void unregisterMBeans(int port) {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName name = makeMBeanName("hivemall", MixServerMetricsMBean.class.getSimpleName(), "port="
                + port);
        try {
            server.unregisterMBean(name);
            logger.logInfo("Unregistered MBean: " + name);
        } catch (Exception e) {
            logger.logWarning("Failed unregistering mbean: " + name);
        }
    }

    private static ObjectName makeMBeanName(@Nonnull final String domain, @Nonnull final String type, @Nonnull final String channelName) {
        final String mbeanName = makeMBeanNameString(domain, type, channelName);
        try {
            return new ObjectName(mbeanName);
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException(e);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static String makeMBeanNameString(@Nonnull final String domain, @Nonnull final String type, @Nonnull final String channelName) {
        return domain + ":type=" + type + ',' + channelName;
    }
}
