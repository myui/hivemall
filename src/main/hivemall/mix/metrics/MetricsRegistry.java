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
        final ObjectName name = makeMBeanName("hivemall", MixServerMetricsMBean.class.getSimpleName(), "port="
                + port);
        try {
            server.registerMBean(mbean, name);
            logger.info("Registered MBean: " + name);
        } catch (Exception e) {
            logger.error("Failed registering mbean: " + name, e);
        }
    }

    public static void unregisterMBeans(int port) {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName name = makeMBeanName("hivemall", MixServerMetricsMBean.class.getSimpleName(), "port="
                + port);
        try {
            server.unregisterMBean(name);
            logger.info("Unregistered MBean: " + name);
        } catch (Exception e) {
            logger.warn("Failed unregistering mbean: " + name);
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
