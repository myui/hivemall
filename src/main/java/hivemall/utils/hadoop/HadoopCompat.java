/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2015
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
package hivemall.utils.hadoop;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;

public final class HadoopCompat {

    private HadoopCompat() {}

    private static final boolean useV21;
    private static final Constructor<?> JOB_CONTEXT_CONSTRUCTOR;
    private static final Method GET_CONFIGURATION_METHOD;

    static {
        boolean v21 = true;
        final String PACKAGE = "org.apache.hadoop.mapreduce";
        try {
            Class.forName(PACKAGE + ".task.JobContextImpl");
        } catch (ClassNotFoundException cnfe) {
            v21 = false;
        }
        useV21 = v21;

        final Class<?> jobContextCls;
        try {
            if(v21) {
                jobContextCls = Class.forName(PACKAGE + ".task.JobContextImpl");
            } else {
                jobContextCls = Class.forName(PACKAGE + ".JobContext");
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Can't find class", e);
        }

        try {
            JOB_CONTEXT_CONSTRUCTOR = jobContextCls.getConstructor(Configuration.class, JobID.class);
            JOB_CONTEXT_CONSTRUCTOR.setAccessible(true);
            GET_CONFIGURATION_METHOD = Class.forName(PACKAGE + ".JobContext").getMethod("getConfiguration");
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Can't find constructor ", e);
        } catch (SecurityException e) {
            throw new IllegalArgumentException("Can't run constructor ", e);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Can't find class", e);
        }

    }

    /**
     * @return True if runtime Hadoop version is 2.x, false otherwise.
     */
    public static boolean isVersion2x() {
        return useV21;
    }

    /**
     * Creates JobContext from a JobConf and jobId using the correct constructor
     * for based on Hadoop version. <code>jobId</code> could be null.
     */
    public static JobContext newJobContext(Configuration conf, JobID jobId) {
        return (JobContext) newInstance(JOB_CONTEXT_CONSTRUCTOR, conf, jobId);
    }

    /**
     * Invoke getConfiguration() on JobContext. Works with both
     * Hadoop 1 and 2.
     */
    public static Configuration getConfiguration(JobContext context) {
        return (Configuration) invoke(GET_CONFIGURATION_METHOD, context);
    }

    private static Object newInstance(Constructor<?> constructor, Object... args) {
        try {
            return constructor.newInstance(args);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("Can't instantiate " + constructor, e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Can't instantiate " + constructor, e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("Can't instantiate " + constructor, e);
        }
    }

    private static Object invoke(Method method, Object obj, Object... args) {
        try {
            return method.invoke(obj, args);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Can't invoke method " + method.getName(), e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("Can't invoke method " + method.getName(), e);
        }
    }

}
