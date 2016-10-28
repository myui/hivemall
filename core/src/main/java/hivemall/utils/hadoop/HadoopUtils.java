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
package hivemall.utils.hadoop;

import hivemall.utils.lang.RandomUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.MapredContextAccessor;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskID;

public final class HadoopUtils {

    private HadoopUtils() {}

    public static BufferedReader getBufferedReader(File file) throws IOException {
        MapredContext context = MapredContextAccessor.get();
        return getBufferedReader(file, context);
    }

    public static BufferedReader getBufferedReader(File file, MapredContext context)
            throws IOException {
        URI fileuri = file.toURI();
        Path path = new Path(fileuri);

        Configuration conf = context.getJobConf();
        CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
        CompressionCodec codec = ccf.getCodec(path);

        if (codec == null) {
            return new BufferedReader(new FileReader(file));
        } else {
            Decompressor decompressor = CodecPool.getDecompressor(codec);
            FileInputStream fis = new FileInputStream(file);
            CompressionInputStream cis = codec.createInputStream(fis, decompressor);
            BufferedReader br = new BufferedReaderExt(new InputStreamReader(cis), decompressor);
            return br;
        }
    }

    private static final class BufferedReaderExt extends BufferedReader {

        private Decompressor decompressor;

        BufferedReaderExt(Reader in, Decompressor decompressor) {
            super(in);
            this.decompressor = decompressor;
        }

        @Override
        public void close() throws IOException {
            super.close();
            if (decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
                this.decompressor = null;
            }
        }

    }

    @Nonnull
    public static String getJobId() {
        MapredContext ctx = MapredContextAccessor.get();
        if (ctx == null) {
            throw new IllegalStateException("MapredContext is not set");
        }
        JobConf conf = ctx.getJobConf();
        if (conf == null) {
            throw new IllegalStateException("JobConf is not set");
        }
        String jobId = conf.get("mapred.job.id");
        if (jobId == null) {
            jobId = conf.get("mapreduce.job.id");
            if (jobId == null) {
                String queryId = conf.get("hive.query.id");
                if (queryId != null) {
                    return queryId;
                }
                String taskidStr = conf.get("mapred.task.id");
                if (taskidStr == null) {
                    throw new IllegalStateException("Cannot resolve jobId: " + toString(conf));
                }
                jobId = getJobIdFromTaskId(taskidStr);
            }
        }
        return jobId;
    }

    @Nonnull
    public static String getJobIdFromTaskId(@Nonnull String taskidStr) {
        if (!taskidStr.startsWith("task_")) {// workaround for Tez
            taskidStr = taskidStr.replace("task", "task_");
            taskidStr = taskidStr.substring(0, taskidStr.lastIndexOf('_'));
        }
        TaskID taskId = TaskID.forName(taskidStr);
        JobID jobId = taskId.getJobID();
        return jobId.toString();
    }

    public static int getTaskId() {
        MapredContext ctx = MapredContextAccessor.get();
        if (ctx == null) {
            throw new IllegalStateException("MapredContext is not set");
        }
        JobConf jobconf = ctx.getJobConf();
        if (jobconf == null) {
            throw new IllegalStateException("JobConf is not set");
        }
        int taskid = jobconf.getInt("mapred.task.partition", -1);
        if (taskid == -1) {
            taskid = jobconf.getInt("mapreduce.task.partition", -1);
            if (taskid == -1) {
                throw new IllegalStateException(
                    "Both mapred.task.partition and mapreduce.task.partition are not set: "
                            + toString(jobconf));
            }
        }
        return taskid;
    }

    public static int getTaskId(final int defaultValue) {
        MapredContext ctx = MapredContextAccessor.get();
        if (ctx == null) {
            return defaultValue;
        }
        JobConf jobconf = ctx.getJobConf();
        if (jobconf == null) {
            return defaultValue;
        }
        int taskid = jobconf.getInt("mapred.task.partition", -1);
        if (taskid == -1) {
            taskid = jobconf.getInt("mapreduce.task.partition", -1);
            if (taskid == -1) {
                return defaultValue;
            }
        }
        return taskid;
    }

    public static String getUniqueTaskIdString() {
        MapredContext ctx = MapredContextAccessor.get();
        if (ctx != null) {
            JobConf jobconf = ctx.getJobConf();
            if (jobconf != null) {
                int taskid = jobconf.getInt("mapred.task.partition", -1);
                if (taskid == -1) {
                    taskid = jobconf.getInt("mapreduce.task.partition", -1);
                }
                if (taskid != -1) {
                    return String.valueOf(taskid);
                }
            }
        }
        return RandomUtils.getUUID();
    }

    @Nonnull
    public static String toString(@Nonnull JobConf jobconf) {
        return toString(jobconf, null);
    }

    @Nonnull
    public static String toString(@Nonnull JobConf jobconf, @Nullable String regexKey) {
        final Iterator<Entry<String, String>> itor = jobconf.iterator();
        boolean hasNext = itor.hasNext();
        if (!hasNext) {
            return "";
        }
        final StringBuilder buf = new StringBuilder(1024);
        do {
            Entry<String, String> e = itor.next();
            hasNext = itor.hasNext();
            String k = e.getKey();
            if (k == null) {
                continue;
            }
            if (regexKey == null || k.matches(regexKey)) {
                String v = e.getValue();
                buf.append(k).append('=').append(v);
                if (hasNext) {
                    buf.append(',');
                }
            }
        } while (hasNext);
        return buf.toString();
    }
}
