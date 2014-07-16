/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;

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

public final class HadoopUtils {

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

        if(codec == null) {
            return new BufferedReader(new FileReader(file));
        } else {
            Decompressor decompressor = CodecPool.getDecompressor(codec);
            FileInputStream fis = new FileInputStream(file);
            CompressionInputStream cis = codec.createInputStream(fis, decompressor);
            BufferedReader br = new BufferedReaderExt(new InputStreamReader(cis), decompressor);
            return br;
        }
    }

    private static class BufferedReaderExt extends BufferedReader {

        private Decompressor decompressor;

        BufferedReaderExt(Reader in, Decompressor decompressor) {
            super(in);
            this.decompressor = decompressor;
        }

        @Override
        public void close() throws IOException {
            super.close();
            if(decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
                this.decompressor = null;
            }
        }

    }

    public static int getTaskId() {
        MapredContext ctx = MapredContextAccessor.get();
        JobConf conf = ctx.getJobConf();
        int taskid = conf.getInt("mapred.task.partition", -1);
        if(taskid == -1) {
            throw new IllegalStateException("mapred.task.partition is not set");
        }
        return taskid;
    }

}
