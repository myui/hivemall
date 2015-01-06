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
package com.facebook.hiveio.input;

import hivemall.utils.hadoop.HadoopCompat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;

@SuppressWarnings("deprecation")
public final class HiveIOUtils {

    private HiveIOUtils() {}

    @Nonnull
    public static StructObjectInspector toJavaObjectOI(@Nonnull StructObjectInspector inputOI) {
        List<? extends StructField> fields = inputOI.getAllStructFieldRefs();
        int size = fields.size();
        final List<String> fieldNames = new ArrayList<String>(size);
        final List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(size);
        for(int i = 1; i < size; i++) {
            StructField field = fields.get(i);
            String fieldName = field.getFieldName();
            fieldNames.add(fieldName);
            ObjectInspector fieldOI = field.getFieldObjectInspector();
            ObjectInspector fieldJavaOI = ObjectInspectorUtils.getStandardObjectInspector(fieldOI, ObjectInspectorCopyOption.JAVA);
            fieldOIs.add(fieldJavaOI);
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Nullable
    public static StructObjectInspector getStructObjectInspector(@Nonnull final HiveInputDescription inputDesc, @Nonnull final Configuration conf)
            throws SerDeException, IOException, InterruptedException {
        String profileID = Long.toString(System.currentTimeMillis());

        HiveApiInputFormat.setProfileInputDesc(conf, inputDesc, profileID);

        final HiveApiInputFormat inputFormat = new HiveApiInputFormat();
        inputFormat.setMyProfileId(profileID);

        JobContext jobContext = HadoopCompat.newJobContext(conf, new JobID());
        List<InputSplit> splits = inputFormat.getSplits(jobContext);

        if(splits.isEmpty()) {
            return null;
        }

        InputSplit inputSplit = splits.get(0);
        return getStructObjectInspector(inputSplit);
    }

    @Nonnull
    public static StructObjectInspector getStructObjectInspector(@Nonnull InputSplit inputSplit)
            throws SerDeException {
        return getStructObjectInspector((HInputSplit) inputSplit);
    }

    @Nonnull
    private static StructObjectInspector getStructObjectInspector(@Nonnull HInputSplit inputSplit)
            throws SerDeException {
        Deserializer de = inputSplit.getDeserializer();
        return (StructObjectInspector) de.getObjectInspector();
    }

}
