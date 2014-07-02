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
package hivemall;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
import hivemall.common.WeightValue;
import hivemall.utils.collections.OpenHashMap;
import hivemall.utils.hadoop.HadoopUtils;
import hivemall.utils.hadoop.HiveUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableFloatObjectInspector;
import org.apache.hadoop.io.Text;

public abstract class LearnerBaseUDTF extends UDTFWithOptions {

    protected boolean returnCovariance() {
        return true;
    }

    protected void loadPredictionModel(OpenHashMap<Object, WeightValue> map, String filename, PrimitiveObjectInspector keyOI) {
        try {
            if(returnCovariance()) {
                loadPredictionModel(map, new File(filename), keyOI, writableFloatObjectInspector, writableFloatObjectInspector);
            } else {
                loadPredictionModel(map, new File(filename), keyOI, writableFloatObjectInspector);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load a model: " + filename, e);
        } catch (SerDeException e) {
            throw new RuntimeException("Failed to load a model: " + filename, e);
        }
    }

    private static void loadPredictionModel(OpenHashMap<Object, WeightValue> map, File file, PrimitiveObjectInspector keyOI, WritableFloatObjectInspector valueOI)
            throws IOException, SerDeException {
        if(!file.exists()) {
            return;
        }
        if(!file.getName().endsWith(".crc")) {
            if(file.isDirectory()) {
                for(File f : file.listFiles()) {
                    loadPredictionModel(map, f, keyOI, valueOI);
                }
            } else {
                LazySimpleSerDe serde = HiveUtils.getKeyValueLineSerde(keyOI, valueOI);
                StructObjectInspector lineOI = (StructObjectInspector) serde.getObjectInspector();

                final BufferedReader reader = HadoopUtils.getBufferedReader(file);
                try {
                    String line;
                    while((line = reader.readLine()) != null) {
                        Text lineText = new Text(line);
                        Object lineObj = serde.deserialize(lineText);
                        List<Object> fields = lineOI.getStructFieldsDataAsList(lineObj);
                        Object f0 = fields.get(0);
                        Object f1 = fields.get(1);
                        if(f0 == null || f1 == null) {
                            continue; // avoid the case that key or value is null
                        }
                        Object k = ObjectInspectorUtils.copyToStandardObject(f0, keyOI);
                        float v = valueOI.get(f1);
                        map.put(k, new WeightValue(v));
                    }
                } finally {
                    reader.close();
                }
            }
        }
    }

    private static void loadPredictionModel(OpenHashMap<Object, WeightValue> map, File file, PrimitiveObjectInspector keyOI, WritableFloatObjectInspector valueOI, WritableFloatObjectInspector covarOI)
            throws IOException, SerDeException {
        if(!file.exists()) {
            return;
        }
        if(!file.getName().endsWith(".crc")) {
            if(file.isDirectory()) {
                for(File f : file.listFiles()) {
                    loadPredictionModel(map, f, keyOI, valueOI);
                }
            } else {
                LazySimpleSerDe serde = HiveUtils.getKeyValueLineSerde(keyOI, valueOI);
                StructObjectInspector lineOI = (StructObjectInspector) serde.getObjectInspector();

                final BufferedReader reader = HadoopUtils.getBufferedReader(file);
                try {
                    String line;
                    while((line = reader.readLine()) != null) {
                        Text lineText = new Text(line);
                        Object lineObj = serde.deserialize(lineText);
                        List<Object> fields = lineOI.getStructFieldsDataAsList(lineObj);
                        Object f0 = fields.get(0);
                        Object f1 = fields.get(1);
                        if(f0 == null || f1 == null) {
                            continue; // avoid the case that key or value is null
                        }
                        Object k = ObjectInspectorUtils.copyToStandardObject(f0, keyOI);
                        float v = valueOI.get(f1);
                        map.put(k, new WeightValue(v));
                    }
                } finally {
                    reader.close();
                }
            }
        }
    }
}
