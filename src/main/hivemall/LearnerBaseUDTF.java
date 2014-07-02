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
import hivemall.common.WeightValue.WeightValueWithCovar;
import hivemall.utils.collections.OpenHashMap;
import hivemall.utils.hadoop.HadoopUtils;
import hivemall.utils.hadoop.HiveUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableFloatObjectInspector;
import org.apache.hadoop.io.Text;

public abstract class LearnerBaseUDTF extends UDTFWithOptions {

    protected boolean feature_hashing;
    protected float bias;
    protected String preloadedModelFile;

    public LearnerBaseUDTF() {}

    protected boolean returnCovariance() {
        return false;
    }

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("fh", "fhash", false, "Enable feature hashing (only used when feature is TEXT type) [default: off]");
        opts.addOption("b", "bias", true, "Bias clause [default 0.0 (disable)]");
        opts.addOption("loadmodel", true, "Model file name in the distributed cache");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        boolean fhashFlag = false;
        float biasValue = 0.f;
        String modelfile = null;

        CommandLine cl = null;
        if(argOIs.length >= 3) {
            String rawArgs = HiveUtils.getConstString(argOIs[2]);
            cl = parseOptions(rawArgs);

            if(cl.hasOption("fh")) {
                fhashFlag = true;
            }

            String biasStr = cl.getOptionValue("b");
            if(biasStr != null) {
                biasValue = Float.parseFloat(biasStr);
            }

            modelfile = cl.getOptionValue("loadmodel");
        }

        this.feature_hashing = fhashFlag;
        this.bias = biasValue;
        this.preloadedModelFile = modelfile;
        return cl;
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
                StructField keyRef = lineOI.getStructFieldRef("key");
                StructField valueRef = lineOI.getStructFieldRef("value");

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
                        Object k = ((PrimitiveObjectInspector) keyRef.getFieldObjectInspector()).getPrimitiveWritableObject(f0);
                        float v = ((FloatObjectInspector) valueRef.getFieldObjectInspector()).get(f1);
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
                    loadPredictionModel(map, f, keyOI, valueOI, covarOI);
                }
            } else {
                LazySimpleSerDe serde = HiveUtils.getLineSerde(keyOI, valueOI, covarOI);
                StructObjectInspector lineOI = (StructObjectInspector) serde.getObjectInspector();
                StructField c1ref = lineOI.getStructFieldRef("c1");
                StructField c2ref = lineOI.getStructFieldRef("c2");
                StructField c3ref = lineOI.getStructFieldRef("c3");

                final BufferedReader reader = HadoopUtils.getBufferedReader(file);
                try {
                    String line;
                    while((line = reader.readLine()) != null) {
                        Text lineText = new Text(line);
                        Object lineObj = serde.deserialize(lineText);
                        List<Object> fields = lineOI.getStructFieldsDataAsList(lineObj);
                        Object f0 = fields.get(0);
                        Object f1 = fields.get(1);
                        Object f2 = fields.get(2);
                        if(f0 == null || f1 == null || f2 == null) {
                            continue; // avoid unexpected case
                        }
                        Object k = ((PrimitiveObjectInspector) c1ref.getFieldObjectInspector()).getPrimitiveWritableObject(f0);
                        float v = ((FloatObjectInspector) c2ref.getFieldObjectInspector()).get(f1);
                        float cov = ((FloatObjectInspector) c3ref.getFieldObjectInspector()).get(f2);
                        map.put(k, new WeightValueWithCovar(v, cov));
                    }
                } finally {
                    reader.close();
                }
            }
        }
    }
}
