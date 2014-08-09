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
import hivemall.common.DenseModel;
import hivemall.common.PredictionModel;
import hivemall.common.SpaceEfficientDenseModel;
import hivemall.common.SparseModel;
import hivemall.common.WeightValue;
import hivemall.common.WeightValue.WeightValueWithCovar;
import hivemall.utils.datetime.StopWatch;
import hivemall.utils.hadoop.HadoopUtils;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Primitives;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
    private static final Log logger = LogFactory.getLog(LearnerBaseUDTF.class);

    protected String preloadedModelFile;
    protected boolean dense_model;
    protected int model_dims;
    protected boolean disable_halffloat;

    public LearnerBaseUDTF() {}

    protected boolean useCovariance() {
        return false;
    }

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("loadmodel", true, "Model file name in the distributed cache");
        opts.addOption("dense", "densemodel", false, "Use dense model or not");
        opts.addOption("dims", "feature_dimensions", true, "The dimension of model [default: 16777216 (2^24)]");
        opts.addOption("disable_halffloat", false, "Toggle this option to disable the use of SpaceEfficientDenseModel");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        String modelfile = null;
        boolean denseModel = false;
        int modelDims = -1;
        boolean disableHalfFloat = false;

        CommandLine cl = null;
        if(argOIs.length >= 3) {
            String rawArgs = HiveUtils.getConstString(argOIs[2]);
            cl = parseOptions(rawArgs);

            modelfile = cl.getOptionValue("loadmodel");

            denseModel = cl.hasOption("dense");
            if(denseModel) {
                modelDims = Primitives.parseInt(cl.getOptionValue("dims"), 16777216);
            }

            disableHalfFloat = cl.hasOption("disable_halffloat");
        }

        this.preloadedModelFile = modelfile;
        this.dense_model = denseModel;
        this.model_dims = modelDims;
        this.disable_halffloat = disableHalfFloat;
        return cl;
    }

    protected PredictionModel createModel() {
        if(dense_model) {
            if(model_dims > 16777216) {
                return new SpaceEfficientDenseModel(model_dims, useCovariance());
            } else {
                return new DenseModel(model_dims, useCovariance());
            }
        } else {
            return new SparseModel(getInitialModelSize());
        }
    }

    protected int getInitialModelSize() {
        return 16384;
    }

    protected void loadPredictionModel(PredictionModel model, String filename, PrimitiveObjectInspector keyOI) {
        final StopWatch elapsed = new StopWatch();
        final long lines;
        try {
            if(useCovariance()) {
                lines = loadPredictionModel(model, new File(filename), keyOI, writableFloatObjectInspector, writableFloatObjectInspector);
            } else {
                lines = loadPredictionModel(model, new File(filename), keyOI, writableFloatObjectInspector);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load a model: " + filename, e);
        } catch (SerDeException e) {
            throw new RuntimeException("Failed to load a model: " + filename, e);
        }
        if(model.size() > 0) {
            logger.info("Loaded " + model.size() + " features from distributed cache '" + filename
                    + "' (" + lines + " lines) in " + elapsed);
        }
    }

    private static long loadPredictionModel(PredictionModel model, File file, PrimitiveObjectInspector keyOI, WritableFloatObjectInspector valueOI)
            throws IOException, SerDeException {
        long count = 0L;
        if(!file.exists()) {
            return count;
        }
        if(!file.getName().endsWith(".crc")) {
            if(file.isDirectory()) {
                for(File f : file.listFiles()) {
                    count += loadPredictionModel(model, f, keyOI, valueOI);
                }
            } else {
                LazySimpleSerDe serde = HiveUtils.getKeyValueLineSerde(keyOI, valueOI);
                StructObjectInspector lineOI = (StructObjectInspector) serde.getObjectInspector();
                StructField keyRef = lineOI.getStructFieldRef("key");
                StructField valueRef = lineOI.getStructFieldRef("value");
                PrimitiveObjectInspector keyRefOI = (PrimitiveObjectInspector) keyRef.getFieldObjectInspector();
                FloatObjectInspector varRefOI = (FloatObjectInspector) valueRef.getFieldObjectInspector();

                final BufferedReader reader = HadoopUtils.getBufferedReader(file);
                try {
                    String line;
                    while((line = reader.readLine()) != null) {
                        count++;
                        Text lineText = new Text(line);
                        Object lineObj = serde.deserialize(lineText);
                        List<Object> fields = lineOI.getStructFieldsDataAsList(lineObj);
                        Object f0 = fields.get(0);
                        Object f1 = fields.get(1);
                        if(f0 == null || f1 == null) {
                            continue; // avoid the case that key or value is null
                        }
                        Object k = keyRefOI.getPrimitiveWritableObject(keyRefOI.copyObject(f0));
                        float v = varRefOI.get(f1);
                        model.set(k, new WeightValue(v, false));
                    }
                } finally {
                    reader.close();
                }
            }
        }
        return count;
    }

    private static long loadPredictionModel(PredictionModel model, File file, PrimitiveObjectInspector featureOI, WritableFloatObjectInspector weightOI, WritableFloatObjectInspector covarOI)
            throws IOException, SerDeException {
        long count = 0L;
        if(!file.exists()) {
            return count;
        }
        if(!file.getName().endsWith(".crc")) {
            if(file.isDirectory()) {
                for(File f : file.listFiles()) {
                    count += loadPredictionModel(model, f, featureOI, weightOI, covarOI);
                }
            } else {
                LazySimpleSerDe serde = HiveUtils.getLineSerde(featureOI, weightOI, covarOI);
                StructObjectInspector lineOI = (StructObjectInspector) serde.getObjectInspector();
                StructField c1ref = lineOI.getStructFieldRef("c1");
                StructField c2ref = lineOI.getStructFieldRef("c2");
                StructField c3ref = lineOI.getStructFieldRef("c3");
                PrimitiveObjectInspector c1oi = (PrimitiveObjectInspector) c1ref.getFieldObjectInspector();
                FloatObjectInspector c2oi = (FloatObjectInspector) c2ref.getFieldObjectInspector();
                FloatObjectInspector c3oi = (FloatObjectInspector) c3ref.getFieldObjectInspector();

                final BufferedReader reader = HadoopUtils.getBufferedReader(file);
                try {
                    String line;
                    while((line = reader.readLine()) != null) {
                        count++;
                        Text lineText = new Text(line);
                        Object lineObj = serde.deserialize(lineText);
                        List<Object> fields = lineOI.getStructFieldsDataAsList(lineObj);
                        Object f0 = fields.get(0);
                        Object f1 = fields.get(1);
                        Object f2 = fields.get(2);
                        if(f0 == null || f1 == null) {
                            continue; // avoid unexpected case
                        }
                        Object k = c1oi.getPrimitiveWritableObject(c1oi.copyObject(f0));
                        float v = c2oi.get(f1);
                        float cov = (f2 == null) ? WeightValueWithCovar.DEFAULT_COVAR
                                : c3oi.get(f2);
                        model.set(k, new WeightValueWithCovar(v, cov, false));
                    }
                } finally {
                    reader.close();
                }
            }
        }
        return count;
    }
}
