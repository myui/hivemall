/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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
package hivemall.smile.tools;

import hivemall.smile.ModelType;
import hivemall.smile.classification.DecisionTree;
import hivemall.smile.regression.RegressionTree;
import hivemall.smile.vm.StackMachine;
import hivemall.smile.vm.VMRuntimeException;
import hivemall.utils.codec.Base91;
import hivemall.utils.codec.DeflateCodec;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

@Description(
        name = "tree_predict",
        value = "_FUNC_(int modelId, int modelType, string script, array<double> features [, const boolean classification])"
                + " - Returns a prediction result of a random forest")
@UDFType(deterministic = true, stateful = false)
public final class TreePredictUDF extends GenericUDF {

    private boolean classification;
    private IntObjectInspector modelIdOI;
    private IntObjectInspector modelTypeOI;
    private StringObjectInspector stringOI;
    private ListObjectInspector featureListOI;
    private PrimitiveObjectInspector featureElemOI;

    @Nullable
    private transient Evaluator evaluator;
    private boolean support_javascript_eval = true;

    @Override
    public void configure(MapredContext context) {
        super.configure(context);

        if (context != null) {
            JobConf conf = context.getJobConf();
            String tdJarVersion = conf.get("td.jar.version");
            if (tdJarVersion != null) {
                this.support_javascript_eval = false;
            }
        }
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 4 && argOIs.length != 5) {
            throw new UDFArgumentException("_FUNC_ takes 4 or 5 arguments");
        }

        this.modelIdOI = HiveUtils.asIntOI(argOIs[0]);
        this.modelTypeOI = HiveUtils.asIntOI(argOIs[1]);
        this.stringOI = HiveUtils.asStringOI(argOIs[2]);
        ListObjectInspector listOI = HiveUtils.asListOI(argOIs[3]);
        this.featureListOI = listOI;
        ObjectInspector elemOI = listOI.getListElementObjectInspector();
        this.featureElemOI = HiveUtils.asDoubleCompatibleOI(elemOI);

        boolean classification = false;
        if (argOIs.length == 5) {
            classification = HiveUtils.getConstBoolean(argOIs[4]);
        }
        this.classification = classification;

        if (classification) {
            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        } else {
            return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        }
    }

    @Override
    public Writable evaluate(@Nonnull DeferredObject[] arguments) throws HiveException {
        Object arg0 = arguments[0].get();
        int modelId = modelIdOI.get(arg0);

        Object arg1 = arguments[1].get();
        int modelTypeId = modelTypeOI.get(arg1);
        ModelType modelType = ModelType.resolve(modelTypeId);

        Object arg2 = arguments[2].get();
        if (arg2 == null) {
            return null;
        }
        Text script = stringOI.getPrimitiveWritableObject(arg2);

        Object arg3 = arguments[3].get();
        if (arg3 == null) {
            throw new HiveException("array<double> features was null");
        }
        double[] features = HiveUtils.asDoubleArray(arg3, featureListOI, featureElemOI);

        if (evaluator == null) {
            this.evaluator = getEvaluator(modelType, support_javascript_eval);
        }

        Writable result = evaluator.evaluate(modelId, modelType.isCompressed(), script, features,
            classification);
        return result;
    }

    @Nonnull
    private static Evaluator getEvaluator(@Nonnull ModelType type, boolean supportJavascriptEval)
            throws UDFArgumentException {
        final Evaluator evaluator;
        switch (type) {
            case serialization:
            case serialization_compressed: {
                evaluator = new JavaSerializationEvaluator();
                break;
            }
            case opscode:
            case opscode_compressed: {
                evaluator = new StackmachineEvaluator();
                break;
            }
            case javascript:
            case javascript_compressed: {
                if (!supportJavascriptEval) {
                    throw new UDFArgumentException(
                        "Javascript evaluation is not allowed in Treasure Data env");
                }
                evaluator = new JavascriptEvaluator();
                break;
            }
            default:
                throw new UDFArgumentException("Unexpected model type was detected: " + type);
        }
        return evaluator;
    }

    @Override
    public void close() throws IOException {
        this.modelIdOI = null;
        this.modelTypeOI = null;
        this.stringOI = null;
        this.featureElemOI = null;
        this.featureListOI = null;
        IOUtils.closeQuietly(evaluator);
        this.evaluator = null;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "tree_predict(" + Arrays.toString(children) + ")";
    }

    public interface Evaluator extends Closeable {

        @Nullable
        Writable evaluate(int modelId, boolean compressed, @Nonnull final Text script,
                @Nonnull final double[] features, final boolean classification)
                throws HiveException;

    }

    static final class JavaSerializationEvaluator implements Evaluator {

        private int prevModelId = -1;
        private DecisionTree.Node cNode = null;
        private RegressionTree.Node rNode = null;

        JavaSerializationEvaluator() {}

        @Override
        public Writable evaluate(int modelId, boolean compressed, @Nonnull Text script,
                double[] features, boolean classification) throws HiveException {
            if (classification) {
                return evaluateClassification(modelId, compressed, script, features);
            } else {
                return evaluteRegression(modelId, compressed, script, features);
            }
        }

        private IntWritable evaluateClassification(int modelId, boolean compressed,
                @Nonnull Text script, double[] features) throws HiveException {
            if (modelId != prevModelId) {
                this.prevModelId = modelId;
                int length = script.getLength();
                byte[] b = script.getBytes();
                b = Base91.decode(b, 0, length);
                this.cNode = DecisionTree.deserializeNode(b, b.length, compressed);
            }
            assert (cNode != null);
            int result = cNode.predict(features);
            return new IntWritable(result);
        }

        private DoubleWritable evaluteRegression(int modelId, boolean compressed,
                @Nonnull Text script, double[] features) throws HiveException {
            if (modelId != prevModelId) {
                this.prevModelId = modelId;
                int length = script.getLength();
                byte[] b = script.getBytes();
                b = Base91.decode(b, 0, length);
                this.rNode = RegressionTree.deserializeNode(b, b.length, compressed);
            }
            assert (rNode != null);
            double result = rNode.predict(features);
            return new DoubleWritable(result);
        }

        @Override
        public void close() throws IOException {}

    }

    static final class StackmachineEvaluator implements Evaluator {

        private int prevModelId = -1;
        private StackMachine prevVM = null;
        private DeflateCodec codec = null;

        StackmachineEvaluator() {}

        @Override
        public Writable evaluate(int modelId, boolean compressed, @Nonnull Text script,
                double[] features, boolean classification) throws HiveException {
            final String scriptStr;
            if (compressed) {
                if (codec == null) {
                    this.codec = new DeflateCodec(false, true);
                }
                byte[] b = script.getBytes();
                int len = script.getLength();
                b = Base91.decode(b, 0, len);
                try {
                    b = codec.decompress(b);
                } catch (IOException e) {
                    throw new HiveException("decompression failed", e);
                }
                scriptStr = new String(b);
            } else {
                scriptStr = script.toString();
            }

            final StackMachine vm;
            if (modelId == prevModelId) {
                vm = prevVM;
            } else {
                vm = new StackMachine();
                try {
                    vm.compile(scriptStr);
                } catch (VMRuntimeException e) {
                    throw new HiveException("failed to compile StackMachine", e);
                }
                this.prevModelId = modelId;
                this.prevVM = vm;
            }

            try {
                vm.eval(features);
            } catch (VMRuntimeException vme) {
                throw new HiveException("failed to eval StackMachine", vme);
            } catch (Throwable e) {
                throw new HiveException("failed to eval StackMachine", e);
            }

            Double result = vm.getResult();
            if (result == null) {
                return null;
            }
            if (classification) {
                return new IntWritable(result.intValue());
            } else {
                return new DoubleWritable(result.doubleValue());
            }
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeQuietly(codec);
        }

    }

    static final class JavascriptEvaluator implements Evaluator {

        private final ScriptEngine scriptEngine;
        private final Compilable compilableEngine;

        private int prevModelId = -1;
        private CompiledScript prevCompiled;

        private DeflateCodec codec = null;

        JavascriptEvaluator() throws UDFArgumentException {
            ScriptEngineManager manager = new ScriptEngineManager();
            ScriptEngine engine = manager.getEngineByExtension("js");
            if (!(engine instanceof Compilable)) {
                throw new UDFArgumentException("ScriptEngine was not compilable: "
                        + engine.getFactory().getEngineName() + " version "
                        + engine.getFactory().getEngineVersion());
            }
            this.scriptEngine = engine;
            this.compilableEngine = (Compilable) engine;
        }

        @Override
        public Writable evaluate(int modelId, boolean compressed, @Nonnull Text script,
                double[] features, boolean classification) throws HiveException {
            final String scriptStr;
            if (compressed) {
                if (codec == null) {
                    this.codec = new DeflateCodec(false, true);
                }
                byte[] b = script.getBytes();
                int len = script.getLength();
                b = Base91.decode(b, 0, len);
                try {
                    b = codec.decompress(b);
                } catch (IOException e) {
                    throw new HiveException("decompression failed", e);
                }
                scriptStr = new String(b);
            } else {
                scriptStr = script.toString();
            }

            final CompiledScript compiled;
            if (modelId == prevModelId) {
                compiled = prevCompiled;
            } else {
                try {
                    compiled = compilableEngine.compile(scriptStr);
                } catch (ScriptException e) {
                    throw new HiveException("failed to compile: \n" + script, e);
                }
                this.prevCompiled = compiled;
            }

            final Bindings bindings = scriptEngine.createBindings();
            final Object result;
            try {
                bindings.put("x", features);
                result = compiled.eval(bindings);
            } catch (ScriptException se) {
                throw new HiveException("failed to evaluate: \n" + script, se);
            } catch (Throwable e) {
                throw new HiveException("failed to evaluate: \n" + script, e);
            } finally {
                bindings.clear();
            }

            if (result == null) {
                return null;
            }
            if (!(result instanceof Number)) {
                throw new HiveException("Got an unexpected non-number result: " + result);
            }
            if (classification) {
                Number casted = (Number) result;
                return new IntWritable(casted.intValue());
            } else {
                Number casted = (Number) result;
                return new DoubleWritable(casted.doubleValue());
            }
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeQuietly(codec);
        }

    }

}
