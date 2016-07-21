/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2016 Makoto YUI
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

package hivemall.ftvec;

import hivemall.utils.collections.ComparableList;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

@Description(name = "onehot_encoding", value = "_FUNC_(feature) - Compute onehot encoded array of given label")
@UDFType(deterministic = true, stateful = true)
public class OnehotEncodingUDAF extends AbstractGenericUDAFResolver {

    public static Logger log = Logger.getLogger(OnehotEncodingUDAF.class);
    public OnehotEncodingUDAF() {
        super();
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        return new GenericUDAFOnehotEncodingEvaluator();
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        int numFeatures = parameters.length;
        for (int i = 0; i < numFeatures; i++) {
            if (parameters[i] == null) {
                throw new UDFArgumentTypeException(i,
                        "Null type is found. Only primitive type arguments are accepted.");
            }
            if (parameters[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i,
                        "Only primitive type arguments are accepted but "
                                + parameters[i].getTypeName() + " was passed as parameter 1.");
            }

            switch (((PrimitiveTypeInfo) parameters[i]).getPrimitiveCategory()) {
                case BYTE:
                case INT:
                case SHORT:
                case LONG:
                case TIMESTAMP:
                case STRING:
                    break;
                default:
                    throw new UDFArgumentTypeException(i,
                        "Only integer type or string arguments are accepted but "
                        + parameters[i].getTypeName() + " was passed as parameter 1.");
            }
        }

        return new GenericUDAFOnehotEncodingEvaluator();
    }

    public static class GenericUDAFOnehotEncodingEvaluator extends GenericUDAFEvaluator {
        private int numFeatures;
        private List<String> fieldNames = new ArrayList<String>();
        private List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        private List<PrimitiveObjectInspector> featureOI = new ArrayList<PrimitiveObjectInspector>();
        private ListObjectInspector listOI;

        public GenericUDAFOnehotEncodingEvaluator() {
            // TODO: Configurable number of features.
            numFeatures = 1;
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                for (int i = 0; i < parameters.length; i++) {
                    featureOI.add((PrimitiveObjectInspector)parameters[i]);
                }
            } else {
                listOI = (ListObjectInspector)parameters[0];
            }

            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableByteObjectInspector);
            } else {
                for (int i = 0; i < numFeatures; i++) {
                    fieldNames.add("feature" + String.valueOf(i));
                }
                fieldNames.add("encode");
                for (int i = 0; i < numFeatures; i++) {
                    fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
                }
                fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector
                ));

                return ObjectInspectorFactory.getStandardListObjectInspector(
                    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs)
                );
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            EncodingBuffer buf = new EncodingBuffer(numFeatures);
            reset(buf);
            return buf;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            EncodingBuffer buf = (EncodingBuffer)aggregationBuffer;
            buf.reset();
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] parameters) throws HiveException {
            assert(parameters.length == numFeatures);
            for (int i = 0; i < numFeatures; i++) {
                if (parameters[i] == null) {
                    return;
                }
            }

            EncodingBuffer buf = (EncodingBuffer)aggregationBuffer;
            ComparableList<String> featuresList = new ComparableList<String>();
            for (int i = 0; i < numFeatures; i++) {
                String s = PrimitiveObjectInspectorUtils.getString(parameters[i], featureOI.get(i));
                featuresList.add(s);
            }
            buf.add(featuresList);
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            EncodingBuffer buf = (EncodingBuffer)aggregationBuffer;
            return buf.serialize();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object partial)
                throws HiveException {
            if (partial == null) {
                return;
            }

            EncodingBuffer buf = (EncodingBuffer)aggregationBuffer;
            buf.setNumFeatures(numFeatures);
            EncodingBuffer partialFeatures = EncodingBuffer.deserialize((List<ByteWritable>)listOI.getList(partial));
            buf.merge(partialFeatures.getFeaturesSet());
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer)
                throws HiveException {
            EncodingBuffer buf = (EncodingBuffer)aggregationBuffer;

            return buf.terminate();
        }

        /**
         * EncodingBuffer sort all input values with TreeSet and
         * attach the index for creating one hot encoded vector
         */
        static class EncodingBuffer implements AggregationBuffer, Serializable {
            private final Set<ComparableList<String>> featuresSet;
            private int numFeatures;

            public EncodingBuffer(int numFeatures) {
                this.numFeatures = numFeatures;
                // Labels should be sorted in order to keep index in case of same input
                this.featuresSet = new TreeSet<ComparableList<String>>();
            }

            public void setNumFeatures(int numFeatures) {
                this.numFeatures = numFeatures;
            }

            public void reset() {
                featuresSet.clear();
            }

            public void add(ComparableList<String> features) {
                featuresSet.add(features);
            }

            public Set<ComparableList<String>> getFeaturesSet() {
                return featuresSet;
            }

            public void merge(Set<ComparableList<String>> partialFeatures) {
                for (ComparableList<String> features : partialFeatures) {
                    featuresSet.add(features);
                }
            }

            public void merge(List<ComparableList<String>> partialFeatures) {
                for (List<String> features : partialFeatures) {
                    featuresSet.add(new ComparableList<String>(features));
                }
            }

            public List<ByteWritable> serialize()  {
                ByteArrayOutputStream b = null;
                try {
                    b = new ByteArrayOutputStream();
                    ObjectOutputStream o = new ObjectOutputStream(b);
                    o.writeObject(this);
                } catch (IOException e) {
                    log.error("Fail to serialize intermediate buffer.");
                }
                byte[] byteArray = b.toByteArray();
                List<ByteWritable> ret = new ArrayList<ByteWritable>();
                for (int i = 0; i < byteArray.length; i++) {
                    ret.add(new ByteWritable(byteArray[i]));
                }
                return ret;
            }

            public static EncodingBuffer deserialize(List<ByteWritable> bytes)  {
                byte[] obj = new byte[bytes.size()];
                for (int i = 0; i < bytes.size(); i++) {
                    obj[i] = bytes.get(i).get();
                }
                EncodingBuffer buffer = null;
                try {
                    ByteArrayInputStream b = new ByteArrayInputStream(obj);
                    ObjectInputStream o = new ObjectInputStream(b);
                    buffer = (EncodingBuffer)o.readObject();
                } catch (IOException e) {
                    log.error("Fail to deserialize intermediate buffer.", e);
                } catch (ClassNotFoundException e) {
                    log.error("Class not found for EncodingBuffer.", e);
                }
                return buffer;
            }

            @SuppressWarnings("unchecked")
            private List<Text> onehotEncode(ComparableList<String> features, List<Set<String>> uniqueFeatures, int numFeatures) {
                List<Text> encode = new ArrayList<Text>();

                for (int i = 0; i < numFeatures; i++) {
                    List<String> uniqueFeatureList = new ArrayList(uniqueFeatures.get(i));
                    int index = uniqueFeatureList.indexOf(features.get(i));
                    if (i != 0) {
                        index += uniqueFeatures.get(i - 1).size();
                    }
                    encode.add(new Text(index + ":1"));
                }
                return encode;
            }

            /**
             * Generate one hot encoding table with given input features.
             * It generates like
             *
             *     category1   | category2
             *     -------------------------
             *     cat         | mammal
             *     dog         | mammal
             *     human       | mammal
             *     seahawk     | bird
             *     wasp        | insect
             *     wasp        | insect
             *     cat         | mammal
             *     dog         | mammal
             *     human       | mammal
             *
             *     is converted to
             *
             *     category1   | category2   | encoded_features
             *     --------------------------------------------
             *     cat         | mammal      | [1:1, 6:1]     |
             *     dog         | mammal      | [2:1, 6:1]     |
             *     human       | mammal      | [3:1, 6:1]     |
             *     seahawk     | bird        | [4:1, 7:1]     |
             *     wasp        | insect      | [5:1, 8:1]     |
             *
             *   TODO: Only 1 number of features is supported.
             */
            public Object terminate() {
                // Unique values per features
                List<Set<String>> uniqueFeatures = new ArrayList<Set<String>>();

                // Build unique features which is necessary to obtain encoded index.
                for (ComparableList<String> features : featuresSet) {
                    for (int i = 0; i < features.size(); i++) {
                        if (uniqueFeatures.size() <= i) {
                            uniqueFeatures.add(i, new TreeSet<String>());
                        }
                        uniqueFeatures.get(i).add(features.get(i));
                    }
                }

                List<Object[]> ret = new ArrayList<Object[]>();
                for (ComparableList<String> features : featuresSet) {
                    // Features + encoding string
                    Object[] row = new Object[numFeatures + 1];
                    for (int i = 0; i < numFeatures; i++) {
                        row[i] = new Text(features.get(i));
                    }
                    row[numFeatures] = onehotEncode(features, uniqueFeatures, numFeatures);
                    ret.add(row);
                }
                return ret;
            }
        }
    }
}
