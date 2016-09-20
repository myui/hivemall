/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2016 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
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
package hivemall.tools.matrix;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hadoop.WritableUtils;
import hivemall.utils.lang.Preconditions;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Description(
        name = "transpose_and_dot",
        value = "_FUNC_(array<number> matrix0_row, array<number> matrix1_row)"
                + " - Returns dot(matrix0.T, matrix1) as array<array<double>>, shape = (matrix0.#cols, matrix1.#cols)")
public final class TransposeAndDotUDAF extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
            throws SemanticException {
        ObjectInspector[] OIs = info.getParameterObjectInspectors();

        if (OIs.length != 2) {
            throw new UDFArgumentLengthException("Specify two arguments.");
        }

        if (!HiveUtils.isNumberListOI(OIs[0])) {
            throw new UDFArgumentTypeException(0,
                "Only array<number> type argument is acceptable but " + OIs[0].getTypeName()
                        + " was passed as `matrix0_row`");
        }

        if (!HiveUtils.isNumberListOI(OIs[1])) {
            throw new UDFArgumentTypeException(1,
                "Only array<number> type argument is acceptable but " + OIs[1].getTypeName()
                        + " was passed as `matrix1_row`");
        }

        return new TransposeAndDotUDAFEvaluator();
    }

    private static final class TransposeAndDotUDAFEvaluator extends GenericUDAFEvaluator {
        // PARTIAL1 and COMPLETE
        private ListObjectInspector matrix0RowOI;
        private PrimitiveObjectInspector matrix0ElOI;
        private ListObjectInspector matrix1RowOI;
        private PrimitiveObjectInspector matrix1ElOI;

        // PARTIAL2 and FINAL
        private ListObjectInspector aggMatrixOI;
        private ListObjectInspector aggMatrixRowOI;
        private DoubleObjectInspector aggMatrixElOI;

        private double[] matrix0Row;
        private double[] matrix1Row;

        @AggregationType(estimable = true)
        static class TransposeAndDotAggregationBuffer extends AbstractAggregationBuffer {
            double[][] aggMatrix;

            @Override
            public int estimate() {
                return aggMatrix != null ? aggMatrix.length * aggMatrix[0].length * 8 : 0;
            }

            public void init(int n, int m) {
                aggMatrix = new double[n][m];
            }

            public void reset() {
                if (aggMatrix != null) {
                    for (double[] row : aggMatrix) {
                        Arrays.fill(row, 0.d);
                    }
                }
            }
        }

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] OIs) throws HiveException {
            super.init(mode, OIs);

            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                matrix0RowOI = HiveUtils.asListOI(OIs[0]);
                matrix0ElOI = HiveUtils.asDoubleCompatibleOI(matrix0RowOI.getListElementObjectInspector());
                matrix1RowOI = HiveUtils.asListOI(OIs[1]);
                matrix1ElOI = HiveUtils.asDoubleCompatibleOI(matrix1RowOI.getListElementObjectInspector());
            } else {
                aggMatrixOI = HiveUtils.asListOI(OIs[0]);
                aggMatrixRowOI = HiveUtils.asListOI(aggMatrixOI.getListElementObjectInspector());
                aggMatrixElOI = HiveUtils.asDoubleOI(aggMatrixRowOI.getListElementObjectInspector());
            }

            return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
        }

        @Override
        public AbstractAggregationBuffer getNewAggregationBuffer() throws HiveException {
            TransposeAndDotAggregationBuffer myAgg = new TransposeAndDotAggregationBuffer();
            reset(myAgg);
            return myAgg;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            TransposeAndDotAggregationBuffer myAgg = (TransposeAndDotAggregationBuffer) agg;
            myAgg.reset();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            TransposeAndDotAggregationBuffer myAgg = (TransposeAndDotAggregationBuffer) agg;

            if (matrix0Row == null) {
                matrix0Row = new double[matrix0RowOI.getListLength(parameters[0])];
            }
            if (matrix1Row == null) {
                matrix1Row = new double[matrix1RowOI.getListLength(parameters[1])];
            }

            HiveUtils.toDoubleArray(parameters[0], matrix0RowOI, matrix0ElOI, matrix0Row, false);
            HiveUtils.toDoubleArray(parameters[1], matrix1RowOI, matrix1ElOI, matrix1Row, false);

            Preconditions.checkNotNull(matrix0Row);
            Preconditions.checkNotNull(matrix1Row);

            if (myAgg.aggMatrix == null) {
                myAgg.init(matrix0Row.length, matrix1Row.length);
            }

            for (int i = 0; i < matrix0Row.length; i++) {
                for (int j = 0; j < matrix1Row.length; j++) {
                    myAgg.aggMatrix[i][j] += matrix0Row[i] * matrix1Row[j];
                }
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object other) throws HiveException {
            if (other == null) {
                return;
            }

            TransposeAndDotAggregationBuffer myAgg = (TransposeAndDotAggregationBuffer) agg;

            List matrix = aggMatrixOI.getList(other);
            final int n = matrix.size();
            final double[] row = new double[aggMatrixRowOI.getListLength(matrix.get(0))];
            for (int i = 0; i < n; i++) {
                HiveUtils.toDoubleArray(matrix.get(i), aggMatrixRowOI, aggMatrixElOI, row, false);

                if (myAgg.aggMatrix == null) {
                    myAgg.init(n, row.length);
                }

                for (int j = 0; j < row.length; j++) {
                    myAgg.aggMatrix[i][j] += row[j];
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            TransposeAndDotAggregationBuffer myAgg = (TransposeAndDotAggregationBuffer) agg;

            List<List<DoubleWritable>> result = new ArrayList<List<DoubleWritable>>();
            for (double[] row : myAgg.aggMatrix) {
                result.add(WritableUtils.toWritableList(row));
            }
            return result;
        }
    }
}
