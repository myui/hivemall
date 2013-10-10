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
package hivemall.tools.array;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

public class ConcatArrayUDF extends GenericUDF {
    /**
     * @see org.apache.hadoop.hive.serde.serdeConstants
     */
    private static final java.lang.String LIST_TYPE_NAME = "array";

    private final List<Object> ret = new ArrayList<Object>();
    private ListObjectInspector[] argumentOIs;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if(arguments.length < 1) {
            throw new UDFArgumentLengthException("The function CONCAT_ARRAY(array1, array2) needs at least 1 argument.");
        }
        final int nargs = arguments.length;
        for(int i = 0; i < nargs; i++) {
            switch(arguments[i].getCategory()) {
                case LIST:
                    if(((ListObjectInspector) (arguments[i])).getListElementObjectInspector().getCategory().equals(Category.PRIMITIVE)) {
                        break;
                    }
                default:
                    throw new UDFArgumentTypeException(0, "Argument " + i
                            + " of function CONCAT_ARRAY must be " + LIST_TYPE_NAME + "<"
                            + Category.PRIMITIVE + ">, but " + arguments[0].getTypeName()
                            + " was found.");
            }
        }

        ListObjectInspector[] listOIs = new ListObjectInspector[nargs];
        for(int i = 0; i < nargs; i++) {
            listOIs[i] = (ListObjectInspector) arguments[i];
        }
        this.argumentOIs = listOIs;

        ObjectInspector firstElemOI = listOIs[0].getListElementObjectInspector();
        ObjectInspector returnElemOI = ObjectInspectorUtils.getStandardObjectInspector(firstElemOI);

        return ObjectInspectorFactory.getStandardListObjectInspector(returnElemOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        ret.clear();

        for(int i = 0; i < arguments.length; i++) {
            final Object arrayObject = arguments[i].get();
            if(arrayObject == null) {
                continue;
            }

            final ListObjectInspector arrayOI = (ListObjectInspector) argumentOIs[i];
            final int arraylength = arrayOI.getListLength(arrayObject);
            for(int j = 0; j < arraylength; j++) {
                Object rawObj = arrayOI.getListElement(arrayObject, j);
                ObjectInspector elemOI = arrayOI.getListElementObjectInspector();
                Object obj = ObjectInspectorUtils.copyToStandardObject(rawObj, elemOI);
                ret.add(obj);
            }
        }

        return ret;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "concat_array(" + Arrays.toString(children) + ")";
    }
}