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
package hivemall.tools.map;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

public class MapTailN extends GenericUDF {

    private MapObjectInspector mapObjectInspector;
    private IntObjectInspector intObjectInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if(arguments.length != 2) {
            throw new UDFArgumentLengthException("map_tail_n only takes 2 arguments: map<object, object>, int");
        }
        if(!(arguments[0] instanceof MapObjectInspector)) {
            throw new UDFArgumentException("The first argument must be a map");
        }
        this.mapObjectInspector = (MapObjectInspector) arguments[0];
        if(!(arguments[1] instanceof IntObjectInspector)) {
            throw new UDFArgumentException("The second argument must be an int");
        }
        this.intObjectInspector = (IntObjectInspector) arguments[1];

        ObjectInspector keyOI = ObjectInspectorUtils.getStandardObjectInspector(mapObjectInspector.getMapKeyObjectInspector());
        ObjectInspector valueOI = mapObjectInspector.getMapValueObjectInspector();

        return ObjectInspectorFactory.getStandardMapObjectInspector(keyOI, valueOI);
    }

    @Override
    public Map<?, ?> evaluate(DeferredObject[] arguments) throws HiveException {
        Object mapObj = arguments[0].get();
        Map<?, ?> map = this.mapObjectInspector.getMap(mapObj);
        int n = this.intObjectInspector.get(arguments[1].get());
        Map<?, ?> ret = tailN(map, n);
        return ret;
    }

    @Override
    public String getDisplayString(String[] arguments) {
        return "map_tail_n( " + Arrays.toString(arguments) + " )";
    }

    private Map<Object, Object> tailN(Map<?, ?> m, int n) {
        final ObjectInspector keyInspector = mapObjectInspector.getMapKeyObjectInspector();

        final TreeMap<Object, Object> tail = new TreeMap<Object, Object>();
        for(Map.Entry<?, ?> e : m.entrySet()) {
            Object k = ObjectInspectorUtils.copyToStandardObject(e.getKey(), keyInspector);
            Object v = e.getValue();
            tail.put(k, v);
        }
        if(tail.size() <= n) {
            return tail;
        }
        TreeMap<Object, Object> ret = new TreeMap<Object, Object>();
        for(int i = 0; i < n; i++) {
            Object k = tail.lastKey();
            Object v = tail.remove(k);
            ret.put(k, v);
        }
        return ret;
    }

}
