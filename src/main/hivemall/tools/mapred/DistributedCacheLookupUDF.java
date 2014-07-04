package hivemall.tools.mapred;

import hivemall.utils.collections.OpenHashMap;
import hivemall.utils.hadoop.HadoopUtils;
import hivemall.utils.hadoop.HiveUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;

@Description(name = "distcache_gets", value = "_FUNC_(filepath, key, default_value) - Returns map<key_type, value_type>|value_type")
@UDFType(deterministic = false)
public final class DistributedCacheLookupUDF extends GenericUDF {

    private boolean multipleKeyLookup;
    private boolean multipleDefaultValues;
    private Object defaultValue;

    private PrimitiveObjectInspector keyInputOI;
    private PrimitiveObjectInspector valueInputOI;
    private ListObjectInspector keysInputOI;
    private ListObjectInspector valuesInputOI;

    private OpenHashMap<Object, Object> cache;

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length != 3) {
            throw new UDFArgumentException("Invalid number of arguments for distcache_gets(FILEPATH, KEYS, DEFAULT_VAL): "
                    + argOIs.length + getUsage());
        }
        if(!ObjectInspectorUtils.isConstantObjectInspector(argOIs[2])) {
            throw new UDFArgumentException("Third argument DEFAULT_VALUE must be a constant value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(argOIs[2]));
        }

        String filepath = HiveUtils.getConstString(argOIs[0]);

        ObjectInspector argOI2 = argOIs[2];
        this.multipleDefaultValues = (argOI2.getCategory() == Category.LIST);
        if(multipleDefaultValues) {
            this.valuesInputOI = (ListObjectInspector) argOI2;
            ObjectInspector valuesElemOI = valuesInputOI.getListElementObjectInspector();
            valueInputOI = HiveUtils.asPrimitiveObjectInspector(valuesElemOI);
        } else {
            this.defaultValue = HiveUtils.getConstValue(argOI2);
            valueInputOI = HiveUtils.asPrimitiveObjectInspector(argOI2);
        }
        ObjectInspector valueOutputOI = ObjectInspectorUtils.getStandardObjectInspector(valueInputOI, ObjectInspectorCopyOption.WRITABLE);

        final ObjectInspector outputOI;
        switch(argOIs[1].getCategory()) {
            case PRIMITIVE:
                this.multipleKeyLookup = false;
                this.keyInputOI = (PrimitiveObjectInspector) argOIs[1];
                outputOI = valueOutputOI;
                break;
            case LIST:
                this.multipleKeyLookup = true;
                this.keysInputOI = (ListObjectInspector) argOIs[1];
                ObjectInspector keysElemOI = keysInputOI.getListElementObjectInspector();
                this.keyInputOI = HiveUtils.asPrimitiveObjectInspector(keysElemOI);
                outputOI = ObjectInspectorFactory.getStandardMapObjectInspector(keyInputOI, valueOutputOI);
                break;
            default:
                throw new UDFArgumentException("Unexpected key type: " + argOIs[1].getTypeName());
        }

        final OpenHashMap<Object, Object> map = new OpenHashMap<Object, Object>(8192);
        try {
            loadValues(map, new File(filepath), keyInputOI, valueInputOI);
            this.cache = map;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SerDeException e) {
            throw new RuntimeException(e);
        }

        return outputOI;
    }

    private static void loadValues(OpenHashMap<Object, Object> map, File file, PrimitiveObjectInspector keyOI, PrimitiveObjectInspector valueOI)
            throws IOException, SerDeException {
        if(!file.exists()) {
            return;
        }
        if(!file.getName().endsWith(".crc")) {
            if(file.isDirectory()) {
                for(File f : file.listFiles()) {
                    loadValues(map, f, keyOI, valueOI);
                }
            } else {
                LazySimpleSerDe serde = HiveUtils.getKeyValueLineSerde(keyOI, valueOI);
                StructObjectInspector lineOI = (StructObjectInspector) serde.getObjectInspector();
                StructField keyRef = lineOI.getStructFieldRef("key");
                StructField valueRef = lineOI.getStructFieldRef("value");
                PrimitiveObjectInspector keyRefOI = (PrimitiveObjectInspector) keyRef.getFieldObjectInspector();
                PrimitiveObjectInspector valueRefOI = (PrimitiveObjectInspector) valueRef.getFieldObjectInspector();

                final BufferedReader reader = HadoopUtils.getBufferedReader(file);
                try {
                    String line;
                    while((line = reader.readLine()) != null) {
                        Text lineText = new Text(line);
                        Object lineObj = serde.deserialize(lineText);
                        List<Object> fields = lineOI.getStructFieldsDataAsList(lineObj);
                        Object f0 = fields.get(0);
                        Object f1 = fields.get(1);
                        Object k = keyRefOI.getPrimitiveJavaObject(f0);
                        Object v = valueRefOI.getPrimitiveWritableObject(valueRefOI.copyObject(f1));
                        map.put(k, v);
                    }
                } finally {
                    reader.close();
                }
            }
        }
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        final Object arg1 = args[1].get();
        if(multipleKeyLookup) {
            if(multipleDefaultValues) {
                Object arg2 = args[2].get();
                return gets(arg1, arg2);
            } else {
                return gets(arg1);
            }
        } else {
            return get(arg1);
        }
    }

    private Object get(Object arg) {
        Object key = keyInputOI.getPrimitiveJavaObject(arg);
        Object value = cache.get(key);
        return (value == null) ? defaultValue : value;
    }

    private Map<Object, Object> gets(Object arg) {
        List<?> keys = keysInputOI.getList(arg);

        final Map<Object, Object> map = new HashMap<Object, Object>();
        for(Object k : keys) {
            if(k == null) {
                continue;
            }
            Object kj = keyInputOI.getPrimitiveJavaObject(k);
            final Object v = cache.get(kj);
            if(v == null) {
                map.put(k, defaultValue);
            } else {
                map.put(k, v);
            }
        }
        return map;
    }

    private Map<Object, Object> gets(Object argKeys, Object argValues) throws HiveException {
        final List<?> keys = keysInputOI.getList(argKeys);
        final List<?> defaultValues = valuesInputOI.getList(argValues);
        final int numKeys = keys.size();
        if(numKeys != defaultValues.size()) {
            throw new HiveException("# of default values != # of lookup keys: keys " + argKeys
                    + ", values: " + argValues);
        }

        final Map<Object, Object> map = new HashMap<Object, Object>();
        for(int i = 0; i < numKeys; i++) {
            Object k = keys.get(i);
            if(k == null) {
                continue;
            }
            Object kj = keyInputOI.getPrimitiveJavaObject(k);
            Object v = cache.get(kj);
            if(v == null) {
                v = defaultValues.get(i);
                if(v != null) {
                    v = valueInputOI.getPrimitiveWritableObject(valueInputOI.copyObject(v));
                }
            }
            map.put(k, v);
        }
        return map;
    }

    @Override
    public String getDisplayString(String[] args) {
        return "distcache_gets()";
    }

    private static String getUsage() {
        return "\nUSAGE: "
                + "\n\tdistcache_gets(const string FILEPATH, object[] keys, const object defaultValue)::map<key_type, value_type>"
                + "\n\tdistcache_gets(const string FILEPATH, object key, const object defaultValue)::value_type"
                + "\n\tdistcache_gets(const string FILEPATH, object[] key, object[] defaultValues)::map<key_type, value_type>";
    }

}
