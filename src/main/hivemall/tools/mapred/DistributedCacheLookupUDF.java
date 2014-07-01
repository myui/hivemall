package hivemall.tools.mapred;

import hivemall.utils.collections.OpenHashMap;
import hivemall.utils.hadoop.HiveUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
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

    private boolean multipleLookup;
    private Object defaultValue;

    private PrimitiveObjectInspector keyOI;
    private ListObjectInspector keysOI;

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

        this.defaultValue = HiveUtils.getConstValue(argOIs[2]);
        PrimitiveObjectInspector defaultValueOI = HiveUtils.asPrimitiveObjectInspector(argOIs[2]);
        ObjectInspector returnValueOI = ObjectInspectorUtils.getStandardObjectInspector(defaultValueOI, ObjectInspectorCopyOption.WRITABLE);

        final ObjectInspector returnOI;
        final PrimitiveObjectInspector keyObjOI;
        switch(argOIs[1].getCategory()) {
            case PRIMITIVE:
                this.multipleLookup = false;
                keyObjOI = (PrimitiveObjectInspector) argOIs[1];
                this.keyOI = keyObjOI;
                returnOI = returnValueOI;
                break;
            case LIST:
                this.multipleLookup = true;
                this.keysOI = (ListObjectInspector) argOIs[1];
                ObjectInspector keysElemOI = keysOI.getListElementObjectInspector();
                keyObjOI = HiveUtils.asPrimitiveObjectInspector(keysElemOI);
                this.keyOI = keyObjOI;
                returnOI = ObjectInspectorFactory.getStandardMapObjectInspector(keyObjOI, returnValueOI);
                break;
            default:
                throw new UDFArgumentException("Unexpected key type: " + argOIs[1].getTypeName());
        }

        final OpenHashMap<Object, Object> map = new OpenHashMap<Object, Object>(8192);
        try {
            loadValues(map, new File(filepath), keyObjOI, defaultValueOI);
            this.cache = map;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SerDeException e) {
            throw new RuntimeException(e);
        }

        return returnOI;
    }

    private void loadValues(OpenHashMap<Object, Object> map, File file, PrimitiveObjectInspector keyOI, PrimitiveObjectInspector valueOI)
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
                LazySimpleSerDe serde = getLineSerde(keyOI, valueOI);
                StructObjectInspector lineOI = (StructObjectInspector) serde.getObjectInspector();
                StructField keyRef = lineOI.getStructFieldRef("key");
                StructField valueRef = lineOI.getStructFieldRef("value");

                final BufferedReader reader = new BufferedReader(new FileReader(file));
                try {
                    String line;
                    while((line = reader.readLine()) != null) {
                        Text lineText = new Text(line);
                        Object lineObj = serde.deserialize(lineText);
                        List<Object> fields = lineOI.getStructFieldsDataAsList(lineObj);
                        Object k = ((PrimitiveObjectInspector) keyRef.getFieldObjectInspector()).getPrimitiveJavaObject(fields.get(0));
                        Object v = ((PrimitiveObjectInspector) valueRef.getFieldObjectInspector()).getPrimitiveWritableObject(fields.get(1));
                        map.put(k, v);
                    }
                } finally {
                    reader.close();
                }
            }
        }
    }

    private static LazySimpleSerDe getLineSerde(PrimitiveObjectInspector keyOI, PrimitiveObjectInspector valueOI)
            throws SerDeException {
        LazySimpleSerDe serde = new LazySimpleSerDe();
        Configuration conf = new Configuration();
        Properties tbl = new Properties();
        tbl.setProperty("columns", "key,value");
        tbl.setProperty("columns.types", keyOI.getTypeName() + "," + valueOI.getTypeName());
        serde.initialize(conf, tbl);
        return serde;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        final Object arg1 = args[1].get();
        if(multipleLookup) {
            return gets(arg1);
        } else {
            return get(arg1);
        }
    }

    private Object get(Object arg) throws HiveException {
        Object key = keyOI.getPrimitiveJavaObject(arg);
        Object value = cache.get(key);
        return (value == null) ? defaultValue : value;
    }

    private Map<Object, Object> gets(Object arg) {
        List<?> keys = keysOI.getList(arg);

        final Map<Object, Object> map = new HashMap<Object, Object>();
        for(Object k : keys) {
            Object kj = keyOI.getPrimitiveJavaObject(k);
            final Object v = cache.get(kj);
            if(v == null) {
                map.put(k, defaultValue);
            } else {
                map.put(k, v);
            }
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
                + "\n\tdistcache_gets(const string FILEPATH, object key, const object defaultValue)::value_type";
    }

}
