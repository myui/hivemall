package hivemall.model;

import hivemall.utils.lang.Preconditions;

import java.util.List;

public class InsertHQ extends TableHQ {
    public List<Object[]> data;
    public List<String> header;


    InsertHQ(String tableName, List<String> header, List<Object[]> data) {
        super(tableName);

        int l = 0;
        for (Object[] objs : data) {
            Preconditions.checkArgument(objs.length == header.size(),
                "l.%d: Mismatch between number of elements in row(%d) and length of header(%d)", l,
                objs.length, header.size());
            l++;
        }

        this.data = data;
        this.header = header;
    }


    public String getAsValuesFormat() {
        StringBuilder sb = new StringBuilder();
        for (Object[] row : data) {
            sb.append("(");
            for (Object val : row) {
                sb.append(serialize(val));
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append("),");
        }
        sb.deleteCharAt(sb.length() - 1);

        return sb.toString();
    }

    public static String serialize(Object val) {
        // TODO array, map
        if (val instanceof String) {
            return "'" + String.valueOf(val) + "'";
        } else {
            return String.valueOf(val);
        }
    }
}
