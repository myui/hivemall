package hivemall.model;

import java.util.LinkedHashMap;
import java.util.Map;

public class CreateTableHQ extends TableHQ {
    public LinkedHashMap<String, String> header;


    CreateTableHQ(String tableName, LinkedHashMap<String, String> header) {
        super(tableName);
        this.header = header;
    }


    public String getTableDeclaration() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (Map.Entry<String, String> e : header.entrySet()) {
            sb.append(e.getKey());
            sb.append(" ");
            sb.append(e.getValue());
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
        return sb.toString();
    }
}
