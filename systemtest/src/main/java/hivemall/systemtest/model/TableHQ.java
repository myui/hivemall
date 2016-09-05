package hivemall.systemtest.model;

abstract public class TableHQ extends StrictHQ {
    public final String tableName;


    TableHQ(String tableName) {
        this.tableName = tableName;
    }
}
