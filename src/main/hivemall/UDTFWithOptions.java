package hivemall;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public abstract class UDTFWithOptions extends GenericUDTF {

    protected abstract Options getOptions();

    protected final CommandLine parseOptions(String optionValue) throws UDFArgumentException {
        String[] args = optionValue.split("\\s+");

        Options opts = getOptions();

        BasicParser parser = new BasicParser();
        final CommandLine cl;
        try {
            cl = parser.parse(opts, args);
        } catch (ParseException e) {
            throw new UDFArgumentException(e);
        }
        return cl;
    }

    protected abstract CommandLine processOptions(ObjectInspector[] argOIs)
            throws UDFArgumentException;
}
