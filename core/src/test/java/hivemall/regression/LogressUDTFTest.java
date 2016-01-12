package hivemall.regression;

import com.google.common.collect.Sets;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.*;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import org.apache.commons.collections.MapUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@RunWith(StandaloneHiveRunner.class)
public class LogressUDTFTest {

    /**
     * Setting up execution engine which can be selected 'mr' or 'tez'
     */
    @HiveRunnerSetup
    public final HiveRunnerConfig CONFIG = new HiveRunnerConfig(){{
        setHiveExecutionEngine("mr");
    }};

    @HiveProperties
    public Map<String, String> hiveProperties = MapUtils.putAll(new HashMap(), new Object[]{
            "LOCAL.HDFS.DIR", "${hadoop.tmp.dir}",
            "hivemall.schema", "irisdb",
    });

    @HiveSetupScript
    private String createSchemaScript = "create schema ${hiveconf:hivemall.schema}";

    /**
     * Declarations for creating temporary functions here
     */
    @HiveSetupScript
    private String createTemporaryFunctions
            = "create temporary function logress as 'hivemall.regression.LogressUDTF'; " +
            "create temporary function addBias as 'hivemall.ftvec.AddBiasUDF'; ";

    @HiveResource(targetFile = "${hiveconf:LOCAL.HDFS.DIR}/irisdb/iris_subset.tsv")
    private File dataFromFile =
            new File(ClassLoader.getSystemResource("testdb/iris_subset.tsv").getPath());

    /**
     * Data is imported by creating external table
     */
    @HiveSQL(files = {
            "testdb/create_iris_table.sql"
    }, encoding = "UTF-8")
    private HiveShell hiveShell;

    @Test
    public void testLogressTraining() {
        String q = "SELECT \n" +
                "  logress(\n" +
                "    addBias(features) ,\n" +
                "    target, \n" +
                "    \"-total_steps 2\"" +
                "  ) AS(\n" +
                "    feature,\n" +
                "    weight\n" +
                "  )\n" +
                "FROM\n" +
                "  iris_subset";

        HashSet<String> expected = Sets.newHashSet("3\t0.5466020107269287", "2\t0.08432850241661072", "1\t0.43803688883781433", "0\t0.04183880612254143", "4\t0.1995106041431427");

        HashSet<String> actual = Sets.newHashSet(hiveShell.executeQuery(q));

        Assert.assertEquals(expected, actual);
    }
}
