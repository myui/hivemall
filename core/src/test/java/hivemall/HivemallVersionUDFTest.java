package hivemall;

import com.google.common.collect.Sets;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.*;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import org.apache.commons.collections.MapUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@RunWith(StandaloneHiveRunner.class)
public class HivemallVersionUDFTest {
    @HiveRunnerSetup
    public final HiveRunnerConfig CONFIG = new HiveRunnerConfig(){{
       setHiveExecutionEngine("mr");
    }};

    @HiveProperties
    public Map<String, String> hiveProperties = MapUtils.putAll(new HashMap(), new Object[]{
            "LOCAL.HDFS.DIR", "${hadoop.tmp.dir}",
            "hivemall.schema", "testdb",
    });

    @HiveSetupScript
    private String createSchemaScript = "create schema ${hiveconf:hivemall.schema}";

    @HiveSetupScript
    private String createTemporaryFunctions = "create temporary function hivemall_version as 'hivemall.HivemallVersionUDF'";

    @HiveSQL(files = {
            "testdb/use_testdb.sql"
    }, encoding = "UTF-8")
    private HiveShell hiveShell;

    @Test
    public void testHivemallVersion() {
        HashSet<String> expected = Sets.newHashSet("0.4.1-alpha.2");
        HashSet<String> actual = Sets.newHashSet(hiveShell.executeQuery("SELECT hivemall_version()"));

        Assert.assertEquals(expected, actual);
    }
}
