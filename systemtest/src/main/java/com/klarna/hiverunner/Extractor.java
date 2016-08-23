package com.klarna.hiverunner;

import com.klarna.hiverunner.config.HiveRunnerConfig;
import org.junit.rules.TemporaryFolder;

public class Extractor {
    public static StandaloneHiveServerContext getStandaloneHiveServerContext(
            TemporaryFolder basedir, HiveRunnerConfig hiveRunnerConfig) {
        return new StandaloneHiveServerContext(basedir, hiveRunnerConfig);
    }

    public static HiveServerContainer getHiveServerContainer(HiveServerContext context) {
        return new HiveServerContainer(context);
    }
}
