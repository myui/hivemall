/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package yarnkit.appmaster;

import static yarnkit.config.YarnkitFields.KEY_APP_CONFIG_FILENAME;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import yarnkit.YarnkitException;
import yarnkit.config.YarnkitConfig;
import yarnkit.config.hocon.HoconConfigLoader;

public class ApplicationMaster extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

    protected ApplicationMaster() {
        super();
    }

    @Nonnull
    protected List<Runnable> getAuxiliaryServices(@Nonnull YarnkitConfig appConf,
            @Nonnull Configuration jobConf) {
        return Collections.emptyList();
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration jobConf = getConf();

        YarnkitConfig appConf = getApplicationConfigFile(jobConf);
        ApplicationMasterParameters params = getApplicationMasterParameters(appConf, jobConf);
        ApplicationMasterService appmaster = new ApplicationMasterService(params);

        // First, start auxiliary service
        List<Future<?>> services = runAuxiliaryServices(appConf, jobConf);
        // Then, launch ApplicationMaster
        appmaster.startAndWait();

        while (appmaster.hasRunningContainers()) {
            Thread.sleep(1000);
        }

        for (Future<?> f : services) {
            f.cancel(true);
        }
        appmaster.stopAndWait();
        return 0;
    }

    @Nonnull
    private static YarnkitConfig getApplicationConfigFile(@Nonnull Configuration jobConf)
            throws YarnkitException, IOException {
        String appconf = jobConf.get(KEY_APP_CONFIG_FILENAME);
        if (appconf == null) {
            throw new YarnkitException(KEY_APP_CONFIG_FILENAME + " property is not set");
        }

        File file = new File(appconf);
        if (!file.exists()) {
            throw new YarnkitException(KEY_APP_CONFIG_FILENAME + " does not exist: "
                    + file.getAbsolutePath());
        }
        FileInputStream is = new FileInputStream(file);
        return HoconConfigLoader.load(is);
    }

    @Nonnull
    protected ApplicationMasterParameters getApplicationMasterParameters(
            @Nonnull YarnkitConfig appConf, @Nonnull Configuration jobConf) {
        return new ApplicationMasterParametersImpl(appConf, jobConf);
    }

    @Nonnull
    private List<Future<?>> runAuxiliaryServices(@Nonnull YarnkitConfig appConf,
            @Nonnull Configuration jobConf) {
        List<Runnable> runnables = getAuxiliaryServices(appConf, jobConf);
        if (runnables.isEmpty()) {
            return Collections.emptyList();
        }

        int size = runnables.size();
        ExecutorService execServ = Executors.newFixedThreadPool(size);
        List<Future<?>> futures = new ArrayList<Future<?>>(size);
        for (Runnable task : runnables) {
            Future<?> f = execServ.submit(task);
            futures.add(f);
        }
        return futures;
    }

    public static void main(String[] args) throws Exception {
        try {
            int rc = ToolRunner.run(new Configuration(), new ApplicationMaster(), args);
            System.exit(rc);
        } catch (Exception e) {
            LOG.fatal("Failed to launch ApplicationMaster", e);
            ExitUtil.terminate(1, e);
        }
    }
}
