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
package yarnkit.client;

import static yarnkit.config.YarnkitFields.OPT_APPMASTER_JAR;
import static yarnkit.config.YarnkitFields.YARNKIT_APPMASTER_CLASS;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import yarnkit.YarnkitException;
import yarnkit.config.YarnkitConfig;
import yarnkit.config.hocon.HoconConfigLoader;
import yarnkit.utils.YarnUtils;

import com.google.common.base.Preconditions;

/**
 * A Yarn Application client.
 */
public final class YarnkitClient extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(YarnkitClient.class);

    public YarnkitClient() {
        super();
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: hadoop jar yarnkit-all-xxx-with-dependencies.jar"
                    + " [options] <application.conf>");
            return -1;
        }

        String scriptPath = Preconditions.checkNotNull(args[0]);
        YarnkitConfig config = HoconConfigLoader.load(scriptPath);
        Configuration jobConf = getConf();
        configureAppmasterJar(jobConf);
        YarnClientParameters params = new YarnClientParametersImpl(config, jobConf);

        YarnClientService service = new YarnClientService(params);
        return handle(service);
    }

    @Nonnull
    private void configureAppmasterJar(@Nonnull Configuration jobConf) throws YarnkitException {
        String jarPath = YarnUtils.findJar(YARNKIT_APPMASTER_CLASS, getClass().getClassLoader());
        if (jarPath != null) {
            LOG.info("Adding " + jarPath + " to LocalResource");
            jobConf.setIfUnset(OPT_APPMASTER_JAR, jarPath);
        }
    }

    private int handle(@Nonnull YarnClientService service) throws Exception {
        service.startAndWait();
        if (!service.isRunning()) {
            LOG.error("Service failed to startup");
            return 1;
        }

        String trackingUrl = null;
        while (service.isRunning()) {
            if (trackingUrl == null) {
                Thread.sleep(1000);
                ApplicationReport report = service.getApplicationReport();
                YarnApplicationState yarnAppState = report.getYarnApplicationState();
                if (yarnAppState == YarnApplicationState.RUNNING) {
                    trackingUrl = report.getTrackingUrl();
                    if (trackingUrl == null || trackingUrl.isEmpty()) {
                        LOG.info("Application is running, but did not specify a tracking URL");
                        trackingUrl = "";
                    } else {
                        LOG.info("Master Tracking URL = " + trackingUrl);
                    }
                }
            }
        }

        ApplicationReport report = service.getFinalReport();
        if (report == null) {
            LOG.error("No final report");
            return 1;
        } else if (report.getFinalApplicationStatus() != FinalApplicationStatus.SUCCEEDED) {
            LOG.error(report);
            return 1;
        } else {
            LOG.info("Final report: \n" + report);
            return 0;
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            int rc = ToolRunner.run(new Configuration(), new YarnkitClient(), args);
            System.exit(rc);
        } catch (Exception e) {
            LOG.fatal("Failed to run Yarn applicaton", e);
            ExitUtil.terminate(1, e);
        }
    }
}
