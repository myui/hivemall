/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
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
package hivemall.mix.yarn;

import hivemall.mix.launcher.WorkerCommandBuilder;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.net.URL;
import java.util.*;

public final class MixServerRunner {
    private static final Log logger = LogFactory.getLog(MixServerRunner.class);

    private final YarnClient yarnClient;
    private final Options opts;
    private final Configuration conf;

    // For an application master
    private Path appMasterJar;
    private String amQueue;
    private int amPriority;
    private int amVCores;
    private int amMemory;

    // For containers
    private Path mixServJar;
    private int containerMemory;
    private int containerVCores;
    private int numContainers;

    // log4j.properties file used in a yarn cluster
    private Path log4jPropFile;

    private ApplicationId appId;

    public static void main(String[] args) {
        boolean result = false;
        try {
            MixServerRunner mixServerRunner = new MixServerRunner();
            try {
                boolean doRun = mixServerRunner.init(args);
            if (!doRun) {
                System.exit(0);
            }
        } catch (IllegalArgumentException e) {
            System.err.println(e.getLocalizedMessage());
            mixServerRunner.printUsage();
            System.exit(-1);
        }
        result = mixServerRunner.run();
        } catch (Throwable t) {
            logger.fatal("Error running MixServerRunner", t);
            System.exit(1);
        }
        if (result) {
            logger.info("MixServer stopped successfully");
            System.exit(0);
        }
        logger.error("MixServer failed to stop successfully");
        System.exit(2);
    }

    public MixServerRunner() throws Exception  {
      this(new YarnConfiguration());
    }

    public MixServerRunner(Configuration conf) {
        this.yarnClient = YarnClient.createYarnClient();
        this.yarnClient.init(conf);
        this.conf = conf;
        this.opts = new Options();
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
        opts.addOption("jar", true, "Jar file containing AM");
        opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run AM");
        opts.addOption("master_vcores", true, "Amount of virtual cores to be requested to run AM");
        opts.addOption("container_jar", true, "Jar file containing a mix server");
        opts.addOption("num_containers", true, "# of containers for mix servers");
        opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run a mix server");
        opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run a mix server");
        opts.addOption("log_properties", true, "log4j.properties file");
        opts.addOption("help", false, "Print usage");
    }

    // Helper function to print out usage
    private void printUsage() {
        new HelpFormatter().printHelp("MixServerRunner", opts);
    }

    @Override
    public String toString() {
        return "[appMasterJar=" + appMasterJar + ", appMasterJar=" + appMasterJar
                + ", amQueue=" + amQueue + ", amPriority=" + amPriority
                + ", amVCores=" + amVCores + ", amMemory=" + amMemory
                + ", mixServJar=" + mixServJar + ", numContainers=" + numContainers
                + ", containerVCores=" + containerVCores
                + ", containerMemory=" + containerMemory + "]";
    }

    public boolean init(String[] args) throws ParseException {
        assert appId == null;

        if (args.length == 0) {
            throw new IllegalArgumentException(
                    "No args specified for MixServerRunner to initialize");
        }

        CommandLine cliParser = new GnuParser().parse(opts, args);
        if (cliParser.hasOption("help")) {
            printUsage();
            return false;
        }

        appMasterJar = new Path(cliParser.getOptionValue("jar"));
        amQueue = cliParser.getOptionValue("queue", "default");
        amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        amVCores = Integer.parseInt(cliParser.getOptionValue("master_vcores", "1"));
        amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "10"));
        if (amMemory < 0 || amVCores < 0) {
          throw new IllegalArgumentException(
                  "Invalid resources for AM: " + "cores=" + amVCores + "mem=" + amMemory);
        }

        if (cliParser.hasOption("container_jar")) {
            mixServJar = new Path(cliParser.getOptionValue("container_jar"));
        } else {
            // Self-contained jar used for mix servers
            URL jar = this.getClass().getResource("/hivemall-mixserv.jar");
            assert jar != null;
            mixServJar = new Path(jar.getPath());
        }

        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "512"));
        containerVCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
        if (containerMemory < 0 || containerVCores < 0 || numContainers < 1) {
            throw new IllegalArgumentException(
                    "Invalid resources for containers: "
                  + "num=" + numContainers + "cores=" + containerVCores
                  + "mem=" + containerMemory);
        }

        if (cliParser.hasOption("log_properties")) {
            log4jPropFile = new Path(cliParser.getOptionValue("log_properties"));
            try {
                Log4jPropertyHelper.updateLog4jConfiguration(
                        MixServerRunner.class, log4jPropFile.toString());
            } catch (Exception e) {
                logger.warn("Can not set up custom log4j properties. " + e);
            }
        } else {
            // Self-contained file used for a yarn cluster
            URL log4j = this.getClass().getResource("/log4j.properties");
            assert log4j != null;
            log4jPropFile = new Path(log4j.getPath());
        }

        // Print the configurations that this yarn client works with
        logger.info(this);

        return true;
    }

    public boolean run() throws IOException, InterruptedException, YarnException {
        assert appId == null;

        // Get a new application id
        yarnClient.start();
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

        // A resource ask cannot exceed the max
        int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
        if (amVCores > maxVCores) {
            logger.warn("cores:" + amVCores + " requested, but only cores:"
                    + maxVCores + " available.");
            amVCores = maxVCores;
        }
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        if (amMemory > maxMem) {
            logger.warn("mem:" + amMemory + " requested, but only mem:"
                    + maxMem + " available.");
            amMemory = maxMem;
        }

        // Set an application name
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationName(MixServerRunner.class.getName());
        appId = appContext.getApplicationId();

        // Local resources (e.g., jar and local files) for AM
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        // Copy the local stuffs to the filesystem
        FileSystem fs = FileSystem.get(conf);
        Path sharedDir = createTempDir(fs, appId);

        YarnUtils.copyFromLocalFile(fs, appMasterJar,
                new Path(sharedDir, "appmaster.jar"), localResources);
        YarnUtils.copyFromLocalFile(fs, log4jPropFile,
                new Path(sharedDir, "log4j.properties"), localResources);
        YarnUtils.copyFromLocalFile(fs, mixServJar,
                new Path(sharedDir, mixServJar.getName()), null);

        // Set the env variables to be setup in the env
        // where AM will be run.
        Map<String, String> env = new HashMap<String, String>();

        env.put(MixYarnEnv.MIXSERVER_RESOURCE_LOCATION, sharedDir.toString());
        env.put(MixYarnEnv.MIXSERVER_CONTAINER_APP, mixServJar.getName());

        // Set yarn-specific classpaths
        StringBuilder yarnAppClassPaths = new StringBuilder();

        String[] yarnClassPath = conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH);
        for (String c : yarnClassPath) {
            YarnUtils.addClassPath(c.trim(), yarnAppClassPaths);
        }

        // Set arguments
        List<String> vargs = new ArrayList<String>();

        vargs.add("--container_memory " + String.valueOf(containerMemory));
        vargs.add("--container_vcores " + String.valueOf(containerVCores));
        vargs.add("--num_containers " + String.valueOf(numContainers));
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        // Create a command executed in NM
        WorkerCommandBuilder cmdBuilder = new WorkerCommandBuilder(
                ApplicationMaster.class, YarnUtils.getClassPaths(yarnAppClassPaths.toString()),
                amMemory, vargs, null);

        // Set a yarn-specific java home
        cmdBuilder.setJavaHome(Environment.JAVA_HOME.$$());

        logger.info("Build an executable command for AM: " + cmdBuilder);

        // Set up the container launch context for AM
        ContainerLaunchContext amContainer =
                ContainerLaunchContext.newInstance(
                        localResources, env, cmdBuilder.buildCommand(), null, null, null);

        // Set up resource type requirements
        Resource capability = Resource.newInstance(amMemory, amVCores);
        appContext.setResource(capability);
        appContext.setAMContainerSpec(amContainer);

        // Set the priority for AM
        appContext.setPriority(Priority.newInstance(amPriority));

        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue(amQueue);

        // Submit the application to AM
        yarnClient.submitApplication(appContext);

        return monitorApplication();
    }

    private static Path createTempDir(FileSystem fs, ApplicationId appId) throws IOException {
        Path dir = new Path(fs.getHomeDirectory(), appId.toString());
        fs.mkdirs(dir);
        fs.deleteOnExit(dir);
        return dir;
    }

    /**
     * Kill a submitted application by sending a call to the ASM.
     * @throws YarnException
     * @throws IOException
     */
    public void forceKillApplication() throws YarnException, IOException {
        assert appId != null;
        yarnClient.killApplication(appId);
    }

    /**
      * Monitor the submitted application for completion.
      * Kill application if time expires.
      * @return true if application completed successfully
      * @throws YarnException
      * @throws IOException
      */
    private boolean monitorApplication() throws YarnException, InterruptedException, IOException {
        assert appId != null;

        while (true) {
            // Check app status every 1 minute
            Thread.sleep(60 * 1000L);

            // Get application report for the appId we are interested in
            ApplicationReport report = yarnClient.getApplicationReport(appId);
            logger.info("Got application report from ASM for "
                    + "appId:" + appId.getId()
                    + ", clientToAMToken:" + report.getClientToAMToken()
                    + ", appDiagnostics:" + report.getDiagnostics()
                    + ", appMasterHost:" + report.getHost()
                    + ", appQueue:" + report.getQueue()
                    + ", appMasterRpcPort:" + report.getRpcPort()
                    + ", appStartTime:" + report.getStartTime()
                    + ", yarnAppState:" + report.getYarnApplicationState().toString()
                    + ", appFinalState:" + report.getFinalApplicationStatus().toString()
                    + ", appTrackingUrl:" + report.getTrackingUrl()
                    + ", appUser:" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            String appStateMsg = "(YarnState:" + state.toString()
                    + " DSFinalStatus:" + dsStatus.toString() + ")";
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    return true;
                } else {
                    logger.info("MixServer did finished unsuccessfully " + appStateMsg);
                    return false;
                }
            } else if (YarnApplicationState.KILLED == state) {
                logger.info("Killed by the user request " + appStateMsg);
                return true;
            } else if (YarnApplicationState.FAILED == state) {
                logger.info("MixServer did not finish " + appStateMsg);
                return false;
            }
        }
    }
}
