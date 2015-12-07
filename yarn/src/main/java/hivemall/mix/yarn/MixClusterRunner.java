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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import hivemall.mix.yarn.launcher.WorkerCommandBuilder;
import hivemall.mix.yarn.utils.Log4jPropertyHelper;
import hivemall.mix.yarn.utils.YarnUtils;

/**
 * MixClusterRunner and ApplicationMaster support YARN version 2.4 or more.
 * An API set that the classes uses is almost the same
 * with the DistributedShell example in YARN package.
 * The issue of YARN compatibility can be found in a URL below;
 *
 *  - https://issues.apache.org/jira/browse/YARN-2879
 */
public final class MixClusterRunner {
    private static final Log logger = LogFactory.getLog(MixClusterRunner.class);

    private final YarnClient yarnClient;
    private final Options opts;
    private final Configuration conf;

    private Path appJar;

    // For an application master
    private String appMasterMainClass;
    private String amQueue;
    private int amPriority;
    private int amVCores;
    private int amMemory;

    // For containers
    private Path mixServJar;
    private int containerMemory;
    private int containerVCores;
    private int numContainers;
    private int numRetries;

    // log4j.properties file used in a yarn cluster
    private Path log4jPropFile;

    private ApplicationId appId;

    // Check if AM finished
    private volatile boolean isFinished = false;

    public static void main(String[] args) {
        boolean result = false;
        try {
            MixClusterRunner mixClusterRunner = new MixClusterRunner();
            try {
                boolean doRun = mixClusterRunner.init(args);
                if(!doRun) {
                    logger.error("MixServer failed to start");
                    System.exit(0);
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getLocalizedMessage());
                mixClusterRunner.printUsage();
                System.exit(-1);
            }
            result = mixClusterRunner.run();
        } catch(Throwable t) {
            logger.fatal("Error running MixServerRunner", t);
            System.exit(1);
        }
        if(result) {
            logger.info("MixServer stopped successfully");
            System.exit(0);
        }
        logger.error("MixServer failed to stop successfully");
        System.exit(2);
    }

    public MixClusterRunner() throws Exception {
        this(ApplicationMaster.class.getName(), new YarnConfiguration());
    }

    public MixClusterRunner(String appMasterMainClass, Configuration conf) {
        this.appMasterMainClass = appMasterMainClass;
        this.yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        this.conf = conf;
        this.opts = new Options();
        opts.addOption("priority", true, "Application Priority [Default: 0]");
        opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
        opts.addOption("jar", true, "Jar file containing classes executed in a YARN cluster");
        opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run AM");
        opts.addOption("master_vcores", true, "Amount of virtual cores to be requested to run AM");
        opts.addOption("num_containers", true, "# of containers for MIX servers");
        opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run a MIX server");
        opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run a MIX server");
        opts.addOption("num_retries", true, "# of retries for failed containers [Default: 32]");
        opts.addOption("log_properties", true, "log4j.properties file");
        opts.addOption("help", false, "Print usage");
    }

    // Helper function to print out usage
    private void printUsage() {
        new HelpFormatter().printHelp("MixServerRunner", opts);
    }

    @Override
    public String toString() {
        return "[appJar=" + appJar + ", amQueue=" + amQueue + ", amPriority=" + amPriority
                + ", amVCores=" + amVCores + ", amMemory=" + amMemory
                + ", numContainers=" + numContainers + ", containerVCores=" + containerVCores
                + ", containerMemory=" + containerMemory + ", numRetries=" + numRetries + "]";
    }

    public boolean init(String[] args) throws ParseException {
        assert appId == null;

        if(args.length == 0) {
            throw new IllegalArgumentException("No args specified for MixServerRunner to initialize");
        }

        CommandLine cliParser = new GnuParser().parse(opts, args);
        if(cliParser.hasOption("help")) {
            printUsage();
            return false;
        }

        final String jarPath = cliParser.getOptionValue("jar");
        if(jarPath == null || !isFileExist(jarPath)) {
            throw new IllegalArgumentException(
                    "Invalid jar not specified for an application master with -jar");
        }
        appJar = new Path(jarPath);
        amQueue = cliParser.getOptionValue("queue", "default");
        amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        amVCores = Integer.parseInt(cliParser.getOptionValue("master_vcores", "1"));
        amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "1024"));
        if(amMemory < 0 || amVCores < 0) {
            throw new IllegalArgumentException("Invalid resources for AM: " + "cores=" + amVCores
                    + "mem=" + amMemory);
        }

        final String preloadMixServ= System.getenv(MixYarnEnv.MIXSERVER_PRELOAD);
        if(preloadMixServ != null) {
            mixServJar = new Path(preloadMixServ);
        }

        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "512"));
        containerVCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
        numRetries = Integer.parseInt(cliParser.getOptionValue("num_retries", "0"));
        if(containerMemory < 0 || containerVCores < 0 || numContainers < 1 || numRetries < 0) {
            throw new IllegalArgumentException("Invalid resources for containers: " + "num="
                    + numContainers + "cores=" + containerVCores + "mem=" + containerMemory);
        }

        if(cliParser.hasOption("log_properties")) {
            log4jPropFile = new Path(cliParser.getOptionValue("log_properties"));
            try {
                Log4jPropertyHelper.updateLog4jConfiguration(MixClusterRunner.class, log4jPropFile.toString());
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

    private boolean isFileExist(String path) {
        return new File(path).exists();
    }

    public boolean run() throws IOException, InterruptedException, YarnException {
        assert appId == null;

        // Get a new application id
        yarnClient.start();
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

        // A resource ask cannot exceed the max
        int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
        if(amVCores > maxVCores) {
            logger.warn("cores:" + amVCores + " requested, but only cores:" + maxVCores
                    + " available.");
            amVCores = maxVCores;
        }
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        if(amMemory > maxMem) {
            logger.warn("mem:" + amMemory + " requested, but only mem:" + maxMem + " available.");
            amMemory = maxMem;
        }

        // Set an application name
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationName(MixClusterRunner.class.getName());
        // Keep no container between application attempts
        appContext.setKeepContainersAcrossApplicationAttempts(false);
        appId = appContext.getApplicationId();

        // Local resources (e.g., jar and local files) for AM
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        // Copy the local stuffs to the filesystem
        FileSystem fs = FileSystem.get(conf);
        Path sharedDir = createTempDir(fs, appId);

        YarnUtils.copyFromLocalFile(fs, appJar, new Path(sharedDir, appJar.getName()), localResources);
        YarnUtils.copyFromLocalFile(fs, log4jPropFile, new Path(sharedDir, "log4j.properties"), localResources);
        if(mixServJar != null) {
            YarnUtils.copyFromLocalFile(fs, mixServJar, new Path(sharedDir, mixServJar.getName()), null);
        }

        // Set the env variables to be setup in the env
        // where AM will be run.
        Map<String, String> env = new HashMap<String, String>();

        env.put(MixYarnEnv.MIXSERVER_RESOURCE_LOCATION, sharedDir.toString());
        env.put(MixYarnEnv.MIXSERVER_CONTAINER_APP, (mixServJar == null)? appJar.getName() : mixServJar.getName());

        // Set yarn-specific classpaths
        StringBuilder yarnAppClassPaths = new StringBuilder();
        YarnUtils.addClassPath(YarnUtils.getSystemClassPath(), yarnAppClassPaths);
        String[] yarnClassPath = conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH);
        for(String c : yarnClassPath) {
            YarnUtils.addClassPath(c.trim(), yarnAppClassPaths);
        }

        // Set arguments
        List<String> vargs = new ArrayList<String>();

        vargs.add("--container_memory");
        vargs.add(String.valueOf(containerMemory));
        vargs.add("--container_vcores");
        vargs.add(String.valueOf(containerVCores));
        vargs.add("--num_containers");
        vargs.add(String.valueOf(numContainers));
        if(numRetries != 0) {
            vargs.add("--num_retries");
            vargs.add(String.valueOf(numRetries));
        }
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        // Workaround: Containers killed when the amounts of memory for containers and
        // MIX servers (JVMs) are the same with each other, so MIX servers
        // have smaller memory space than containers.
        int amShrankMemory = (int) (amMemory * 0.90);

        // Create a command executed in NM
        WorkerCommandBuilder cmdBuilder = new WorkerCommandBuilder(appMasterMainClass, yarnAppClassPaths.toString(), amShrankMemory, vargs, null);

        // Set a yarn-specific java home
        cmdBuilder.setJavaHome(Environment.JAVA_HOME.$$());

        logger.info("Build an executable command for AM: " + cmdBuilder);

        // Set up the container launch context for AM
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(localResources, env, cmdBuilder.buildCommand(), null, null, null);

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

        // Add a hook in case of shutdown requested by users
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if(!isFinished) {
                    forceKillApplication();
                }
            }
        });

        boolean success = monitorApplication();
        isFinished = true;
        return success;
    }

    private static Path createTempDir(FileSystem fs, ApplicationId appId) throws IOException {
        Path dir = new Path(fs.getHomeDirectory(), appId.toString());
        fs.mkdirs(dir);
        fs.deleteOnExit(dir);
        return dir;
    }

    /**
     * Get the host name of launched AM
     * @return return host name if appId assigned
     */
    public String getApplicationMasterHost() {
        assert appId != null;
        try {
            return yarnClient.getApplicationReport(appId).getHost();
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * Get the diagnostic message from AM
     * @return return host name if appId assigned
     */
    public String getApplicationMasterDiagnostics() {
        assert appId != null;
        try {
            return yarnClient.getApplicationReport(appId).getDiagnostics();
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * Kill a submitted application by sending a call to the ASM.
     * @throws YarnException
     * @throws IOException
     */
    public void forceKillApplication() {
        assert appId != null;
        try {
            yarnClient.killApplication(appId);
        } catch (Exception e) {
            e.printStackTrace();
        }
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

        while(true) {
            // Check app status every 20 sec.
            Thread.sleep(20 * 1000L);

            // Get application report for the appId we are interested in
            final ApplicationReport report = yarnClient.getApplicationReport(appId);
            final YarnApplicationState state = report.getYarnApplicationState();
            final FinalApplicationStatus exitStatus = report.getFinalApplicationStatus();

            logger.info("Got application report from ASM for " + "appId:" + appId.getId()
                    + ", clientToAMToken:" + report.getClientToAMToken() + ", appDiagnostics:"
                    + report.getDiagnostics() + ", appMasterHost:" + report.getHost()
                    + ", appQueue:" + report.getQueue() + ", appMasterRpcPort:"
                    + report.getRpcPort() + ", appStartTime:" + report.getStartTime()
                    + ", yarnAppState:" + state + ", appFinalState:" + exitStatus
                    + ", appTrackingUrl:" + report.getTrackingUrl() + ", appUser:"
                    + report.getUser());

            if(YarnApplicationState.FINISHED == state) {
                if(FinalApplicationStatus.SUCCEEDED == exitStatus) {
                    return true;
                } else {
                    logger.info("MixServer did finished unsuccessfully");
                    return false;
                }
            } else if(YarnApplicationState.KILLED == state) {
                logger.info("Killed by the user request");
                return true;
            } else if(YarnApplicationState.FAILED == state) {
                logger.info("MixServer did not finish");
                return false;
            }
        }
    }
}
