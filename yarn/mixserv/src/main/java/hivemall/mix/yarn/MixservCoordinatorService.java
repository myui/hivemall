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
package hivemall.mix.yarn;

import static hivemall.mix.MixNodeManager.ENV_MIX_COORDINATOR_ENDPOINT;
import hivemall.mix.MixNodeManager;
import hivemall.mix.rest.MixservModule;

import java.util.EnumSet;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import yarnkit.config.YarnkitConfig;
import yarnkit.config.hocon.HoconConfigLoader;

import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Stage;
import com.google.inject.servlet.GuiceFilter;

public final class MixservCoordinatorService implements Runnable {
    private static final Log LOG = LogFactory.getLog(MixservCoordinatorService.class);

    // Jetty configuration constants
    private static final String CONF_PATH_COORDINATOR_PORT = "mixserv.coordinator.port";
    private static final String CONF_PATH_COORDINATOR_MIN_THREADS = "mixserv.coodinator.min_threads";
    private static final String CONF_PATH_COORDINATOR_MAX_THREADS = "mixserv.coodinator.max_threads";
    private static final int DEFAULT_API_PORT = 8080;
    private static final int MIN_THREADS = 1;
    private static final int MAX_THREADS = 4;

    @Nonnull
    private final MixNodeManager mixManager;
    @Nonnull
    private final YarnkitConfig appConf;

    public MixservCoordinatorService(@Nonnull MixNodeManager mixManager, @Nonnull YarnkitConfig appConf) {
        this.mixManager = mixManager;
        this.appConf = appConf;
    }

    @Override
    public void run() {
        Guice.createInjector(Stage.PRODUCTION, new MixservModule(mixManager));

        Server jettyServer = initJettyServer(appConf);
        try {
            runJettyServer(jettyServer, appConf);
        } finally {
            jettyServer.destroy();
        }
    }

    @Nonnull
    private static Server initJettyServer(@Nonnull YarnkitConfig appConf) {
        int port = appConf.getInt(CONF_PATH_COORDINATOR_PORT, DEFAULT_API_PORT);
        int minThreads = appConf.getInt(CONF_PATH_COORDINATOR_MIN_THREADS, MIN_THREADS);
        int maxThreads = appConf.getInt(CONF_PATH_COORDINATOR_MAX_THREADS, MAX_THREADS);

        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setMinThreads(minThreads);
        threadPool.setMaxThreads(maxThreads);

        Server jettyServer = new Server(threadPool);

        ServerConnector connector = new ServerConnector(jettyServer);
        connector.setPort(port);
        jettyServer.addConnector(connector);

        configureRestResource(jettyServer);

        return jettyServer;
    }

    private static void configureRestResource(@Nonnull Server jettyServer) {
        ServletContextHandler context = new ServletContextHandler(jettyServer, "/",
            ServletContextHandler.SESSIONS);
        // Configure Jetty to reroute all requests through Guice Filter. 
        context.addFilter(GuiceFilter.class, "/api/*", EnumSet.<javax.servlet.DispatcherType>of(
            javax.servlet.DispatcherType.REQUEST, javax.servlet.DispatcherType.ASYNC));
        // capture any request GuiceFilter refuses to address.
        context.addServlet(DefaultServlet.class, "/*");
    }

    private static void runJettyServer(@Nonnull Server jettyServer, @Nonnull YarnkitConfig appConf) {
        try {
            jettyServer.start();
        } catch (Exception e) {
            LOG.fatal("Failed to start Jetty server", e);
            Throwables.propagate(e);
        }

        String endpoint = getHttpEndpoint(jettyServer);
        appConf.setEnv(ENV_MIX_COORDINATOR_ENDPOINT, endpoint);
        LOG.info("Started Mix coordinator: " + endpoint);

        try {
            jettyServer.join();
        } catch (InterruptedException e) {
            LOG.warn("Jetty server is interrupted", e);
        }
    }

    @Nonnull
    private static String getHttpEndpoint(@Nonnull Server jettyServer) {
        return jettyServer.getURI().toString();
    }

    public static void main(String[] args) {
        MixNodeManager mixManager = new MixNodeManager();
        YarnkitConfig appConf = HoconConfigLoader.load("application.conf");
        MixservCoordinatorService service = new MixservCoordinatorService(mixManager, appConf);
        service.run();
    }

}
