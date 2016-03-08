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
package yarnkit.config;

public final class YarnkitFields {
    
    public static final String YARN_APPLICATION_ID = "YARN_APPLICATION_ID";

    public static final String KEY_APP_CONFIG_FILENAME = "appconf";
    public static final String APP_CONFIG_FILENAME = "application.conf";

    public static final String YARNKIT_APPMASTER_CLASS = "yarnkit.appmaster.ApplicationMaster";
    public static final String OPT_APPMASTER_JAR = "appmaster_jar";

    // application  
    public static final String PATH_APPLICATION = "application";
    public static final String PATH_APP_NAME = PATH_APPLICATION + ".name";
    public static final String PATH_APP_QUEUE = PATH_APPLICATION + ".queue";
    public static final String PATH_APP_CLIENT_TIMEOUT = PATH_APPLICATION + ".timeout";
    public static final String PATH_APP_CONF = PATH_APPLICATION + ".conf";
    public static final String PATH_APP_ENV = PATH_APPLICATION + ".env";
    public static final String PATH_APP_RESOURCE_MAPPING = PATH_APPLICATION + ".resource_mapping";

    // application.appmaster
    public static final String PATH_APPMASTER = PATH_APPLICATION + ".appmaster";
    public static final String PATH_APPMASTER_HOSTNAME = PATH_APPMASTER + ".hostname";
    public static final String PATH_APPMASTER_CLIENT_PORT = PATH_APPMASTER + ".client_port";
    public static final String PATH_APPMASTER_TRACKING_URL = PATH_APPMASTER + ".tracking_url";
    public static final String PATH_APPMASTER_ALLOWED_FAILURES = PATH_APPMASTER
            + ".allowed_failures";
    public static final String PATH_APPMASTER_CONF = PATH_APPMASTER + ".conf";
    public static final String PATH_APPMASTER_JAR = PATH_APPMASTER + ".jar";
    public static final String PATH_APPMASTER_CLASS = PATH_APPMASTER + ".class";

    // application.appmaster.container
    // application.container
    public static final String PATH_APPMASTER_CONTAINER = PATH_APPMASTER + ".container";
    public static final String PATH_CONTAINER = PATH_APPLICATION + ".container";
    public static final String PATH_CONTAINER_JAR = PATH_CONTAINER + ".jar";

    // application.appmaster.container.resources
    // application.container.resources
    public static final String PATH_APPMASTER_CONTAINER_RESOURCES = PATH_APPMASTER_CONTAINER
            + ".resources";
    public static final String PATH_CONTAINER_RESOURCES = PATH_CONTAINER + ".resources";

    // container
    public static final String TAG_JAR = "jar";
    public static final String TAG_INSTANCES = "instances";
    public static final String TAG_VCORES = "vcores";
    public static final String TAG_MEMORY = "memory_mb";
    public static final String TAG_NODES = "nodes";
    public static final String TAG_RACKS = "racks";
    public static final String TAG_PRIORITY = "priority";
    public static final String TAG_RELAX_LOCALITY = "relax_locality";
    public static final String TAG_NODE_LABEL = "node_label";
    public static final String TAG_RESOURCES = "resources";
    public static final String TAG_RESOURCE_MAPPING = "resource_mapping";

    // application.appmaster.container.commands
    // application.container.commands
    public static final String TAG_COMMAND = "command";
    public static final String TAG_ENV = "env";

    // application.appmaster.container.resurces
    // application.container.resources
    public static final String TAG_NAME = "name";
    public static final String TAG_FILE = "file";
    public static final String TAG_TYPE = "type";
    public static final String TAG_VISIBILITY = "visibility";

}
