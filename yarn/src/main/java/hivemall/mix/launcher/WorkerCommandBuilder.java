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
package hivemall.mix.launcher;

import hivemall.utils.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class WorkerCommandBuilder {

    private final String mainClass;
    private final String extraClassPath;
    private final int memoryMb;
    private final List<String> arguments;
    private final List<String> javaOps;

    private String javaHome;

    public WorkerCommandBuilder(
            Class<?> mainClass,  String extraClassPath, int memoryMb,
            List<String> arguments, List<String> javaOps) {
        this(mainClass.getCanonicalName(), extraClassPath, memoryMb, arguments, javaOps);
    }

    public WorkerCommandBuilder(
            String mainClass,  String extraClassPath, int memoryMb,
            List<String> arguments, List<String> javaOps) {
        this.mainClass = mainClass;
        this.extraClassPath = extraClassPath;
        this.memoryMb = memoryMb;
        this.arguments = arguments;
        this.javaOps = javaOps;
    }

    public void setJavaHome(String javaHome) {
        this.javaHome = javaHome;
    }

    public List<String> buildCommand() throws IOException {
        final List<String> command = new ArrayList<String>();
        String envJavaHome;
        if (javaHome != null) {
            envJavaHome = javaHome;
        } else {
            envJavaHome = System.getenv("JAVA_HOME");
            if (envJavaHome != null) {
                envJavaHome = System.getProperty("java.home");
            }
        }
        command.add(StringUtils.join(File.separator, envJavaHome, "bin", "java"));
        command.add("-cp");
        command.add(StringUtils.join(File.pathSeparator, buildClassPath(extraClassPath)));
        command.addAll(Arrays.asList("-Xms" + memoryMb + "m", "-Xmx" + memoryMb + "m"));
        if (javaOps != null) command.addAll(javaOps);
        command.add(mainClass);
        if (arguments != null) command.addAll(arguments);
        return command;
    }


    // Build the classpath for the application
    private List<String> buildClassPath(String appClassPath) throws IOException {
        List<String> cp = new ArrayList<String>();
        addToClassPath(cp, appClassPath);
        addToClassPath(cp, System.getProperty("java.class.path"));
        return cp;
    }

    // Add entries to the classpath
    private void addToClassPath(List<String> cp, String entries) {
        if (entries == null || entries.isEmpty()) {
          return;
        }
        String[] split = entries.split(Pattern.quote(File.pathSeparator));
        for (String e : split) {
            if (e != null && !e.isEmpty()) {
                if (new File(e).isDirectory() && !e.endsWith(File.separator)) {
                    e += File.separator;
                }
            }
            cp.add(e);
        }
    }

    @Override
    public String toString() {
        return "WorkerCommandBuilder["
                + "mainClass=" + mainClass + ", extraClassPath=" + extraClassPath
                + ", memoryMb=" + memoryMb + ", arguments="  + arguments
                + ", javaOps=" + javaOps + "]";
    }
}
