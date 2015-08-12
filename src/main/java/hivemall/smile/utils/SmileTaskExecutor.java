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
package hivemall.smile.utils;

import hivemall.utils.concurrent.ExecutorFactory;
import hivemall.utils.lang.Primitives;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.mapred.JobConf;

public final class SmileTaskExecutor {
    private static final Log logger = LogFactory.getLog(SmileTaskExecutor.class);

    @Nullable
    private final ExecutorService exec;

    public SmileTaskExecutor(@Nullable MapredContext mapredContext) {
        int nprocs = Runtime.getRuntime().availableProcessors();
        int threads = nprocs - 1;

        if(mapredContext != null) {
            JobConf conf = mapredContext.getJobConf();
            if(conf != null) {
                String tdJarVersion = conf.get("td.jar.version");
                if(tdJarVersion != null) {
                    String tdHivemallNprocs = conf.get("td.hivemall.smile.nprocs");
                    threads = Primitives.parseInt(tdHivemallNprocs, 2);
                }
            }
        }

        if(threads > 1) {
            logger.info("Initialized FixedThreadPool of " + threads + " threads");
            this.exec = ExecutorFactory.newFixedThreadPool(threads, "Hivemall-SMILE", true);
        } else {
            logger.info("Direct execution in a caller thread is selected");
            this.exec = null;
        }
    }

    public <T> List<T> run(Collection<? extends Callable<T>> tasks) throws Exception {
        final List<T> results = new ArrayList<T>(tasks.size());
        if(exec == null) {
            for(Callable<T> task : tasks) {
                results.add(task.call());
            }
        } else {
            final List<Future<T>> futures = exec.invokeAll(tasks);
            for(Future<T> future : futures) {
                results.add(future.get());
            }
        }
        return results;
    }

    public void shotdown() {
        if(exec != null) {
            exec.shutdownNow();
        }
    }

}
