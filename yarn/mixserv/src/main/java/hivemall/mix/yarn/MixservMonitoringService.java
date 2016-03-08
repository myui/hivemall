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

import hivemall.mix.MixNodeManager;

import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * A service that does MixServer monitoring and health check.
 */
public final class MixservMonitoringService extends TimerTask {
    private static final Log LOG = LogFactory.getLog(MixservMonitoringService.class);
        
    private static final int PERIODICAL_DALAY_SEC = 5;

    @Nonnull
    private final MixNodeManager mixManager;
    @Nonnull
    private final Configuration jobConf;

    public MixservMonitoringService(@Nonnull MixNodeManager mixManager,
            @Nonnull Configuration jobConf) {
        this.mixManager = mixManager;
        this.jobConf = jobConf;
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
    }

    @Override
    public long scheduledExecutionTime() {
        return TimeUnit.SECONDS.toMillis(PERIODICAL_DALAY_SEC);
    }

}
