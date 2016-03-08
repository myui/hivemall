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

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ToolRunner;

import yarnkit.appmaster.ApplicationMaster;
import yarnkit.config.YarnkitConfig;

import com.google.common.collect.Lists;

public final class MixservAppMaster extends ApplicationMaster {
    private static final Log LOG = LogFactory.getLog(MixservAppMaster.class);

    @Nonnull
    private final MixNodeManager mixManager;

    public MixservAppMaster() {
        super();
        this.mixManager = new MixNodeManager();
    }

    @Override
    protected List<Runnable> getAuxiliaryServices(@Nonnull YarnkitConfig appConf,
            @Nonnull Configuration jobConf) {
        List<Runnable> services = Lists.newArrayList();
        services.add(new MixservCoordinatorService(mixManager, appConf));
        return services;
    }

    public static void main(String[] args) throws Exception {
        try {
            int rc = ToolRunner.run(new Configuration(), new MixservAppMaster(), args);
            System.exit(rc);
        } catch (Exception e) {
            LOG.fatal("Failed to launch MixservAppMaster", e);
            ExitUtil.terminate(1, e);
        }
    }

}
