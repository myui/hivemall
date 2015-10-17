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
package hivemall.mf;

import hivemall.common.EtaEstimator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class MatrixFactorizationSGDUDTF extends OnlineMatrixFactorizationUDTF {

    private EtaEstimator etaEstimator;

    public MatrixFactorizationSGDUDTF() {
        super();
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("eta", true, "The initial learning rate [default: 0.001]");
        opts.addOption("eta0", true, "The initial learning rate [default 0.2]");
        opts.addOption("t", "total_steps", true, "The total number of training examples");
        opts.addOption("power_t", true, "The exponent for inverse scaling learning rate [default 0.1]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = super.processOptions(argOIs);
        this.etaEstimator = EtaEstimator.get(cl);
        return cl;
    }

    @Override
    protected float eta() {
        return etaEstimator.eta(count);
    }

}
