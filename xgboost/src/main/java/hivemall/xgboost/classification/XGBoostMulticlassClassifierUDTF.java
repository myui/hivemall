/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.xgboost.classification;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import hivemall.xgboost.XGBoostUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * A XGBoost multiclass classification and the document is as follows;
 *  - https://github.com/dmlc/xgboost/tree/master/demo/multiclass_classification
 */
@Description(
    name = "train_multiclass_xgboost_classifier",
    value = "_FUNC_(string[] features, double target [, string options]) - Returns a relation consisting of <string model_id, array<byte> pred_model>"
)
public class XGBoostMulticlassClassifierUDTF extends XGBoostUDTF {

    public XGBoostMulticlassClassifierUDTF() {}

    {
        // Settings for multiclass classification
        params.put("objective", "multi:softprob");
        params.put("eval_metric", "error");
        params.put("num_class", 2);
    }

    @Override
    protected Options getOptions() {
        final Options opts = super.getOptions();
        opts.addOption("num_class", true, "Number of classes to classify");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        final CommandLine cli = super.processOptions(argOIs);
        if(cli != null) {
            if(cli.hasOption("num_class")) {
                int _num_class = Integer.valueOf(cli.getOptionValue("num_class"));
                if(_num_class < 2) {
                    throw new UDFArgumentException(
                            "num_class must be greater than 1: " + _num_class);
                }
                params.put("num_class", _num_class);
            }
        }
        return cli;
    }

    @Override
    public void checkTargetValue(double target) throws HiveException {
        double num_class = ((Integer) params.get("num_class")).doubleValue();
        if(target < 0.0 || target > num_class
                || Double.compare(target - Math.floor(target), 0.0) != 0) {
            throw new HiveException(
                    "target must be {0.0, ..., "
                            + String.format("%.1f", (num_class - 1.0))
                            + "}: " + target);
        }
    }

}
