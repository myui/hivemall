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
package hivemall.ftvec;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

@Description(name = "feature", value = "_FUNC_(string feature, double weight) - Returns a feature string")
@UDFType(deterministic = true, stateful = false)
public final class FeatureUDF extends UDF {

    public Text evaluate(int feature, int weight) {
        return new Text(feature + ":" + weight);
    }

    public Text evaluate(int feature, long weight) {
        return new Text(feature + ":" + weight);
    }

    public Text evaluate(int feature, float weight) {
        return new Text(feature + ":" + weight);
    }

    public Text evaluate(int feature, double weight) {
        return new Text(feature + ":" + weight);
    }

    public Text evaluate(long feature, int weight) {
        return new Text(feature + ":" + weight);
    }

    public Text evaluate(long feature, long weight) {
        return new Text(feature + ":" + weight);
    }

    public Text evaluate(long feature, float weight) {
        return new Text(feature + ":" + weight);
    }

    public Text evaluate(long feature, double weight) {
        return new Text(feature + ":" + weight);
    }

    public Text evaluate(String feature, int weight) {
        if(feature == null) {
            return null;
        }
        return new Text(feature + ':' + weight);
    }

    public Text evaluate(String feature, long weight) {
        if(feature == null) {
            return null;
        }
        return new Text(feature + ':' + weight);
    }

    public Text evaluate(String feature, float weight) {
        if(feature == null) {
            return null;
        }
        return new Text(feature + ':' + weight);
    }

    public Text evaluate(String feature, double weight) {
        if(feature == null) {
            return null;
        }
        return new Text(feature + ':' + weight);
    }

}
