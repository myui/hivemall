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
package hivemall.model;

import hivemall.utils.lang.Copyable;

import javax.annotation.Nonnegative;

public interface IWeightValue extends Copyable<IWeightValue> {

    public enum WeightValueType {
        NoParams, ParamsF1, ParamsF2, ParamsCovar;
    }

    WeightValueType getType();

    float getFloatParams(@Nonnegative int i);

    float get();

    void set(float weight);

    boolean hasCovariance();

    float getCovariance();

    void setCovariance(float cov);

    float getSumOfSquaredGradients();

    float getSumOfSquaredDeltaX();

    float getSumOfGradients();

    /** 
     * @return whether touched in training or not
     */
    boolean isTouched();

    void setTouched(boolean touched);

    short getClock();

    void setClock(short clock);

    byte getDeltaUpdates();

    void setDeltaUpdates(byte deltaUpdates);

}