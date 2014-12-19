/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.io;

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