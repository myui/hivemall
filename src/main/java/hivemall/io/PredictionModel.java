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

import hivemall.utils.collections.IMapIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface PredictionModel {

    ModelUpdateHandler getUpdateHandler();

    void setUpdateHandler(ModelUpdateHandler handler);

    int getNumMixed();

    boolean hasCovariance();

    void configureParams(boolean sum_of_squared_gradients, boolean sum_of_squared_delta_x, boolean sum_of_gradients);

    void configureClock();

    boolean hasClock();

    void resetDeltaUpdates(int feature);

    int size();

    boolean contains(@Nonnull Object feature);

    void delete(@Nonnull Object feature);

    @Nullable
    <T extends IWeightValue> T get(@Nonnull Object feature);

    <T extends IWeightValue> void set(@Nonnull Object feature, @Nonnull T value);

    float getWeight(@Nonnull Object feature);

    float getCovariance(@Nonnull Object feature);

    void _set(@Nonnull Object feature, float weight, short clock);

    void _set(@Nonnull Object feature, float weight, float covar, short clock);

    <K, V extends IWeightValue> IMapIterator<K, V> entries();

}