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
package hivemall.common;

import hivemall.utils.collections.IMapIterator;

public interface PredictionModel {

    public int size();

    public boolean contains(Object feature);

    public <T extends WeightValue> T get(Object feature);

    public <T extends WeightValue> void set(Object feature, T value);

    public float getWeight(Object feature);

    public float getCovariance(Object feature);

    public void setValue(Object feature, float weight);

    public void setValue(Object feature, float weight, float covar);

    public <K, V extends WeightValue> IMapIterator<K, V> entries();

}
