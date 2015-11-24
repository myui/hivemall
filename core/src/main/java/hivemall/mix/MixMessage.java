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
package hivemall.mix;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class MixMessage {

    @Nonnull
    private MixEventName event;
    @Nonnull
    private Object feature;
    private float weight;
    private float covariance;
    private short clock;
    private int deltaUpdates;
    private boolean cancelRequest;

    @Nullable
    private String groupID;

    public MixMessage(@Nonnull MixEventName event, @CheckForNull Object feature, float weight,
            short clock, int deltaUpdates) {
        this(event, feature, weight, 0.f, clock, deltaUpdates, false);
    }

    public MixMessage(@Nonnull MixEventName event, @CheckForNull Object feature, float weight,
            float covariance, short clock, int deltaUpdates) {
        this(event, feature, weight, covariance, clock, deltaUpdates, false);
    }

    public MixMessage(@Nonnull MixEventName event, @CheckForNull Object feature, float weight,
            float covariance, int deltaUpdates, boolean cancelRequest) {
        this(event, feature, weight, covariance, (short) 0 /* dummy clock */, deltaUpdates, cancelRequest);
    }

    MixMessage(@Nonnull MixEventName event, @CheckForNull Object feature, float weight,
            float covariance, short clock, int deltaUpdates, boolean cancelRequest) {
        if (feature == null) {
            throw new IllegalArgumentException("feature is null");
        }
        if (deltaUpdates < 0 || deltaUpdates > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Illegal deletaUpdates: " + deltaUpdates);
        }
        this.event = event;
        this.feature = feature;
        this.weight = weight;
        this.covariance = covariance;
        this.clock = clock;
        this.deltaUpdates = deltaUpdates;
        this.cancelRequest = cancelRequest;
    }

    public enum MixEventName {
        average((byte) 1), argminKLD((byte) 2), closeGroup((byte) 3);

        private final byte id;

        MixEventName(byte id) {
            this.id = id;
        }

        public byte getID() {
            return id;
        }

        public static MixEventName resolve(int b) {
            switch (b) {
                case 1:
                    return average;
                case 2:
                    return argminKLD;
                default:
                    throw new IllegalArgumentException("Illegal ID: " + b);
            }
        }
    }

    public MixEventName getEvent() {
        return event;
    }

    public Object getFeature() {
        return feature;
    }

    public float getWeight() {
        return weight;
    }

    public float getCovariance() {
        return covariance;
    }

    public short getClock() {
        return clock;
    }

    public int getDeltaUpdates() {
        return deltaUpdates;
    }

    public String getGroupID() {
        return groupID;
    }

    public void setGroupID(String groupID) {
        this.groupID = groupID;
    }

    public boolean isCancelRequest() {
        return cancelRequest;
    }

    @Override
    public String toString() {
        return "MixMessage [event=" + event + ", feature=" + feature + ", weight=" + weight
                + ", covariance=" + covariance + ", clock=" + clock + ", deltaUpdates="
                + deltaUpdates + ", cancel=" + cancelRequest + ", groupID=" + groupID + "]";
    }

}
