/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
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
package hivemall.mix;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public final class MixMessage implements Externalizable {

    private MixEventName event;
    private Object feature;
    private float weight;
    private float covariance;
    private short clock;
    private int deltaUpdates;

    private String groupID;

    public MixMessage() {} // for Externalizable

    public MixMessage(MixEventName event, Object feature, float weight, short clock, int deltaUpdates) {
        this(event, feature, weight, 0.f, clock, deltaUpdates);
    }

    public MixMessage(MixEventName event, Object feature, float weight, float covariance, short clock, int deltaUpdates) {
        if(feature == null) {
            throw new IllegalArgumentException("feature is null");
        }
        if(deltaUpdates < 0 || deltaUpdates > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Illegal deletaUpdates: " + deltaUpdates);
        }
        this.event = event;
        this.feature = feature;
        this.weight = weight;
        this.covariance = covariance;
        this.clock = clock;
        this.deltaUpdates = deltaUpdates;
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
            switch(b) {
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

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(event.getID());
        out.writeObject(feature);
        out.writeFloat(weight);
        out.writeFloat(covariance);
        out.writeShort(clock);
        out.writeInt(deltaUpdates);
        if(groupID == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(groupID);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte id = in.readByte();
        this.event = MixEventName.resolve(id);
        this.feature = in.readObject();
        this.weight = in.readFloat();
        this.covariance = in.readFloat();
        this.clock = in.readShort();
        this.deltaUpdates = in.readInt();
        boolean hasGroupID = in.readBoolean();
        if(hasGroupID) {
            this.groupID = in.readUTF();
        }
    }

}
