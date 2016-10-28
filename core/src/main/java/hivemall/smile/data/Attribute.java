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
package hivemall.smile.data;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public abstract class Attribute {

    public final AttributeType type;
    public final int attrIndex;

    Attribute(AttributeType type, int attrIndex) {
        this.type = type;
        this.attrIndex = attrIndex;
    }

    public void setSize(int size) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return -1 if not set
     */
    public int getSize() {
        throw new UnsupportedOperationException();
    }

    public void writeTo(ObjectOutput out) throws IOException {
        out.writeInt(type.getTypeId());
        out.writeInt(attrIndex);
    }

    public enum AttributeType {
        NUMERIC(1), NOMINAL(2);

        private final int id;

        private AttributeType(int id) {
            this.id = id;
        }

        public int getTypeId() {
            return id;
        }

        public static AttributeType resolve(int id) {
            final AttributeType type;
            switch (id) {
                case 1:
                    type = NUMERIC;
                    break;
                case 2:
                    type = NOMINAL;
                    break;
                default:
                    throw new IllegalStateException("Unexpected type: " + id);
            }
            return type;
        }

    }

    public static final class NumericAttribute extends Attribute {

        public NumericAttribute(int attrIndex) {
            super(AttributeType.NUMERIC, attrIndex);
        }

        @Override
        public String toString() {
            return "NumericAttribute [type=" + type + ", attrIndex=" + attrIndex + "]";
        }

    }

    public static final class NominalAttribute extends Attribute {

        private int size;

        public NominalAttribute(int attrIndex) {
            super(AttributeType.NOMINAL, attrIndex);
            this.size = -1;
        }

        @Override
        public int getSize() {
            return size;
        }

        @Override
        public void setSize(int size) {
            this.size = size;
        }

        @Override
        public void writeTo(ObjectOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(size);
        }

        @Override
        public String toString() {
            return "NominalAttribute [size=" + size + ", type=" + type + ", attrIndex=" + attrIndex
                    + "]";
        }

    }

    public static Attribute readFrom(ObjectInput in) throws IOException {
        int typeId = in.readInt();
        int attrIndex = in.readInt();

        final Attribute attr;
        final AttributeType type = AttributeType.resolve(typeId);
        switch (type) {
            case NUMERIC: {
                attr = new NumericAttribute(attrIndex);
                break;
            }
            case NOMINAL: {
                attr = new NominalAttribute(attrIndex);
                int size = in.readInt();
                attr.setSize(size);
                break;
            }
            default:
                throw new IllegalStateException("Unexpected type: " + type);
        }
        return attr;
    }

}
