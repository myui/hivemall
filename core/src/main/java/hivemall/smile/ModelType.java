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
package hivemall.smile;

public enum ModelType {

    // not compressed
    opscode(1, false), javascript(2, false), serialization(3, false),
    // compressed
    opscode_compressed(-1, true), javascript_compressed(-2, true),
    serialization_compressed(-3, true);

    private final int id;
    private final boolean compressed;

    private ModelType(int id, boolean compressed) {
        this.id = id;
        this.compressed = compressed;
    }

    public int getId() {
        return id;
    }

    public boolean isCompressed() {
        return compressed;
    }

    public static ModelType resolve(String name, boolean compressed) {
        name = name.toLowerCase();
        if ("opscode".equals(name) || "vm".equals(name)) {
            return compressed ? opscode_compressed : opscode;
        } else if ("javascript".equals(name) || "js".equals(name)) {
            return compressed ? javascript_compressed : javascript;
        } else if ("serialization".equals(name) || "ser".equals(name)) {
            return compressed ? serialization_compressed : serialization;
        } else {
            throw new IllegalStateException("Unexpected output type: " + name);
        }
    }

    public static ModelType resolve(final int id) {
        final ModelType type;
        switch (id) {
            case 1:
                type = opscode;
                break;
            case -1:
                type = opscode_compressed;
                break;
            case 2:
                type = javascript;
                break;
            case -2:
                type = javascript_compressed;
                break;
            case 3:
                type = serialization;
                break;
            case -3:
                type = serialization_compressed;
                break;
            default:
                throw new IllegalStateException("Unexpected ID for ModelType: " + id);
        }
        return type;
    }

}
