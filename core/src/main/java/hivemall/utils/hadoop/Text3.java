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
package hivemall.utils.hadoop;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.udf.generic.UDTFCollector;
import org.apache.hadoop.io.Text;

/**
 * {@link Text} passed to {@link UDTFCollector#collect(Object)}. It releases a given byte array
 * after copying the byte array.
 */
public final class Text3 extends Text {

    @Nonnull
    private/* final */byte[] buf;
    private final int len;

    public Text3(@Nonnull byte[] b) {
        this(b, b.length);
    }

    public Text3(@Nonnull byte[] b, int len) {
        super();
        this.buf = b;
        this.len = len;
    }

    @Override
    public byte[] getBytes() {
        if (buf == null) {
            throw new IllegalStateException("Text2#getBytes() SHOULD NOT be called twice");
        }
        byte[] b = buf;
        this.buf = null;
        return b;
    }

    @Override
    public int getLength() {
        return len;
    }

}
