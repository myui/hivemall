/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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
package yarnkit.config;

import yarnkit.YarnkitException;

public final class YarnkitConfigException extends YarnkitException {
    private static final long serialVersionUID = -2302943903670363288L;

    public YarnkitConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public YarnkitConfigException(String message) {
        super(message);
    }

    public YarnkitConfigException(Throwable cause) {
        super(cause);
    }

}
