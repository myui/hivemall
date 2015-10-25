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
package hivemall.utils.logging;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

public abstract class UDTFWithLogging extends GenericUDTF {

    private transient Logging logger = new Logging() {};

    public void logDebug(String msg) {
        logger.logDebug(msg);
    }

    public void logInfo(String msg) {
        logger.logInfo(msg);
    }

    public void logTrace(String msg) {
        logger.logTrace(msg);
    }

    public void logWarning(String msg) {
        logger.logWarning(msg);
    }

    public void logError(String msg) {
        logger.logError(msg);
    }
     public void logDebug(String msg, Throwable e) {
        logger.logDebug(msg, e);
    }

    public void logInfo(String msg, Throwable e) {
        logger.logInfo(msg, e);
    }

    public void logTrace(String msg, Throwable e) {
        logger.logTrace(msg, e);
    }

    public void logWarning(String msg, Throwable e) {
        logger.logWarning(msg, e);
    }

    public void logError(String msg, Throwable e) {
        logger.logError(msg, e);
    }
}
