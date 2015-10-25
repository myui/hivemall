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

public interface LoggingBase {

    // Log messages in each given log level
    void logDebug(String msg);
    void logInfo(String msg);
    void logTrace(String msg);
    void logWarning(String msg);
    void logError(String msg);

    // Log methods that take Throwables (Exceptions/Errors)
    void logDebug(String msg, Throwable e);
    void logInfo(String msg, Throwable e);
    void logTrace(String msg, Throwable e);
    void logWarning(String msg, Throwable e);
    void logError(String msg, Throwable e);

    boolean isDebugEnabled();
    boolean isInfoEnabled();
    boolean isTraceEnabled();
    boolean isWarningEnabled();
    boolean isErrorEnabled();
}
