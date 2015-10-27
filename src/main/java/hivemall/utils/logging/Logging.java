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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class Logging implements LoggingBase {

    private transient Log logger = null;

    private Log logger() {
        if (logger == null)
            logger = LogFactory.getLog(this.getClass());
        return logger;
    }

    @Override
    public void logDebug(String msg) {
        if (logger.isDebugEnabled()) logger().debug(msg);
    }

    @Override
    public void logInfo(String msg) {
        if (logger.isInfoEnabled()) logger().info(msg);
    }

    @Override
    public void logTrace(String msg) {
        if (logger.isTraceEnabled()) logger().trace(msg);
    }

    @Override
    public void logWarning(String msg) {
        if (logger.isWarnEnabled()) logger().warn(msg);
    }

    @Override
    public void logError(String msg) {
        if (logger.isErrorEnabled()) logger().error(msg);
    }

    @Override
    public void logDebug(String msg, Throwable e) {
        if (logger.isDebugEnabled()) logger().debug(msg, e);
    }

    @Override
    public void logInfo(String msg, Throwable e) {
        if (logger.isInfoEnabled()) logger().info(msg, e);
    }

    @Override
    public void logTrace(String msg, Throwable e) {
        if (logger.isTraceEnabled()) logger().trace(msg, e);
    }

    @Override
    public void logWarning(String msg, Throwable e) {
        if (logger.isWarnEnabled()) logger().warn(msg, e);
    }

    @Override
    public void logError(String msg, Throwable e) {
        if (logger.isErrorEnabled()) logger().error(msg, e);
    }

    @Override
    public boolean isDebugEnabled() {
        return logger().isDebugEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return logger().isInfoEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return logger().isTraceEnabled();
    }

    @Override
    public boolean isWarningEnabled() {
        return logger().isWarnEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
        return logger().isErrorEnabled();
    }
}
