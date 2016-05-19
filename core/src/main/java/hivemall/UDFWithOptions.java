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
package hivemall;

import hivemall.utils.lang.CommandLineUtils;

import java.io.PrintWriter;
import java.io.StringWriter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Reporter;

public abstract class UDFWithOptions extends GenericUDF {
    @Nullable
    protected MapredContext mapredContext;

    @Override
    public final void configure(MapredContext mapredContext) {
        this.mapredContext = mapredContext;
    }

    @Nullable
    protected final Reporter getReporter() {
        if (mapredContext == null) {
            return null;
        }
        return mapredContext.getReporter();
    }

    protected static void reportProgress(@Nonnull Reporter reporter) {
        if (reporter != null) {
            synchronized (reporter) {
                reporter.progress();
            }
        }
    }

    protected static void setCounterValue(@Nullable Counter counter, long value) {
        if (counter != null) {
            synchronized (counter) {
                counter.setValue(value);
            }
        }
    }

    protected static void incrCounter(@Nullable Counter counter, long incr) {
        if (counter != null) {
            synchronized (counter) {
                counter.increment(incr);
            }
        }
    }

    protected abstract Options getOptions();

    protected final CommandLine parseOptions(String optionValue) throws UDFArgumentException {
        String[] args = optionValue.split("\\s+");
        Options opts = getOptions();
        opts.addOption("help", false, "Show function help");
        CommandLine cl = CommandLineUtils.parseOptions(args, opts);

        if (cl.hasOption("help")) {
            Description funcDesc = getClass().getAnnotation(Description.class);
            final String cmdLineSyntax;
            if (funcDesc == null) {
                cmdLineSyntax = getClass().getSimpleName();
            } else {
                String funcName = funcDesc.name();
                cmdLineSyntax = funcName == null ? getClass().getSimpleName()
                        : funcDesc.value().replace("_FUNC_", funcDesc.name());
            }
            StringWriter sw = new StringWriter();
            sw.write('\n');
            PrintWriter pw = new PrintWriter(sw);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, cmdLineSyntax, null, opts,
                HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, null, true);
            pw.flush();
            String helpMsg = sw.toString();
            throw new UDFArgumentException(helpMsg);
        }

        return cl;
    }

    protected abstract CommandLine processOptions(@Nonnull String optionValue)
            throws UDFArgumentException;

}
