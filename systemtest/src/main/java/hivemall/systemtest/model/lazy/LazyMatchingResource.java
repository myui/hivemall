/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2016 Makoto YUI
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
package hivemall.systemtest.model.lazy;

import com.klarna.hiverunner.CommandShellEmulation;
import com.klarna.hiverunner.sql.StatementsSplitter;
import hivemall.systemtest.model.HQ;
import hivemall.systemtest.model.RawHQ;
import hivemall.systemtest.utils.IO;
import hivemall.utils.lang.Preconditions;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LazyMatchingResource {
    @Nonnull
    private final String fileName;
    @Nonnull
    private final Charset charset;

    public LazyMatchingResource(@Nonnull final String fileName, @Nonnull final Charset charset) {
        this.fileName = fileName;
        this.charset = charset;
    }

    public List<RawHQ> toStrict(@CheckForNull final String caseDir) {
        Preconditions.checkNotNull(caseDir);

        final String query = IO.getFromResourcePath(caseDir + fileName, charset);
        final String formatted = CommandShellEmulation.HIVE_CLI.transformScript(query);
        final List<String> split = StatementsSplitter.splitStatements(formatted);
        final List<RawHQ> results = new ArrayList<RawHQ>();
        for (String q : split) {
            results.add(HQ.fromStatement(q));
        }
        return results;
    }

    public String[] getAnswers(@CheckForNull final String answerDir) {
        Preconditions.checkNotNull(answerDir);

        return IO.getFromResourcePath(answerDir + fileName).split(IO.QD);
    }
}
