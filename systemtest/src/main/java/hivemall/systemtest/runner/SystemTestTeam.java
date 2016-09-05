package hivemall.systemtest.runner;

import hivemall.systemtest.model.HQ;
import hivemall.systemtest.model.RawHQ;
import hivemall.systemtest.model.StrictHQ;
import hivemall.systemtest.model.lazy.LazyMatchingResource;
import hivemall.utils.lang.Preconditions;
import org.junit.rules.ExternalResource;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class SystemTestTeam extends ExternalResource {
    private List<SystemTestRunner> runners = new ArrayList<SystemTestRunner>();
    private List<SystemTestRunner> reachGoal = new ArrayList<SystemTestRunner>(); // distinct

    private List<StrictHQ> initHqs = new ArrayList<StrictHQ>();
    private Set<Entry<StrictHQ, String>> entries = new LinkedHashSet<Entry<StrictHQ, String>>();


    public SystemTestTeam(SystemTestRunner... runners) {
        this.runners.addAll(Arrays.asList(runners));
    }


    @Override
    protected void after() {
        for (SystemTestRunner runner : reachGoal) {
            try {
                List<String> tables = runner.exec(HQ.tableList());
                for (String t : tables) {
                    if (!runner.isImmutableTable(t)) {
                        runner.exec(HQ.dropTable(t));
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException("Failed to resetPerMethod database. " + ex.getMessage());
            }
        }
    }

    // add additional runner for each @Test method
    public void add(SystemTestRunner... runners) {
        this.runners.addAll(Arrays.asList(runners));
    }

    // add initialization for each @Test method
    public void initBy(StrictHQ hq) {
        initHqs.add(hq);
    }

    public void initBy(List<? extends StrictHQ> hqs) {
        initHqs.addAll(hqs);
    }

    public void set(StrictHQ hq, String expected) {
        entries.add(pair(hq, expected));
    }

    public void set(List<? extends StrictHQ> hqs, String... expecteds) {
        Preconditions.checkArgument(hqs.size() == expecteds.length,
            "Mismatch between number of queries(%d) and length of answers(%d)", hqs.size(),
            expecteds.length);

        for (int i = 0; i < expecteds.length; i++) {
            set(hqs.get(i), expecteds[i]);
        }
    }

    public void set(LazyMatchingResource hq, SystemTestCommonInfo ci) {
        List<RawHQ> rhqs = hq.toStrict(ci.caseDir);
        String[] answers = hq.getAnswers(ci.answerDir);

        Preconditions.checkArgument(rhqs.size() == answers.length,
            "Mismatch between number of queries(%d) and length of answers(%d)", rhqs.size(),
            answers.length);

        for (int i = 0; i < answers.length; i++) {
            set(rhqs.get(i), answers[i]);
        }
    }

    public void run() throws Exception {
        for (SystemTestRunner runner : runners) {
            if (!reachGoal.contains(runner)) {
                // initialization each @Test methods
                for (StrictHQ q : initHqs) {
                    runner.exec(q);
                }
                reachGoal.add(runner);
            }

            for (Entry<StrictHQ, String> entry : entries) {
                runner.matching(entry.getKey(), entry.getValue());
            }
        }
    }

    private Entry<StrictHQ, String> pair(StrictHQ hq, String answer) {
        return new SimpleEntry<StrictHQ, String>(hq, answer);
    }
}
