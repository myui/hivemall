package hivemall.ftvec.selection;

import hivemall.utils.math.StatsUtils;
import org.apache.hadoop.hive.ql.exec.Description;

import javax.annotation.Nonnull;

@Description(name = "chi2_test",
        value = "_FUNC_(array<number> expected, array<number> observed) - Returns p-value as double")
public class ChiSquareTestUDF extends DissociationDegreeUDF {
    @Override
    double calcDissociation(@Nonnull final double[] expected,@Nonnull final  double[] observed) {
        return StatsUtils.chiSquareTest(expected, observed);
    }

    @Override
    @Nonnull
    String getFuncName() {
        return "chi2_test";
    }
}
