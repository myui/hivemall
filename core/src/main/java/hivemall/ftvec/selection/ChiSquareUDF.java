package hivemall.ftvec.selection;

import hivemall.utils.math.StatsUtils;
import org.apache.hadoop.hive.ql.exec.Description;

import javax.annotation.Nonnull;

@Description(name = "chi2",
        value = "_FUNC_(array<number> expected, array<number> observed) - Returns chi2-value as double")
public class ChiSquareUDF extends DissociationDegreeUDF {
    @Override
    double calcDissociation(@Nonnull final double[] expected,@Nonnull final  double[] observed) {
        return StatsUtils.chiSquare(expected, observed);
    }

    @Override
    @Nonnull
    String getFuncName() {
        return "chi2";
    }
}
