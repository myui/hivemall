package hivemall.dataset;

import hivemall.UDTFWithOptions;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Primitives;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public final class RegressionDataGeneratorUDTF extends UDTFWithOptions {
    private static final Log logger = LogFactory.getLog(RegressionDataGeneratorUDTF.class);

    private static final int N_BUFFERS = 1000;

    // control variable
    private int position;
    private float[] labels;
    private String[][] featuresArray;
    private Float[][] featuresFloatArray;

    private int n_examples;
    private int n_features;
    private int n_dimensions;
    private float eps;
    private float prob_one;
    private int r_seed;
    private boolean sparse;

    private Random rnd1, rnd2;

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("ne", "n_examples", true, "Number of training examples created for each task [DEFAULT: 1000]");
        opts.addOption("nf", "n_features", true, "Number of features contained for each example [DEFAULT: 10]");
        opts.addOption("nd", "n_dims", true, "The size of feature dimensions [DEFAULT: 100]");
        opts.addOption("eps", true, "eps Epsilon factor by which positive examples are scaled [DEFAULT: 3.0]");
        opts.addOption("p1", "prob_one", true, " Probability in [0, 1.0) that a label is 1 [DEFAULT: 0.6]");
        opts.addOption("seed", true, "The seed value for random number generator [DEFAULT: 43]");
        opts.addOption("sparse", true, "Make a sparse dataset or not. [DEFAULT: true]\n"
                + "For sparse, n_dims should be much larger than n_features. When disabled, n_features must be equals to n_dims ");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length != 1) {
            throw new UDFArgumentException("Expected number of arguments is 1: " + argOIs.length);
        }
        String opts = HiveUtils.getConstString(argOIs[0]);
        CommandLine cl = parseOptions(opts);

        this.n_examples = Primitives.parseInt(cl.getOptionValue("n_examples"), 1000);
        this.n_features = Primitives.parseInt(cl.getOptionValue("n_features"), 10);
        this.n_dimensions = Primitives.parseInt(cl.getOptionValue("n_dims"), 100);
        this.eps = Primitives.parseFloat(cl.getOptionValue("eps"), 3.f);
        this.prob_one = Primitives.parseFloat(cl.getOptionValue("prob_one"), 0.6f);
        this.r_seed = Primitives.parseInt(cl.getOptionValue("seed"), 43);
        this.sparse = Primitives.parseBoolean(cl.getOptionValue("sparse"), true);

        if(n_features > n_dimensions) {
            throw new UDFArgumentException("n_features '" + n_features
                    + "' should be greater than or equals to n_dimensions '" + n_dimensions + "'");
        }
        if(!sparse) {
            if(n_features != n_dimensions) {
                throw new UDFArgumentException("n_features '" + n_features
                        + "' must be equlas to n_dimensions '" + n_dimensions
                        + "' when making a dense dataset");
            }
        }

        return cl;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        processOptions(argOIs);

        init();

        ArrayList<String> fieldNames = new ArrayList<String>(2);
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);
        fieldNames.add("label");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
        fieldNames.add("features");
        if(sparse) {
            fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector));
        } else {
            fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaFloatObjectInspector));
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    private void init() {
        this.labels = new float[N_BUFFERS];
        if(sparse) {
            this.featuresArray = new String[N_BUFFERS][n_features];
        } else {
            this.featuresFloatArray = new Float[N_BUFFERS][n_features];
        }
        this.position = 0;
        this.rnd1 = new Random(r_seed);
        this.rnd2 = new Random(r_seed + 1);
    }

    @Override
    public void process(Object[] argOIs) throws HiveException {
        for(int i = 0; i < n_examples; i++) {
            if(sparse) {
                generateSparseData();
            } else {
                generateDenseData();
            }
            position++;
            if(position == N_BUFFERS) {
                flushBuffered(position);
                this.position = 0;
            }
        }
    }

    private void generateSparseData() throws HiveException {
        float label = rnd1.nextFloat();
        labels[position] = label;
        String[] features = featuresArray[position];
        assert (features != null);
        final BitSet used = new BitSet(n_dimensions);
        float sign = (label >= prob_one) ? 1.f : 0.f;
        for(int i = 0, retry = 0; i < n_features; i++) {
            int f = rnd2.nextInt(n_dimensions);
            if(used.get(f)) {
                logger.warn(f + " is already used");
                if(retry >= 2) {
                    throw new HiveException("Exceeded max retries. For sparse data generation, n_dims "
                            + n_dimensions
                            + " should be much larger than n_features "
                            + n_features
                            + ".");
                }
                --i;
                ++retry;
                continue;
            }
            used.set(f);
            float w = (float) rnd2.nextGaussian() + (sign * eps);
            String y = f + ":" + w;
            logger.info("set y: " + y);
            features[i] = y;
        }
    }

    private void generateDenseData() {
        float label = rnd1.nextFloat();
        labels[position] = label;
        Float[] features = featuresFloatArray[position];
        assert (features != null);
        float sign = (label >= prob_one) ? 1.f : 0.f;
        for(int i = 0; i < n_features; i++) {
            float w = (float) rnd2.nextGaussian() + (sign * eps);
            features[i] = Float.valueOf(w);
        }
    }

    private void flushBuffered(int position) throws HiveException {
        final Object[] forwardObjs = new Object[2];
        if(sparse) {
            for(int i = 0; i < position; i++) {
                forwardObjs[0] = Float.valueOf(labels[i]);
                forwardObjs[1] = Arrays.asList(featuresArray[i]);
                forward(forwardObjs);
            }
        } else {
            for(int i = 0; i < position; i++) {
                forwardObjs[0] = Float.valueOf(labels[i]);
                forwardObjs[1] = Arrays.asList(featuresFloatArray[i]);
                forward(forwardObjs);
            }
        }
    }

    @Override
    public void close() throws HiveException {
        if(position > 0) {
            flushBuffered(position);
        }
        // release resources to help GCs
        this.labels = null;
        this.featuresArray = null;
        this.featuresFloatArray = null;
    }

}
