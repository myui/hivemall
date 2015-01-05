/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.mf;

import hivemall.UDTFWithOptions;
import hivemall.common.RatingInitilizer;
import hivemall.io.FactorizedModel;
import hivemall.io.Rating;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Primitives;

import java.util.ArrayList;

import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

public abstract class OnlineMatrixFactorizationUDTF extends UDTFWithOptions
        implements RatingInitilizer {
    private static final Log logger = LogFactory.getLog(OnlineMatrixFactorizationUDTF.class);

    /** The number of latent factors */
    protected int factor;
    /** The regularization factor */
    protected float lambda;
    /** The initial mean rating */
    protected float meanRating;
    /** Whether update (and return) the mean rating or not */
    protected boolean updateMeanRating;

    /** Perform random initialization of rank matrix */
    protected boolean randInit;
    /** The maximum initial value in the rank matrix */
    protected float maxInitValue;
    /** The standard deviation of initial rank matrix */
    protected double initStdDev;

    protected FactorizedModel model;

    /** The number of processed training examples */
    protected int count;
    /** The cumulative loss in the training */
    protected double cumulativeLoss;

    protected IntObjectInspector userOI;
    protected IntObjectInspector itemOI;
    protected PrimitiveObjectInspector ratingOI;

    private float[] userProbe, itemProbe;

    public OnlineMatrixFactorizationUDTF() {
        this.factor = 10;
        this.lambda = 0.02f;
        this.meanRating = 0.f;
        this.updateMeanRating = false;
        this.maxInitValue = 1.f;
    }

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("k", "factor", true, "The number of latent factor [default: 10]");
        opts.addOption("r", "lambda", true, "The regularization factor [default: 0.02]");
        opts.addOption("mu", "mean_rating", true, "The mean rating [default: 0.0]");
        opts.addOption("update_mean", false, "Whether update (and return) the mean rating or not");
        opts.addOption("rand_init", false, "Perform random initialization of rank matrix [default: false]");
        opts.addOption("maxval", "max_init_value", true, "The maximum initial value in the rank matrix [default: 1.0]");
        opts.addOption("min_init_stddev", true, "The minimum standard deviation of initial rank matrix [default: 0.1]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = null;
        if(argOIs.length >= 4) {
            String rawArgs = HiveUtils.getConstString(argOIs[3]);
            cl = parseOptions(rawArgs);
            this.factor = Primitives.parseInt(cl.getOptionValue("factor"), 10);
            this.lambda = Primitives.parseFloat(cl.getOptionValue("lambda"), 0.02f);
            this.meanRating = Primitives.parseFloat(cl.getOptionValue("mu"), 0.f);
            this.updateMeanRating = cl.hasOption("update_mean");
            this.randInit = cl.hasOption("rand_init");
            this.maxInitValue = Primitives.parseFloat(cl.getOptionValue("max_init_value"), 1.f);
            this.initStdDev = Primitives.parseDouble(cl.getOptionValue("min_init_stddev"), 0.1d);
        }
        this.initStdDev = Math.max(initStdDev, 1.0d / factor);
        return cl;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length < 3) {
            throw new UDFArgumentException(getClass().getSimpleName()
                    + " takes 3 arguments: INT user, INT item, FLOAT rating [, CONSTANT STRING options]");
        }
        this.userOI = HiveUtils.asIntOI(argOIs[0]);
        this.itemOI = HiveUtils.asIntOI(argOIs[1]);
        this.ratingOI = HiveUtils.asDoubleCompatibleOI(argOIs[2]);

        processOptions(argOIs);

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("idx");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        fieldNames.add("Pu");
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableFloatObjectInspector));
        fieldNames.add("Qi");
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableFloatObjectInspector));
        fieldNames.add("Bu");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        fieldNames.add("Bi");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        if(updateMeanRating) {
            fieldNames.add("mu");
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        }

        this.model = new FactorizedModel(this, factor, meanRating, randInit, maxInitValue, initStdDev);
        this.count = 0;
        this.cumulativeLoss = 0.d;
        this.userProbe = new float[factor];
        this.itemProbe = new float[factor];
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public Rating newRating(float v) {
        return new Rating(v);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        assert (args.length >= 3) : args.length;

        int user = userOI.get(args[0]);
        if(user < 0) {
            throw new HiveException("Illegal user index: " + user);
        }
        int item = itemOI.get(args[1]);
        if(item < 0) {
            throw new HiveException("Illegal item index: " + user);
        }
        double rating = PrimitiveObjectInspectorUtils.getDouble(args[2], ratingOI);

        count++;
        train(user, item, rating);
    }

    @Nonnull
    protected final float[] copyToUserProbe(@Nonnull final Rating[] rating) {
        for(int k = 0, size = factor; k < size; k++) {
            userProbe[k] = rating[k].getWeight();
        }
        return userProbe;
    }

    @Nonnull
    protected final float[] copyToItemProbe(@Nonnull final Rating[] rating) {
        for(int k = 0, size = factor; k < size; k++) {
            itemProbe[k] = rating[k].getWeight();
        }
        return itemProbe;
    }

    protected void train(final int user, final int item, final double rating) {
        final Rating[] users = model.getUserVector(user, true);
        assert (users != null);
        final Rating[] items = model.getItemVector(item, true);
        assert (items != null);
        final float[] userProbe = copyToUserProbe(users);
        final float[] itemProbe = copyToItemProbe(items);

        final double err = predictionError(user, item, rating);
        this.cumulativeLoss += Math.abs(err);

        final float eta = eta();
        for(int k = 0, size = factor; k < size; k++) {
            float Pu = userProbe[k];
            float Qi = itemProbe[k];
            updateItemRating(items[k], Pu, Qi, err, eta);
            updateUserRating(users[k], Pu, Qi, err, eta);
        }
        updateBias(user, item, err, eta);
        if(updateMeanRating) {
            updateMeanRating(err, eta);
        }

        onUpdate(user, item, users, items, err);
    }

    protected void onUpdate(final int user, final int item, final Rating[] users, final Rating[] items, final double err) {}

    protected double predictionError(final int user, final int item, final double rating) {
        return rating - predict(user, item, userProbe, itemProbe);
    }

    protected double predict(final int user, final int item, final float[] userProbe, final float[] itemProbe) {
        double ret = bias(user, item);
        for(int k = 0, size = factor; k < size; k++) {
            ret += userProbe[k] * itemProbe[k];
        }
        return ret;
    }

    protected double predict(final int user, final int item) throws HiveException {
        final Rating[] users = model.getUserVector(user);
        if(users == null) {
            throw new HiveException("User rating is not found: " + user);
        }
        final Rating[] items = model.getItemVector(item);
        if(items == null) {
            throw new HiveException("Item rating is not found: " + item);
        }
        double ret = bias(user, item);
        for(int k = 0, size = factor; k < size; k++) {
            ret += users[k].getWeight() * items[k].getWeight();
        }
        return ret;
    }

    protected double bias(final int user, final int item) {
        return model.getMeanRating() + model.getUserBias(user) + model.getItemBias(item);
    }

    protected abstract float eta();

    protected void updateItemRating(final Rating rating, final float Pu, final float Qi, final double err, final float eta) {
        double grad = err * Pu - lambda * Qi;
        float newQi = Qi + (float) (eta * grad);
        rating.setWeight(newQi);
    }

    protected void updateUserRating(final Rating rating, final float Pu, final float Qi, final double err, final float eta) {
        double grad = err * Qi - lambda * Pu;
        float newPu = Pu + (float) (eta * grad);
        rating.setWeight(newPu);
    }

    protected void updateMeanRating(final double err, final float eta) {
        assert updateMeanRating;
        float mean = model.getMeanRating();
        mean += eta * err;
        model.setMeanRating(mean);
    }

    protected void updateBias(final int user, final int item, final double err, final float eta) {
        float Bu = model.getUserBias(user);
        Bu += eta * (err - lambda * Bu);
        model.setUserBias(user, Bu);

        float Bi = model.getItemBias(item);
        Bi += eta * (err - lambda * Bi);
        model.setItemBias(item, Bi);
    }

    @Override
    public void close() throws HiveException {
        if(model != null) {
            if(count == 0) {
                this.model = null; // help GC
                return;
            }
            final IntWritable idx = new IntWritable();
            final FloatWritable[] Pu = HiveUtils.newFloatArray(factor, 0.f);
            final FloatWritable[] Qi = HiveUtils.newFloatArray(factor, 0.f);
            final FloatWritable Bu = new FloatWritable();
            final FloatWritable Bi = new FloatWritable();
            final Object[] forwardObj;
            if(updateMeanRating) {
                float meanRating = model.getMeanRating();
                FloatWritable mu = new FloatWritable(meanRating);
                forwardObj = new Object[] { idx, Pu, Qi, Bu, Bi, mu };
            } else {
                forwardObj = new Object[] { idx, Pu, Qi, Bu, Bi };
            }
            int numForwarded = 0;
            for(int i = model.getMinIndex(), maxIdx = model.getMaxIndex(); i <= maxIdx; i++) {
                idx.set(i);
                Rating[] userRatings = model.getUserVector(i);
                if(userRatings == null) {
                    forwardObj[1] = null;
                } else {
                    forwardObj[1] = Pu;
                    copyTo(userRatings, Pu);
                }
                Rating[] itemRatings = model.getItemVector(i);
                if(itemRatings == null) {
                    forwardObj[2] = null;
                } else {
                    forwardObj[2] = Qi;
                    copyTo(itemRatings, Qi);
                }
                Bu.set(model.getUserBias(i));
                Bi.set(model.getItemBias(i));
                forward(forwardObj);
                numForwarded++;
            }
            this.model = null; // help GC
            logger.info("Forwarded the prediction model of " + numForwarded + " rows");
        }
    }

    private static void copyTo(@Nonnull final Rating[] rating, @Nonnull final FloatWritable[] dst) {
        for(int k = 0, size = rating.length; k < size; k++) {
            float w = rating[k].getWeight();
            dst[k].set(w);
        }
    }

}
