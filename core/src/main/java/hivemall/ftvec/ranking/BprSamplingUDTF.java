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
package hivemall.ftvec.ranking;

import hivemall.UDTFWithOptions;
import hivemall.utils.collections.IntArrayList;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Primitives;

import java.util.ArrayList;
import java.util.Random;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.IntWritable;

@Description(name = "bpr_sampling",
        value = "_FUNC_(int userId, List<int> posItems [, const string options])"
                + "- Returns a relation consists of <int userId, int itemId>")
public final class BprSamplingUDTF extends UDTFWithOptions {

    private PrimitiveObjectInspector userOI;
    private ListObjectInspector itemListOI;
    private PrimitiveObjectInspector itemElemOI;

    private PositiveOnlyFeedback feedback;

    // sampling options
    private float samplingRate;
    private boolean withReplacement;
    private boolean pairSampling;

    private Object[] forwardObjs;
    private IntWritable userId;
    private IntWritable posItemId;
    private IntWritable negItemId;

    public BprSamplingUDTF() {}

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("sampling", "sampling_rate", true,
            "Sampling rates of positive items [default: 1.0]");
        opts.addOption("with_replacement", false, "Do sampling with-replacement [default: false]");
        opts.addOption("uniform_pair_sampling", "pair_sampling", false,
            "Sampling pairs uniform from feedbacks [default: false]");
        opts.addOption("maxcol", true, "Max col index [default: -1]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(@Nonnull ObjectInspector[] argOIs)
            throws UDFArgumentException {
        CommandLine cl = null;

        int maxCol = -1;
        float samplingRate = 1.f;
        boolean withReplacement = false;
        boolean pairSampling = false;

        if (argOIs.length >= 3) {
            String args = HiveUtils.getConstString(argOIs[2]);
            cl = parseOptions(args);

            maxCol = Primitives.parseInt(cl.getOptionValue("maxcol"), maxCol);
            withReplacement = cl.hasOption("with_replacement");
            pairSampling = cl.hasOption("uniform_pair_sampling");

            samplingRate = Primitives.parseFloat(cl.getOptionValue("sampling_rate"), samplingRate);
            if (withReplacement == false && samplingRate > 1.f) {
                throw new UDFArgumentException(
                    "sampling_rate MUST be in less than or equals to 1 where withReplacement is false: "
                            + samplingRate);
            }
        }

        this.feedback = new PositiveOnlyFeedback(maxCol);

        this.samplingRate = samplingRate;
        this.withReplacement = withReplacement;
        this.pairSampling = pairSampling;
        return cl;
    }

    @Override
    public StructObjectInspector initialize(@Nonnull ObjectInspector[] argOIs)
            throws UDFArgumentException {
        if (argOIs.length != 2 && argOIs.length != 3) {
            throw new UDFArgumentException(
                "_FUNC_(int userid, array<int> itemid, [, const string options])"
                        + " takes at least two arguments");
        }
        this.userOI = HiveUtils.asIntegerOI(argOIs[0]);
        this.itemListOI = HiveUtils.asListOI(argOIs[1]);
        this.itemElemOI = HiveUtils.asIntegerOI(itemListOI.getListElementObjectInspector());

        processOptions(argOIs);

        this.userId = new IntWritable();
        this.posItemId = new IntWritable();
        this.negItemId = new IntWritable();
        this.forwardObjs = new Object[] {userId, posItemId, negItemId};

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("user");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        fieldNames.add("pos_item");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        fieldNames.add("neg_item");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(@Nonnull Object[] args) throws HiveException {
        int userId = PrimitiveObjectInspectorUtils.getInt(args[0], userOI);
        validateIndex(userId);

        addFeedback(userId, args[1]);
    }

    @Nullable
    private void addFeedback(final int userId, @Nonnull final Object arg)
            throws UDFArgumentException {
        final int size = itemListOI.getListLength(arg);
        if (size == 0) {
            return;
        }

        int maxItemId = feedback.getMaxItemId();
        final IntArrayList posItems = new IntArrayList(size);
        for (int i = 0; i < size; i++) {
            Object elem = itemListOI.getListElement(arg, i);
            if (elem == null) {
                continue;
            }
            int index = PrimitiveObjectInspectorUtils.getInt(elem, itemElemOI);
            validateIndex(index);
            maxItemId = Math.max(index, maxItemId);
            posItems.add(index);
        }

        feedback.addFeedback(userId, posItems);
        feedback.setMaxItemId(maxItemId);
    }

    @Override
    public void close() throws HiveException {
        int feedbacks = feedback.getTotalFeedbacks();
        if (feedbacks == 0) {
            return;
        }
        int numSamples = (int) (feedbacks * samplingRate);

        if (pairSampling) {
            PerEventPositiveOnlyFeedback evFeedback = (PerEventPositiveOnlyFeedback) feedback;
            if (withReplacement) {
                uniformPairSamplingWithReplacement(evFeedback, numSamples);
            } else {
                uniformPairSamplingWithoutReplacement(evFeedback, numSamples);
            }
        } else {
            if (withReplacement) {
                uniformUserSamplingWithReplacement(feedback, numSamples);
            } else {
                uniformUserSamplingWithoutReplacement(feedback, numSamples);
            }
        }
    }

    private void forward(final int user, final int posItem, final int negItem) throws HiveException {
        assert (user >= 0) : user;
        assert (posItem >= 0) : posItem;
        assert (negItem >= 0) : negItem;

        userId.set(user);
        posItemId.set(posItem);
        negItemId.set(negItem);
        forward(forwardObjs);
    }

    /**
     * Sampling pairs uniform for each user with replacement. Sample a user. Then, sample a pair.
     */
    private void uniformUserSamplingWithReplacement(@Nonnull final PositiveOnlyFeedback feedback,
            final int numSamples) throws HiveException {
        final int numUsers = feedback.getNumUsers();
        if (numUsers == 0) {
            return;
        }
        final int maxItemId = feedback.getMaxItemId();
        final Random rand = new Random(31L);

        for (int i = 0; i < numSamples; i++) {
            int user = rand.nextInt(numUsers);
            IntArrayList posItems = feedback.getItems(user, true);
            assert (posItems != null) : user;
            int size = posItems.size();
            assert (size > 0) : size;
            int posItem = rand.nextInt(size);
            int negItem;
            do {
                negItem = rand.nextInt(maxItemId);
            } while (posItems.contains(negItem));

            forward(user, posItem, negItem);
        }
    }

    /**
     * Sampling pairs uniform for each user without replacement. Sample a user. Then, sample a pair.
     * 
     * Caution: This is not a perfect 'without sampling' but it does 'without sampling' for positive
     * feedbacks.
     */
    private void uniformUserSamplingWithoutReplacement(
            @Nonnull final PositiveOnlyFeedback feedback, final int numSamples)
            throws HiveException {
        final int maxItemId = feedback.getMaxItemId();
        final Random rand = new Random(31L);

        for (int i = 0; i < numSamples; i++) {
            int numUsers = feedback.getNumUsers();
            if (numUsers == 0) {
                break;
            }

            int user = rand.nextInt(numUsers);
            IntArrayList posItems = feedback.getItems(user, true);
            assert (posItems != null) : user;
            int size = posItems.size();
            assert (size > 0) : size;
            int posItem = rand.nextInt(size);
            int negItem;
            do {
                negItem = rand.nextInt(maxItemId);
            } while (posItems.contains(negItem));

            posItems.remove(posItem);
            if (posItems.isEmpty()) {
                feedback.removeFeedback(user);
            }

            forward(user, posItem, negItem);
        }
    }

    /**
     * Sampling pairs uniform from feedbacks with replacement.
     */
    private void uniformPairSamplingWithReplacement(
            @Nonnull final PerEventPositiveOnlyFeedback feedback, final int numSamples)
            throws HiveException {
        final int numFeedbacks = feedback.getTotalFeedbacks();
        if (numFeedbacks == 0) {
            return;
        }
        final Random rand = new Random(31L);
        final int maxItemId = feedback.getMaxItemId();

        for (int i = 0; i < numSamples; i++) {
            int index = rand.nextInt(numFeedbacks);
            int user = feedback.getUser(index);
            int posItem = feedback.getPositiveItem(index);

            IntArrayList posItems = feedback.getItems(user, true);
            assert (posItems != null) : user;

            int negItem;
            do {
                negItem = rand.nextInt(maxItemId);
            } while (posItems.contains(negItem));

            forward(user, posItem, negItem);
        }
    }

    /**
     * Sampling pairs uniform from feedbacks without replacement.
     * 
     * Caution: This is not a perfect 'without sampling' but it does 'without sampling' for positive
     * feedbacks.
     */
    private void uniformPairSamplingWithoutReplacement(
            @Nonnull final PerEventPositiveOnlyFeedback feedback, final int numSamples)
            throws HiveException {
        final int numFeedbacks = feedback.getTotalFeedbacks();
        if (numFeedbacks == 0) {
            return;
        }
        final Random rand = new Random(31L);
        final int maxItemId = feedback.getMaxItemId();

        final int[] perm = feedback.getRandomIndex(rand);
        for (int index : perm) {
            int user = feedback.getUser(index);
            int posItem = feedback.getPositiveItem(index);

            IntArrayList posItems = feedback.getItems(user, true);
            assert (posItems != null) : user;

            int negItem;
            do {
                negItem = rand.nextInt(maxItemId);
            } while (posItems.contains(negItem));

            forward(user, posItem, negItem);
        }

    }

    private static void validateIndex(final int index) throws UDFArgumentException {
        if (index < 0) {
            throw new UDFArgumentException("Negative index is not allowed: " + index);
        }
    }


}
