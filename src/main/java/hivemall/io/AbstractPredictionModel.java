/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
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
package hivemall.io;

import hivemall.mix.MixedWeight;
import hivemall.mix.MixedWeight.WeightWithCovar;
import hivemall.utils.collections.IntOpenHashMap;
import hivemall.utils.collections.OpenHashMap;

import javax.annotation.Nonnull;

public abstract class AbstractPredictionModel implements PredictionModel {
    public static final byte BYTE0 = 0;

    protected ModelUpdateHandler handler;
    protected int numMixed;
    private boolean cancelMixRequest;
    private int mixThreshold;

    private IntOpenHashMap<MixedWeight> mixedRequests_i;
    private OpenHashMap<Object, MixedWeight> mixedRequests_o;

    public AbstractPredictionModel() {
        this.numMixed = 0;
    }

    protected abstract boolean isDenseModel();

    @Override
    public ModelUpdateHandler getUpdateHandler() {
        return handler;
    }

    @Override
    public void configureMix(ModelUpdateHandler handler, boolean cancelMixRequest, int mixThreshold) {
        this.handler = handler;
        this.cancelMixRequest = cancelMixRequest;
        this.mixThreshold = mixThreshold;
        if(cancelMixRequest) {
            if(isDenseModel()) {
                this.mixedRequests_i = new IntOpenHashMap<MixedWeight>(327680);
            } else {
                this.mixedRequests_o = new OpenHashMap<Object, MixedWeight>(327680);
            }
        }
    }

    @Override
    public final int getNumMixed() {
        return numMixed;
    }

    @Override
    public void resetDeltaUpdates(int feature) {
        throw new UnsupportedOperationException();
    }

    protected final void onUpdate(final int feature, final float weight, final float covar, final short clock, final int deltaUpdates, final boolean hasCovar) {
        if(handler != null) {
            if(deltaUpdates < mixThreshold) {
                return;
            }
            final int generation = cancelMixRequest(feature, weight, covar, clock, deltaUpdates, hasCovar);
            try {
                handler.sendRequest(feature, weight, covar, clock, generation);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            resetDeltaUpdates(feature);
        }
    }

    protected final void onUpdate(final Object feature, final IWeightValue value) {
        if(handler != null) {
            if(!value.isTouched()) {
                return;
            }
            final int deltaUpdates = value.getDeltaUpdates();
            if(deltaUpdates < mixThreshold) {
                return;
            }
            final float weight = value.get();
            final boolean hasCovar = value.hasCovariance();
            final float covar = hasCovar ? value.getCovariance() : 1.f;
            final short clock = value.getClock();

            final int generation = cancelMixRequest(feature, weight, covar, clock, deltaUpdates, hasCovar);
            try {
                handler.sendRequest(feature, weight, covar, clock, generation);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            value.setDeltaUpdates(BYTE0);
        }
    }

    private int cancelMixRequest(final int feature, final float weight, final float covar, final short clock, final int deltaUpdates, final boolean hasCovar) {
        if(!cancelMixRequest) {
            return 1;
        }

        MixedWeight prevMixed;
        if(hasCovar) {
            prevMixed = mixedRequests_i.get(feature);
            if(prevMixed == null) {
                prevMixed = new WeightWithCovar(weight, covar);
                mixedRequests_i.put(feature, prevMixed);
            } else {
                try {
                    handler.sendCancelRequest(feature, prevMixed);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                prevMixed.setWeight(weight);
                prevMixed.setCovar(covar);
                prevMixed.incrGeneration();
            }
        } else {
            prevMixed = mixedRequests_i.get(feature);
            if(prevMixed == null) {
                prevMixed = new MixedWeight(weight);
                mixedRequests_i.put(feature, prevMixed);
            } else {
                try {
                    handler.sendCancelRequest(feature, prevMixed);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                prevMixed.setWeight(weight);
                prevMixed.incrGeneration();
            }
        }
        return prevMixed.getGeneration();
    }

    private int cancelMixRequest(final Object feature, final float weight, final float covar, final short clock, final int deltaUpdates, final boolean hasCovar) {
        if(!cancelMixRequest) {
            return 1;
        }

        MixedWeight prevMixed;
        if(hasCovar) {
            prevMixed = mixedRequests_o.get(feature);
            if(prevMixed == null) {
                prevMixed = new WeightWithCovar(weight, covar);
                mixedRequests_o.put(feature, prevMixed);
            } else {
                try {
                    handler.sendCancelRequest(feature, prevMixed);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                prevMixed.setWeight(weight);
                prevMixed.setCovar(covar);
                prevMixed.incrGeneration();
            }
        } else {
            prevMixed = mixedRequests_o.get(feature);
            if(prevMixed == null) {
                prevMixed = new MixedWeight(weight);
                mixedRequests_o.put(feature, prevMixed);
            } else {
                try {
                    handler.sendCancelRequest(feature, prevMixed);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                prevMixed.setWeight(weight);
                prevMixed.incrGeneration();
            }
        }
        return prevMixed.getGeneration();
    }

    @Override
    public void set(@Nonnull Object feature, float weight, float covar, short clock) {
        if(hasCovariance()) {
            _set(feature, weight, covar, clock);
        } else {
            _set(feature, weight, clock);
        }
    }

    protected abstract void _set(@Nonnull Object feature, float weight, short clock);

    protected abstract void _set(@Nonnull Object feature, float weight, float covar, short clock);

}
