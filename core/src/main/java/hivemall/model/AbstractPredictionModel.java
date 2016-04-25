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
package hivemall.model;

import hivemall.mix.MixedWeight;
import hivemall.mix.MixedWeight.WeightWithCovar;
import hivemall.mix.MixedWeight.WeightWithDelta;
import hivemall.utils.collections.IntOpenHashMap;
import hivemall.utils.collections.OpenHashMap;

import javax.annotation.Nonnull;

public abstract class AbstractPredictionModel implements PredictionModel {
    public static final byte BYTE0 = 0;

    protected ModelUpdateHandler handler;
    
    private long numMixed;
    private boolean cancelMixRequest;

    private IntOpenHashMap<MixedWeight> mixedRequests_i;
    private OpenHashMap<Object, MixedWeight> mixedRequests_o;

    public AbstractPredictionModel() {
        this.numMixed = 0L;
        this.cancelMixRequest = false;
    }

    protected abstract boolean isDenseModel();

    @Override
    public ModelUpdateHandler getUpdateHandler() {
        return handler;
    }

    @Override
    public void configureMix(ModelUpdateHandler handler, boolean cancelMixRequest) {
        this.handler = handler;
        this.cancelMixRequest = cancelMixRequest;
        if(cancelMixRequest) {
            if(isDenseModel()) {
                this.mixedRequests_i = new IntOpenHashMap<MixedWeight>(327680);
            } else {
                this.mixedRequests_o = new OpenHashMap<Object, MixedWeight>(327680);
            }
        }
    }

    @Override
    public final long getNumMixed() {
        return numMixed;
    }

    @Override
    public void resetDeltaUpdates(int feature) {
        throw new UnsupportedOperationException();
    }

    protected final void onUpdate(final int feature, final float weight, final float covar, final short clock, final int deltaUpdates, final boolean hasCovar) {
        if(handler != null) {
            if(deltaUpdates < 1) {
                return;
            }
            final boolean requestSent;
            try {
                requestSent = handler.onUpdate(feature, weight, covar, clock, deltaUpdates);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if(requestSent) {
                if(cancelMixRequest) {
                    if(hasCovar) {
                        MixedWeight prevMixed = mixedRequests_i.get(feature);
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
                        }
                    } else {
                        MixedWeight prevMixed = mixedRequests_i.get(feature);
                        if(prevMixed == null) {
                            prevMixed = new WeightWithDelta(weight, deltaUpdates);
                            mixedRequests_i.put(feature, prevMixed);
                        } else {
                            try {
                                handler.sendCancelRequest(feature, prevMixed);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            prevMixed.setWeight(weight);
                            prevMixed.setDeltaUpdates(deltaUpdates);
                        }
                    }
                }
                resetDeltaUpdates(feature);
            }
        }
    }

    protected final void onUpdate(final Object feature, final IWeightValue value) {
        if(handler != null) {
            if(!value.isTouched()) {
                return;
            }
            final float weight = value.get();
            final short clock = value.getClock();
            final int deltaUpdates = value.getDeltaUpdates();
            if(value.hasCovariance()) {
                final float covar = value.getCovariance();
                final boolean requestSent;
                try {
                    requestSent = handler.onUpdate(feature, weight, covar, clock, deltaUpdates);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if(requestSent) {
                    if(cancelMixRequest) {
                        MixedWeight prevMixed = mixedRequests_o.get(feature);
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
                        }
                    }
                    value.setDeltaUpdates(BYTE0);
                }
            } else {
                final boolean requestSent;
                try {
                    requestSent = handler.onUpdate(feature, weight, 1.f, clock, deltaUpdates);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if(requestSent) {
                    if(cancelMixRequest) {
                        MixedWeight prevMixed = mixedRequests_o.get(feature);
                        if(prevMixed == null) {
                            prevMixed = new WeightWithDelta(weight, deltaUpdates);
                            mixedRequests_o.put(feature, prevMixed);
                        } else {
                            try {
                                handler.sendCancelRequest(feature, prevMixed);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            prevMixed.setWeight(weight);
                            prevMixed.setDeltaUpdates(deltaUpdates);
                        }
                    }
                    value.setDeltaUpdates(BYTE0);
                }
            }
        }
    }

    /**
     * 
     */
    @Override
    public void set(@Nonnull Object feature, float weight, float covar, short clock) {
        if(hasCovariance()) {
            _set(feature, weight, covar, clock);
        } else {
            _set(feature, weight, clock);
        }
        numMixed++;
    }

    protected abstract void _set(@Nonnull Object feature, float weight, short clock);

    protected abstract void _set(@Nonnull Object feature, float weight, float covar, short clock);

}
