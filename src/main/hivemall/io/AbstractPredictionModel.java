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

public abstract class AbstractPredictionModel implements PredictionModel {
    public static final byte BYTE0 = 0;

    protected ModelUpdateHandler handler;
    protected int numMixed;

    public AbstractPredictionModel() {
        this.numMixed = 0;
    }

    @Override
    public ModelUpdateHandler getUpdateHandler() {
        return handler;
    }

    @Override
    public void setUpdateHandler(ModelUpdateHandler handler) {
        this.handler = handler;
    }

    @Override
    public final int getNumMixed() {
        return numMixed;
    }

    @Override
    public void resetDeltaUpdates(int feature) {
        throw new UnsupportedOperationException();
    }

    protected final void onUpdate(final int feature, final float weight, final float covar, final short clock, final int deltaUpdates) {
        if(deltaUpdates < 1) {
            return;
        }
        if(handler != null) {
            final boolean resetDeltaUpdates;
            try {
                resetDeltaUpdates = handler.onUpdate(feature, weight, covar, clock, deltaUpdates);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if(resetDeltaUpdates) {
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
            final boolean resetDeltaUpdates;
            if(value.hasCovariance()) {
                final float covar = value.getCovariance();
                try {
                    resetDeltaUpdates = handler.onUpdate(feature, weight, covar, clock, deltaUpdates);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                try {
                    resetDeltaUpdates = handler.onUpdate(feature, weight, 1.f, clock, deltaUpdates);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            if(resetDeltaUpdates) {
                value.setDeltaUpdates(BYTE0);
            }
        }
    }

}
