package hivemall.io;

import hivemall.utils.collections.IMapIterator;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnull;

public final class SynchronizedModelWrapper implements PredictionModel {

    private final PredictionModel model;
    private final Lock lock;

    public SynchronizedModelWrapper(PredictionModel model) {
        this.model = model;
        this.lock = new ReentrantLock();
    }

    // ------------------------------------------------------------
    // Non-synchronized methods with care

    public PredictionModel getModel() {
        return model;
    }

    @Override
    public ModelUpdateHandler getUpdateHandler() {
        return model.getUpdateHandler();
    }

    @Override
    public void setUpdateHandler(ModelUpdateHandler handler) {
        model.setUpdateHandler(handler);
    }

    @Override
    public int getNumMixed() {
        return model.getNumMixed();
    }

    @Override
    public boolean hasCovariance() {
        return model.hasCovariance();
    }

    @Override
    public void configurParams(boolean sum_of_squared_gradients, boolean sum_of_squared_delta_x, boolean sum_of_gradients) {
        model.configurParams(sum_of_squared_gradients, sum_of_squared_delta_x, sum_of_gradients);
    }

    @Override
    public void configureClock() {
        model.configureClock();
    }

    @Override
    public boolean hasClock() {
        return model.hasClock();
    }

    @Override
    public <K, V extends IWeightValue> IMapIterator<K, V> entries() {
        return model.entries();
    }

    // ------------------------------------------------------------
    // The below is synchronized methods

    @Override
    public void resetDeltaUpdates(int feature) {
        try {
            lock.lock();
            model.resetDeltaUpdates(feature);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        try {
            lock.lock();
            return model.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean contains(Object feature) {
        try {
            lock.lock();
            return model.contains(feature);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <T extends IWeightValue> T get(Object feature) {
        try {
            lock.lock();
            return model.get(feature);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <T extends IWeightValue> void set(Object feature, T value) {
        try {
            lock.lock();
            model.set(feature, value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(@Nonnull Object feature) {
        try {
            lock.lock();
            model.delete(feature);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public float getWeight(Object feature) {
        try {
            lock.lock();
            return model.getWeight(feature);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public float getCovariance(Object feature) {
        try {
            lock.lock();
            return model.getCovariance(feature);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void _set(Object feature, float weight, short clock) {
        try {
            lock.lock();
            model._set(feature, weight, clock);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void _set(Object feature, float weight, float covar, short clock) {
        try {
            lock.lock();
            model._set(feature, weight, covar, clock);
        } finally {
            lock.unlock();
        }
    }
}
