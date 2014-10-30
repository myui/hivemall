package hivemall.io;

import hivemall.utils.lang.Copyable;

import javax.annotation.Nonnegative;

public interface IWeightValue extends Copyable<IWeightValue> {

    public enum WeightValueType {
        NoParams, ParamsF1, ParamsF2, ParamsCovar;
    }

    WeightValueType getType();

    float getFloatParams(@Nonnegative int i);

    float get();

    void set(float weight);

    boolean hasCovariance();

    float getCovariance();

    void setCovariance(float cov);

    float getSumOfSquaredGradients();

    float getSumOfSquaredDeltaX();

    float getSumOfGradients();

    /** 
     * @return whether touched in training or not
     */
    boolean isTouched();

    void setTouched(boolean touched);

    short getClock();

    void setClock(short clock);

    byte getDeltaUpdates();

    void setDeltaUpdates(byte deltaUpdates);

}