package hivemall.io;

import hivemall.utils.lang.Copyable;

public interface IWeightValue extends Copyable<IWeightValue> {

    float get();

    void set(float weight);

    boolean hasCovariance();

    float getCovariance();

    void setCovariance(float cov);

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