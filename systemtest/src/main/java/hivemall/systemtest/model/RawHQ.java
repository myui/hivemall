package hivemall.systemtest.model;

public class RawHQ extends StrictHQ {
    String hq;


    RawHQ(String hq) {
        this.hq = hq;
    }


    public String get() {
        return hq;
    }
}
