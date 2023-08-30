package omniVLDB.omni;

public abstract class Sample {
    protected String setting;
    protected int ram;
    public int memUsageSynopsis;
    public int curSampleSize;
    double delta;
    public Sample() {

    }
    public abstract void add(int id);
    public abstract void add(long hx);

    public abstract void reset();

    public long getMemoryUsage() {
        return memUsageSynopsis;
    }
}
