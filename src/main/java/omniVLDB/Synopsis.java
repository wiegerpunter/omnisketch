package omniVLDB;

public abstract class Synopsis {
    public int[] maxBits;
    protected String setting;
    protected long ram;
    public long memUsageSynopsis;
    public int[] parameters;

    public Synopsis() {

    }

    public abstract void add(Record record);
    public abstract int query(Query query);
    public abstract int rangeQuery(Query query);
    public abstract void reset();

    public long getMemoryUsage() {
        return memUsageSynopsis;
    }
}
