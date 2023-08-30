package omniVLDB;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.Serializable;

public class DatasetStats extends Dataset implements Serializable {


    public DatasetStats(Dataset d) {
        super(d);
    }

    public AttributeStats[] computeStats() {
        AttributeStats[] stats = new AttributeStats[Main.numAttributes];

        for (int i = 0; i < Main.numAttributes; i++) {
            // Get String repr of attribute via Parser
            String attrName = Parser.getAttributeName(i);
            stats[i] = statsPerAttribute(i);
        }
        return stats;
    }

    public AttributeStats statsPerAttribute(int attr) {
        return new AttributeStats(attr);
    }


    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }
}
