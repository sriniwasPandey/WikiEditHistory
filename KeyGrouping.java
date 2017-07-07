//package wiki.hadoopcomposite;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyGrouping extends WritableComparator {
    public KeyGrouping() {
        super(CompositeKey.class, true);
    }
    @Override
    public int compare(WritableComparable tp1, WritableComparable tp2) {
        CompositeKey key = (CompositeKey) tp1;
        CompositeKey key2 = (CompositeKey) tp2;
        return key.getUser().compareTo(key2.getUser());
    }
}
