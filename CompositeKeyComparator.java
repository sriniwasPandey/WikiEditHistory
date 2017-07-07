//package wiki.hadoopcomposite;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }  
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
    	CompositeKey k1 = (CompositeKey)w1;
    	CompositeKey k2 = (CompositeKey)w2;
         
        int result = k1.getUser().compareTo(k2.getUser());
        if(0 == result) {
            result = -1* k1.gettStamp().compareTo(k2.gettStamp());
        }
        return result;
    }
}
