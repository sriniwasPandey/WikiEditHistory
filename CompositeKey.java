//package wiki.hadoopcomposite;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements Writable, WritableComparable<CompositeKey>  {
	private Text user=new Text();
	public Text getUser() {
		return user;
	}
	public void setUser(Text user) {
		this.user = user;
	}
	public Text gettStamp() {
		return tStamp;
	}
	public void settStamp(Text tStamp) {
		this.tStamp = tStamp;
	}
	private Text tStamp=new Text();
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.user=new Text(in.readUTF());
		this.tStamp=new Text(in.readUTF());
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(user.toString());
		out.writeUTF(tStamp.toString());
		
	}
	@Override
	public int compareTo(CompositeKey key) {
		int compareValue=this.user.compareTo(key.getUser());
		 if (compareValue == 0) 
		 {
			 compareValue = this.tStamp.compareTo(key.gettStamp());
		 }
	    return -1*compareValue; 
	}

}
