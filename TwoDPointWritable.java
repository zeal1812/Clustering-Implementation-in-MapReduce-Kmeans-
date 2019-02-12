import java.io.*;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

public class TwoDPointWritable implements Writable { 
	private FloatWritable x, y;

	public TwoDPointWritable() { 
		this.x= new FloatWritable(); this.y= new FloatWritable(); 
		}

	public void write(DataOutput out) { 
		try {
			x.write(out);
			y.write(out); 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}

	public void readFields(DataInput in){ 
		try {
			x.readFields(in); 
			y.readFields(in); 
		} catch (IOException e) {
			e.printStackTrace();
		}
		}

	public void set(float a, float b) {
		this.x.set(a);
		this.y.set(b);
	}

	public float getx() {
		return x.get();
	}

	public float gety() {
		return y.get();
	}
}// end of class TwoDPointWritable
