import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class KReducer extends Reducer<IntWritable,TwoDPointWritable,IntWritable,Text> {

    public void reduce(IntWritable clusterid, Iterable<TwoDPointWritable> points, 
			 Context context
	  ) throws IOException, InterruptedException {

	  int num = 0;
	  float centerx=0.0f;
	  float centery=0.0f;
	  for (TwoDPointWritable point : points) {
	      num++;
	      float x = point.getx();
	      float y = point.gety();
	      centerx += x;
	      centery += y;
	  }
	  centerx = centerx/num;
	  centery = centery/num;
	  
	  String preres = String.format("%f %f", centerx, centery);
	  Text result = new Text(preres);
	  context.write(clusterid, result);
    }
}
